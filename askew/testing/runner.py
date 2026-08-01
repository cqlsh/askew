"""
Turning a scenario into a test that runs it ten thousand times.

:class:`simulate` is the decorator, :class:`SimulationTest` is what it produces,
and :class:`Simulation` is one iteration. The decorator returns an ordinary
synchronous function, which is what lets plain pytest collect it: from the
outside a simulation test is a normal test, and no asyncio plugin is involved.

The failure path is the part worth reading. Iterations run with tracing off,
because recording anything across ten thousand passing runs is wasted work. When
one fails, its seed is run a second time with tracing on. Logging draws nothing
from the generator, so the second run is the same run, and the trace it produces
describes the failure that already happened rather than a new one.
"""

from __future__ import annotations

from asyncio import all_tasks, gather
from collections.abc import Callable, Coroutine
from os import environ
from traceback import format_exception
from typing import Any

from ..errors import SimulationFailure
from ..sim.world import World
from .config import SimulationConfig
from .report import Report

class Simulation:
    """
    One iteration: one seed, one world, one run.

    :ivar config: the configuration this iteration runs under
    :ivar seed: the seed for this iteration specifically, already derived
    :ivar iteration: its index within the run
    """

    __slots__ = ("config", "seed", "iteration")

    def __init__(self, config: SimulationConfig, seed: int, iteration: int = 0) -> None:
        self.config = config
        self.seed = seed
        self.iteration = iteration

    def run(self, scenario: Callable[[World], Coroutine[Any, Any, Any]]) -> Report:
        """
        Run *scenario* once and return what happened.

        Never raises for a failure in the scenario; that arrives as
        :attr:`Report.error`. Only a genuine interruption propagates, so a
        control-c stops the run rather than counting as one more failing seed.
        """
        config = self.config
        world = World(
            self.seed,
            config.scheduler,
            config.link,
            config.start_time,
            config.max_steps,
            config.max_time,
            config.detect_deadlock,
            config.trace,
            config.trace_limit,
            config.raise_on_partition
        )
        error: BaseException | None = None
        trace = ""
        try:
            world.loop.run_until_complete(self.main(world, scenario))
        except Exception as failure:
            error = failure
            trace = "".join(format_exception(failure))
        finally:
            report = Report.of(world, self.iteration, error, trace, config.trace_lines)
            self.teardown(world)
        return report

    async def main(self, world: World, scenario: Callable[[World], Coroutine[Any, Any, Any]]) -> None:
        """
        Drive one scenario from inside the loop.

        Chaos starts here rather than in the world's constructor, because
        spawning a task needs a loop that is already running.

        A node that died of its own exception is reported even when the scenario
        itself passed. Left alone, asyncio logs such a failure and moves on,
        which is exactly the outcome a simulation must not have.
        """
        if self.config.chaos is not None:
            world.start_chaos(self.config.chaos)
        await scenario(world)
        if self.config.strict and world.node_errors:
            raise world.node_errors[0]

    def teardown(self, world: World) -> None:
        """
        Stop everything the run left behind and close the loop.

        Nodes normally run forever, so an iteration ends with tasks still
        pending. They are cancelled and then given the loop back briefly, which
        is what lets a ``finally`` inside a node actually execute.

        Failures here are swallowed. Teardown runs after the interesting
        exception has already been captured, and a node that refuses to die
        cleanly must not replace the reason its iteration failed.
        """
        world.cancel_all()
        loop = world.loop
        try:
            pending = [task for task in all_tasks(loop) if not task.done()]
            if pending:
                loop.run_until_complete(gather(*pending, return_exceptions=True))
        except Exception:
            pass
        finally:
            loop.close()

class SimulationTest:
    """
    A scenario together with the configuration it runs under.

    Reachable from the decorated test as its ``askew`` attribute, which is how
    :class:`~askew.testing.replay.Replay` gets at a single seed.

    :ivar scenario: the coroutine function under test
    :ivar config: how it is run
    """

    __slots__ = ("scenario", "config")

    def __init__(self, scenario: Callable[[World], Coroutine[Any, Any, Any]], config: SimulationConfig) -> None:
        self.scenario = scenario
        self.config = config

    def run(self) -> None:
        """
        Run every iteration, and raise on the first that fails.

        Stops at the first failure rather than collecting them. The seeds are
        independent, so the second failure is a second bug report; fix the first
        one and the run tells you about the next.

        ``ASKEW_SEED`` short circuits all of this and runs that one seed
        directly, without deriving it from an iteration number, since the seed
        printed by a failure is already the derived one.
        """
        config = self.config.with_environment()
        pinned = environ.get(SimulationConfig.SEED_VARIABLE)
        if pinned:
            self.check(config, SimulationConfig.parse(pinned, SimulationConfig.SEED_VARIABLE))
            return
        for iteration in range(config.iterations):
            self.check(config, config.seed_for(iteration), iteration)

    def run_seed(self, seed: int, trace: bool = True) -> Report:
        """
        Run one specific seed and return its report, without raising.

        Tracing defaults on here because the only reason to run a single seed is
        to look at it.
        """
        return Simulation(self.config.replace(trace=trace), seed).run(self.scenario)

    def check(self, config: SimulationConfig, seed: int, iteration: int) -> None:
        """
        Run one iteration, and turn a failure into a :class:`SimulationFailure`.

        The second run is where the trace comes from. It is the same run, since
        the only thing that changed is whether the log records what happens.
        """
        report = Simulation(config, seed, iteration).run(self.scenario)
        if report.ok:
            return

        detailed = Simulation(config.replace(trace=True), seed, iteration).run(self.scenario)
        source = detailed if not detailed.ok else report
        raise SimulationFailure(
            self.describe(source),
            seed=seed,
            iteration=iteration,
            virtual_time=source.virtual_time,
            steps=source.steps,
            cause=source.error,
            trace=source.trace,
            events=source.events
        ) from source.error

    @staticmethod
    def describe(report: Report) -> str:
        """
        Build the message shown at the top of a failure.

        The event tail is folded into the message rather than left on the
        exception, because a test runner prints the message and little else, and
        the trace is the part that explains what happened.
        """
        error = report.error
        summary = "%s: %s" % (type(error).__name__, error) if error is not None else "failed"
        if report.events:
            summary += "\n  recent events\n" + "\n".join("    " + line for line in report.events)
        return summary

    def __repr__(self) -> str:
        return "SimulationTest(%s, %r)" % (getattr(self.scenario, "__name__", "?"), self.config)

class simulate:
    """
    Turn a scenario into a test that runs it many times.

    Lowercase because it reads as a decorator rather than as a class::

        @askew.simulate(seed=1337, iterations=10_000)
        async def test_leader_election(world):
            ...

    Every keyword is passed straight to :class:`~askew.testing.config.SimulationConfig`.

    What comes back is a plain synchronous function taking no arguments. That
    matters twice over: pytest collects it like any other test without an
    asyncio plugin, and because it declares no parameters, pytest does not try
    to satisfy *world* from a fixture. The original coroutine stays reachable
    through the returned function's ``askew`` attribute.
    """

    __slots__ = ("config",)

    def __init__(self, **options: Any) -> None:
        self.config = SimulationConfig(**options)

    def __call__(self, scenario: Callable[[World], Coroutine[Any, Any, Any]]) -> Callable[[], None]:
        test = SimulationTest(scenario, self.config)

        def run_simulation() -> None:
            test.run()

        # Copied by hand rather than with functools.wraps, which would also set
        # __wrapped__ and make pytest inspect the original signature, see a
        # parameter named world, and demand a fixture for it.
        run_simulation.__name__ = getattr(scenario, "__name__", "run_simulation")
        run_simulation.__qualname__ = getattr(scenario, "__qualname__", "run_simulation")
        run_simulation.__doc__ = scenario.__doc__
        run_simulation.__module__ = scenario.__module__
        run_simulation.askew = test
        return run_simulation

    def __repr__(self) -> str:
        return "simulate(%r)" % self.config