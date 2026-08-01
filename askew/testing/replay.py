"""
Running one specific seed again.

A failure reports a seed, and this is what you do with it. Everything here takes
a decorated test and reaches through it to the
:class:`~askew.testing.runner.SimulationTest` underneath, so the thing you pass
is the same name that appears in the failure you are chasing.

Tracing is on by default in every method here. The only reason to run a single
seed is to look at what happened, and the cost that made tracing off the default
during a ten thousand iteration run does not apply to running one.

:meth:`Replay.check` is the one to reach for once a bug is fixed. Pinning the
seed that used to fail turns a lucky discovery into a permanent regression test::

    def test_regression_8149203():
        askew.Replay.check(test_leader_election, seed=8149203)

That test costs one iteration, always exercises the exact interleaving that
broke, and does not depend on the randomised run happening to visit it again.
"""

from __future__ import annotations

from typing import Any

from ..errors import SimulationFailure
from .report import Report
from .runner import SimulationTest

class Replay:
    """
    Reruns a decorated test under a single seed.

    Every method is static; the class exists to group them under a name that
    reads well at the call site.
    """

    __slots__ = ()

    @staticmethod
    def of(test: Any) -> SimulationTest:
        """
        Return the :class:`~askew.testing.runner.SimulationTest` behind *test*.

        Accepts either a decorated test function, which carries it as its
        ``askew`` attribute, or the simulation test itself.
        """
        if isinstance(test, SimulationTest):
            return test
        found = getattr(test, "askew", None)
        if isinstance(found, SimulationTest):
            return found
        raise TypeError("%r is not an askew test; decorate it with @askew.simulate first" % (test,))

    @staticmethod
    def run(test: Any, seed: int, trace: bool = True) -> Report:
        """
        Run *test* once under *seed* and return the report, without raising.

        The seed is used exactly as given, not derived from an iteration number,
        because the seed a failure prints is already the derived one.
        """
        return Replay.of(test).run_seed(seed, trace)

    @staticmethod
    def iteration(test: Any, index: int, trace: bool = True) -> Report:
        """
        Run the iteration at *index* of the test's own run.

        Derives that iteration's seed from the configured base seed, so this
        reaches iteration 8,417 without running the 8,417 before it. Useful when
        you have the iteration number but not the seed.
        """
        simulation = Replay.of(test)
        return simulation.run_seed(simulation.config.seed_for(index), trace)

    @staticmethod
    def check(test: Any, seed: int) -> Report:
        """
        Run *test* under *seed* and raise if it fails.

        The regression test form. Returns the report when it passes, so an
        assertion can go on to look at the numbers.
        """
        report = Replay.run(test, seed)
        if report.ok:
            return report
        raise SimulationFailure(
            SimulationTest.describe(report),
            seed=seed,
            iteration=report.iteration,
            virtual_time=report.virtual_time,
            steps=report.steps,
            cause=report.error,
            trace=report.trace,
            events=report.events
        ) from report.error