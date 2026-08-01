"""
Everything a run is configured with, in one object.

:class:`SimulationConfig` is the only thing in askew a user is expected to tune,
and the arguments to :func:`~askew.testing.runner.simulate` land here unchanged.
It holds data and derives seeds; it does not build anything. The runner reads it
and constructs the world.

Two of its methods carry weight beyond storing fields. :meth:`seed_for` turns
the base seed and an iteration number into that iteration's seed directly,
without running the iterations before it, which is what makes replaying failure
number 8,417 cost nothing. :meth:`replace` produces a variant, and exists for
exactly one caller: the runner reruns a failed seed with tracing switched on,
and needs a configuration that differs in that one field and in nothing else.
"""

from __future__ import annotations

from os import environ
from typing import TYPE_CHECKING

from ..core.rng import SeedMixer
from ..core.scheduler import RandomScheduler

if TYPE_CHECKING:
    from ..core.scheduler import Scheduler
    from ..sim.faults import FaultPolicy
    from ..sim.net import LinkConfig

class SimulationConfig:
    """
    The parameters of a run.

    :ivar seed: base seed; every iteration derives its own from this
    :ivar iterations: how many times the scenario is run
    :ivar scheduler: how each tick's ready callbacks are ordered
    :ivar link: default behaviour of every network link
    :ivar chaos: fault policy to run in the background, or ``None``
    :ivar start_time: virtual time the clock starts at
    :ivar max_steps: callbacks allowed per iteration, zero for unlimited
    :ivar max_time: virtual seconds allowed per iteration, zero for unlimited
    :ivar detect_deadlock: whether an idle loop with unfinished work is an error
    :ivar strict: whether an exception in a node task fails the iteration
    :ivar trace: whether the event log records anything
    :ivar trace_limit: records kept before the oldest are discarded
    :ivar trace_lines: records attached to a failure
    :ivar raise_on_partition: whether sending across a partition raises
    """

    __slots__ = (
        "seed",
        "iterations",
        "scheduler",
        "link",
        "chaos",
        "start_time",
        "max_steps",
        "max_time",
        "detect_deadlock",
        "strict",
        "trace",
        "trace_limit",
        "trace_lines",
        "raise_on_partition"
    )

    SEED_VARIABLE = "ASKEW_SEED"
    """
    Environment variable pinning the run to a single seed.

    Set it to reproduce a reported failure without editing the test, which is
    what a bug report should let you do.
    """

    ITERATIONS_VARIABLE = "ASKEW_ITERATIONS"
    """
    Environment variable overriding the iteration count.

    Ten thousand iterations belong in a nightly run, not in the loop a developer
    waits on. Set this low locally and leave the number in the source honest.
    """

    def __init__(
            self,
            seed: int = 0,
            iterations: int = 1,
            scheduler: Scheduler | None = None,
            link: LinkConfig | None = None,
            chaos: FaultPolicy | None = None,
            start_time: float = 0.0,
            max_steps: int = 10_000_000,
            max_time: float = 0.0,
            detect_deadlock: bool = True,
            strict: bool = True,
            trace: bool = False,
            trace_limit: int = 256,
            trace_lines: int = 20,
            raise_on_partition: bool = False
    ) -> None:
        self.seed = seed
        self.iterations = iterations
        self.scheduler = scheduler if scheduler is not None else RandomScheduler()
        self.link = link
        self.chaos = chaos
        self.start_time = start_time
        self.max_steps = max_steps
        self.max_time = max_time
        self.detect_deadlock = detect_deadlock
        self.strict = strict
        self.trace = trace
        self.trace_limit = trace_limit
        self.trace_lines = trace_lines
        self.raise_on_partition = raise_on_partition

    def seed_for(self, iteration: int) -> int:
        """
        Return the seed for the given *iteration*.

        Computed straight from the base seed, so iteration 8,417 costs one
        multiplication rather than the 8,417 runs before it. That is what makes
        replaying a late failure instant.
        """
        return SeedMixer.mix(self.seed, iteration)

    def replace(self, **changes: object) -> SimulationConfig:
        """
        Return a copy with the named fields changed.

        Written out rather than generated, because a configuration with a field
        silently missing would produce a run that differs from the one being
        reproduced, and that is the one bug this library cannot afford.
        """
        values = {name: getattr(self, name) for name in SimulationConfig.__slots__}
        values.update(changes)
        return SimulationConfig(**values)

    def with_environment(self) -> SimulationConfig:
        """
        Return a copy with ``ASKEW_SEED`` and ``ASKEW_ITERATIONS`` applied.

        Pinning a seed also forces a single iteration, since a pinned seed
        describes one specific run and repeating it would only run it again.
        """
        changes: dict[str, object] = {}

        raw = environ.get(SimulationConfig.SEED_VARIABLE)
        if raw:
            changes["seed"] = SimulationConfig.parse(raw, SimulationConfig.SEED_VARIABLE)
            changes["iterations"] = 1

        raw = environ.get(SimulationConfig.ITERATIONS_VARIABLE)
        if raw:
            changes["iterations"] = SimulationConfig.parse(raw, SimulationConfig.ITERATIONS_VARIABLE)

        return self.replace(**changes) if changes else self

    @staticmethod
    def parse(raw: str, variable: str) -> int:
        """
        Read an integer from an environment variable, or say which one was wrong.
        """
        try:
            return int(raw)
        except ValueError:
            raise ValueError("%s must be an integer, got %r" % (variable, raw)) from None

    def __repr__(self) -> str:
        return "SimulationConfig(seed=%d, iterations=%d, scheduler=%r)" % (self.seed, self.iterations, self.scheduler)