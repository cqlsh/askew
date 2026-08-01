"""
askew: deterministic simulation testing for asyncio.

Replaces the event loop with one whose clock is virtual and whose callback order
is drawn from a seeded generator. The same scenario run under ten thousand seeds
visits ten thousand interleavings, each in microseconds, and any failure reduces
to the one integer that reproduces it.

Everything meant for use is re-exported here. The subpackages :mod:`askew.core`
and :mod:`askew.sim` are the machinery underneath and can be imported directly,
but nothing in the documented API requires it.
"""

from __future__ import annotations

from .core.clock import Clock
from .core.log import EventLog
from .core.loop import SimLoop
from .core.rng import Rng, SeedMixer
from .core.scheduler import (
    FifoScheduler,
    PartialScheduler,
    RandomScheduler,
    ReverseScheduler,
    Scheduler
)
from .errors import (
    AskewError,
    DeadlockError,
    NodeCrashed,
    NondeterminismError,
    SimulationFailure,
    StepLimitExceeded,
    TimeLimitExceeded,
    Unreachable
)
from .sim.faults import FaultInjector, FaultPolicy
from .sim.net import LinkConfig, Message, Network, Partition
from .sim.node import Node
from .sim.world import World
from .testing.config import SimulationConfig
from .testing.replay import Replay
from .testing.report import Report
from .testing.runner import Simulation, SimulationTest, simulate

__version__ = "0.1.0.dev0"

__all__ = [
    "AskewError",
    "Clock",
    "DeadlockError",
    "EventLog",
    "FaultInjector",
    "FaultPolicy",
    "FifoScheduler",
    "LinkConfig",
    "Message",
    "Network",
    "Node",
    "NodeCrashed",
    "NondeterminismError",
    "PartialScheduler",
    "Partition",
    "RandomScheduler",
    "Replay",
    "Report",
    "ReverseScheduler",
    "Rng",
    "Scheduler",
    "SeedMixer",
    "SimLoop",
    "Simulation",
    "SimulationConfig",
    "SimulationFailure",
    "SimulationTest",
    "StepLimitExceeded",
    "TimeLimitExceeded",
    "Unreachable",
    "World",
    "__version__",
    "simulate"
]