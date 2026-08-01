"""
The deterministic substrate: randomness, ordering, time and tracing.

Nothing in this package knows what a node, a message or a fault is. It provides
a loop whose clock is virtual and whose callback order is drawn from a seeded
generator, and that is all. :mod:`askew.sim` builds the simulated world on top
of it, and :mod:`askew.testing` drives runs of that world.

The layering is worth keeping. Because this package stands alone, the loop can
be tested against plain coroutines with no simulation around it, and it remains
usable on its own by anyone who wants a deterministic loop and nothing else.
"""

from __future__ import annotations

from .clock import Clock
from .log import EventLog
from .loop import SimLoop
from .rng import Rng, SeedMixer
from .scheduler import (
    FifoScheduler,
    PartialScheduler,
    RandomScheduler,
    ReverseScheduler,
    Scheduler
)

__all__ = [
    "Clock",
    "EventLog",
    "FifoScheduler",
    "PartialScheduler",
    "RandomScheduler",
    "ReverseScheduler",
    "Rng",
    "Scheduler",
    "SeedMixer",
    "SimLoop"
]