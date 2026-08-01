"""
Running simulations and reporting what they found.

This is the layer a test touches directly, and the only one it has to. Everything
below it, the loop, the world, the network, the fault injector, exists to be
driven from here.

:class:`~askew.testing.runner.simulate` is the entry point; the rest is what it
hands back when something goes wrong.
"""

from __future__ import annotations

from .config import SimulationConfig
from .replay import Replay
from .report import Report
from .runner import Simulation, SimulationTest, simulate

__all__ = [
    "Replay",
    "Report",
    "Simulation",
    "SimulationConfig",
    "SimulationTest",
    "simulate"
]