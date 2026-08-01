"""
The simulated world: nodes, the network between them, and what goes wrong.

Everything here is built on :mod:`askew.core` and knows nothing about how a run
is started or how failures are reported. A :class:`World` is the object a test
receives, and the rest of the package is what it is made of.

The dependency direction inside the package runs one way and never back. A node
points at the network it sends through, the fault injector points at the nodes
it kills, and the world points at all of them, since the world is what
constructs them. Nothing reaches back up at its own creator, which is what keeps
each piece testable without the two above it.
"""

from __future__ import annotations

from .faults import FaultInjector, FaultPolicy
from .net import LinkConfig, Message, Network, Partition
from .node import Node
from .world import World

__all__ = [
    "FaultInjector",
    "FaultPolicy",
    "LinkConfig",
    "Message",
    "Network",
    "Node",
    "Partition",
    "World"
]