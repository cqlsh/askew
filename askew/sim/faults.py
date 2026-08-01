"""
Things going wrong on purpose.

Faults are the second half of what makes a simulation worth running. The
scheduler explores orderings; faults explore the states a system reaches when
part of it stops answering. Both draw from the same generator, so a seed pins
down not only who ran first but also who died and when.

Everything here can be invoked directly from a test, which is what you want when
reproducing a specific scenario. :meth:`FaultInjector.chaos` is the other mode:
a background coroutine that keeps applying faults at random for as long as the
run lasts, under a :class:`FaultPolicy`.

Chaos has one consequence worth stating plainly. The coroutine always holds a
pending timer, so the loop is never left with nothing scheduled, and the
deadlock detector can no longer fire. A run under continuous chaos trades
deadlock detection for coverage. Reproduce the failing seed without chaos to get
that detection back.
"""

from __future__ import annotations

from asyncio import sleep as async_sleep
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..core.log import EventLog
    from ..core.loop import SimLoop
    from ..core.rng import Rng
    from .net import Network, Partition
    from .node import Node

class FaultPolicy:
    """
    How often and how badly :meth:`FaultInjector.chaos` interferes.

    The probabilities are per wakeup, not per second, so raising the interval
    and the probability together leaves the rate roughly unchanged.

    :ivar crash: probability of killing one node at a wakeup
    :ivar partition: probability of splitting the network at a wakeup
    :ivar min_interval: shortest gap between wakeups, in simulated seconds
    :ivar max_interval: longest gap between wakeups, in simulated seconds
    :ivar min_duration: shortest life of an injected partition
    :ivar max_duration: longest life of an injected partition
    :ivar max_crashed: nodes that may be down at once, so a quorum stays possible
    """

    __slots__ = (
        "crash",
        "partition",
        "min_interval",
        "max_interval",
        "min_duration",
        "max_duration",
        "max_crashed"
    )

    def __init__(
            self,
            crash: float = 0.1,
            partition: float = 0.1,
            min_interval: float = 1.0,
            max_interval: float = 10.0,
            min_duration: float = 1.0,
            max_duration: float = 30.0,
            max_crashed: int = 1
    ) -> None:
        self.crash = crash
        self.partition = partition
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.min_duration = min_duration
        self.max_duration = max_duration
        self.max_crashed = max_crashed

    def __repr__(self) -> str:
        return "FaultPolicy(crash=%.3f, partition=%.3f, max_crashed=%d)" % (self.crash, self.partition, self.max_crashed)

class FaultInjector:
    """
    Kills nodes and cuts the network, on request or at random.

    :ivar crashed: nodes currently down, in the order they were crashed
    """

    __slots__ = (
        "_rng",
        "_loop",
        "_log",
        "_net",
        "_nodes",
        "crashed"
    )

    def __init__(self, rng: Rng, loop: SimLoop, log: EventLog, net: Network, nodes: list[Node]) -> None:
        self._rng = rng
        self._loop = loop
        self._log = log
        self._net = net
        self._nodes = nodes
        self.crashed: list[Node] = []

    def crash(self, node: Node) -> None:
        """
        Kill *node* immediately.

        Its task is cancelled, its mailbox is discarded, and anything sent to it
        afterwards is dropped on arrival. Idempotent, so crashing a node that is
        already down does nothing.

        The cancellation reaches node code as a
        :exc:`~asyncio.CancelledError`, which is what a process being killed
        looks like from inside. Node code that wants to distinguish a crash from
        an orderly shutdown can read ``node.alive`` in its cleanup path.
        """
        if not node.alive:
            return
        node.alive = False
        self.crashed.append(node)
        while node.try_recv() is not None:
            pass
        if node.task is not None:
            node.task.cancel("crashed by the simulator")
        self._log.add("fault", "node %d crashed", node.id)

    def crash_random(self, limit: int = 1) -> Node | None:
        """
        Kill one live node chosen uniformly, and return it.

        Returns ``None`` and does nothing if *limit* nodes are already down,
        which is how a scenario keeps enough of the cluster alive to make
        progress worth asserting about.
        """
        if len(self.crashed) >= limit:
            return None
        alive = [node for node in self._nodes if node.alive]
        if not alive:
            return None
        node = self._rng.choice(alive)
        self.crash(node)
        return node

    def partition_random(self, groups: int = 2) -> Partition:
        """
        Split the nodes into *groups* roughly equal parts, chosen at random.

        Returned unapplied, so the caller decides how long it lasts::

            async with world.faults.partition_random():
                await world.clock.advance(seconds=30)
        """
        identifiers = [node.id for node in self._nodes]
        self._rng.shuffle(identifiers)
        size = max(1, len(identifiers) // groups)
        parts = [set(identifiers[i:i + size]) for i in range(0, len(identifiers), size)]
        return self._net.partition(*parts)

    async def chaos(self, policy: FaultPolicy) -> None:
        """
        Keep injecting faults for as long as this coroutine runs.

        Spawned by the world when a policy is configured, and cancelled when the
        test finishes. Never returns on its own.

        Remember that this holds a pending timer at all times, which suppresses
        deadlock detection for the whole run. That is the price of continuous
        chaos, and the reason it is off by default.
        """
        rng = self._rng
        while True:
            await async_sleep(rng.uniform(policy.min_interval, policy.max_interval))
            if rng.chance(policy.crash):
                self.crash_random(policy.max_crashed)
            if rng.chance(policy.partition):
                partition = self.partition_random()
                partition.apply()
                self._loop.call_later(rng.uniform(policy.min_duration, policy.max_duration), partition.heal)

    def __repr__(self) -> str:
        return "FaultInjector(crashed=%d of %d)" % (len(self.crashed), len(self._nodes))