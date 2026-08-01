"""
The world a test is handed, and the wiring behind it.

:class:`World` owns one run. It builds the generator, the loop, the clock, the
trace, the network and the fault injector, hands them a shared list of nodes,
and exposes the handful of verbs a test actually uses::

    @askew.simulate(seed=1337, iterations=10_000)
    async def test_leader_election(world):
        nodes = [world.spawn(node_main, i) for i in range(5)]
        async with world.partition({0, 1}, {2, 3, 4}):
            await world.clock.advance(seconds=30)
        assert len({n.leader for n in nodes}) == 1

The construction order in :meth:`__init__` is the reason this module comes last
in its package. The node list is created empty and handed to the network and the
injector before a single node exists; all three then share the same list object,
which is what lets :meth:`spawn` simply append. Because identifiers are dense
and assigned from zero, delivery is an index into that list rather than a
lookup.

A world takes plain arguments rather than a configuration object, since
:mod:`askew.testing` sits above this layer and a world must not reach up into
it. The runner unpacks its configuration here.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from ..core.clock import Clock
from ..core.log import EventLog
from ..core.loop import SimLoop
from ..core.rng import Rng
from .faults import FaultInjector
from .net import LinkConfig, Network
from .node import Node

if TYPE_CHECKING:
    from asyncio import Task
    from ..core.scheduler import Scheduler
    from .faults import FaultPolicy
    from .net import Partition

class World:
    """
    One simulation run, and everything a test can do to it.

    :ivar seed: the seed this run was built from
    :ivar random: the run's generator; draw from it so test data is part of the seed
    :ivar loop: the deterministic event loop
    :ivar clock: virtual time, and the way to advance it
    :ivar log: the event trace attached to a failure
    :ivar net: the simulated network
    :ivar faults: crashes and partitions, on request or at random
    :ivar nodes: every node spawned so far, indexed by identifier
    :ivar node_errors: exceptions raised by node tasks, in the order they surfaced
    """

    RESERVED = frozenset(("id", "name", "alive", "task", "_net", "_inbox"))
    """
    Attribute names a node owns, which :meth:`restart` must not wipe.
    """

    __slots__ = (
        "seed",
        "random",
        "loop",
        "clock",
        "log",
        "net",
        "faults",
        "nodes",
        "node_errors",
        "_spawned",
        "_chaos"
    )

    def __init__(
            self,
            seed: int,
            scheduler: Scheduler,
            link: LinkConfig | None = None,
            start_time: float = 0.0,
            max_steps: int = 0,
            max_time: float = 0.0,
            detect_deadlock: bool = True,
            trace: bool = False,
            trace_limit: int = 256,
            raise_on_partition: bool = False
    ) -> None:
        self.seed = seed
        self.random = Rng(seed)
        self.loop = SimLoop(
            self.random,
            scheduler,
            start_time,
            max_steps,
            max_time,
            detect_deadlock
        )
        self.clock = Clock(self.loop)
        self.log = EventLog(self.loop, trace, trace_limit)
        self.nodes: list[Node] = []
        self.net = Network(
            self.random,
            self.loop,
            self.log,
            self.nodes,
            link if link is not None else LinkConfig(),
            raise_on_partition
        )
        self.faults = FaultInjector(self.random, self.loop, self.log, self.net, self.nodes)
        self.node_errors: list[BaseException] = []
        self._spawned: dict[int, tuple[Callable[..., Coroutine[Any, Any, Any]], tuple[Any, ...]]] = {}
        self._chaos: Task[Any] | None = None

    @property
    def now(self) -> float:
        """
        Current virtual time in seconds.
        """
        return self.loop.now

    def spawn(
            self,
            target: Callable[..., Coroutine[Any, Any, Any]],
            *args: Any,
            name: str | None = None
    ) -> Node:
        """
        Start a node and return its handle.

        *target* is called as ``target(node, *args)``. The handle arrives as the
        first argument so that the coroutine can write state the test will read
        back off the same object.

        The node starts on the next tick, not during this call, so a test can
        spawn a whole cluster before any of it runs.

        Exceptions from a node's task are collected in :attr:`node_errors`
        rather than being logged and forgotten, which is what asyncio would do
        with them. The runner fails the iteration on any of them.
        """
        identifier = len(self.nodes)
        label = name if name is not None else "%s-%d" % (getattr(target, "__name__", "node"), identifier)
        node = Node(self.net, identifier, label)
        self.nodes.append(node)
        self._spawned[identifier] = (target, args)
        task = self.loop.create_task(target(node, *args), name=label)
        task.add_done_callback(self._node_finished)
        node.task = task
        self.log.add("node", "spawned %s", label)
        return node

    def restart(self, node: Node, fresh: bool = False) -> Node:
        """
        Bring a crashed node back, running the same coroutine again.

        Its mailbox starts empty, since whatever was in flight when it died is
        gone. With *fresh* the node's own attributes are wiped first, modelling
        a process that came back with nothing; by default they survive, which
        models one that recovered its state from disk.

        Does nothing to a node that is already alive.
        """
        if node.alive:
            return node
        if fresh:
            reserved = World.RESERVED
            for attribute in [key for key in node.__dict__ if key not in reserved]:
                del node.__dict__[attribute]
        node.alive = True
        if node in self.faults.crashed:
            self.faults.crashed.remove(node)
        target, args = self._spawned[node.id]
        task = self.loop.create_task(target(node, *args), name=node.name)
        task.add_done_callback(self._node_finished)
        node.task = task
        self.log.add("node", "restarted %s", node.name)
        return node

    def crash(self, node: Node) -> None:
        """
        Kill *node*. Shorthand for :meth:`FaultInjector.crash`.
        """
        self.faults.crash(node)

    def partition(self, *groups: set[Node | int] | frozenset[Node | int]) -> Partition:
        """
        Split the network, as a context manager.

        Nodes in different groups cannot reach each other, and anything not
        named forms one further group of its own::

            async with world.partition({0, 1}, {2, 3, 4}):
                await world.clock.advance(seconds=30)
        """
        return self.net.partition(*groups)

    def start_chaos(self, policy: FaultPolicy) -> Task[Any]:
        """
        Run :meth:`FaultInjector.chaos` in the background for the rest of the run.

        Remember that a running chaos loop always holds a pending timer, which
        suppresses deadlock detection. Reproduce a failing seed without it.
        """
        task = self.loop.create_task(self.faults.chaos(policy), name="chaos")
        self._chaos = task
        return task

    async def sleep(self, seconds: float) -> float:
        """
        Let *seconds* of simulated time pass. Shorthand for :meth:`Clock.sleep`.
        """
        return await self.clock.sleep(seconds)

    def node(self, identifier: int) -> Node:
        """
       Return the node with the given identifier.
       """
        return self.nodes[identifier]

    def alive(self) -> list[Node]:
        """
        Return every node that has not been crashed.
        """
        return [node for node in self.nodes if node.alive]

    def cancel_all(self) -> None:
        """
        Cancel every node task and the chaos loop.

        Called during teardown. Nodes normally run forever, so a run ends with
        tasks still pending and they have to be told to stop before the loop can
        close cleanly.
        """
        if self._chaos is not None:
            self._chaos.cancel()
        for node in self.nodes:
            if node.task is not None:
                node.task.cancel()

    def _node_finished(self, task: Task[Any]) -> None:
        """
        Record why a node's task ended. Attached to every task spawned here.
        """
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self.node_errors.append(error)
            self.log.add("node", "task failed: %r", error)

    def __repr__(self) -> str:
        return "World(seed=%d, t=%.6fs, nodes=%d, crashed=%d)" % (self.seed, self.loop.now, len(self.nodes), len(self.faults.crashed))