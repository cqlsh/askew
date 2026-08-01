"""
The handle a test holds on a running node.

:meth:`~askew.sim.world.World.spawn` returns one of these and passes the same
object into the coroutine as its first argument, so the node writes its state
where the test reads it::

    async def replica(node, index):
        node.leader = None
        ...

    nodes = [world.spawn(replica, i) for i in range(5)]
    assert len({n.leader for n in nodes}) == 1

This is the one class in askew that deliberately does not declare ``__slots__``.
Everywhere else the tradeoff runs the other way, but a node stores arbitrary
user state under arbitrary names, and routing that through a ``__getattr__``
proxy onto a dictionary measures four times slower to read and two and a half
times slower to write than simply letting the instance keep the dictionary it
would have had anyway. The proxy saves about a hundred bytes per node; a
simulation has dozens of nodes and millions of attribute accesses.

The consequence is that node state and the handle's own attributes share one
namespace. The names a node reserves are ``id``, ``name``, ``alive``, ``task`` and its
methods; anything else is yours.
"""

from __future__ import annotations

from asyncio import Queue, QueueEmpty
from asyncio import timeout as async_timeout
from typing import TYPE_CHECKING, Any

from ..errors import NodeCrashed

if TYPE_CHECKING:
    from asyncio import Task
    from .net import Message, Network

class Node:
    """
    One participant in a simulation, and the state it carries.

     :ivar id: dense integer identifier, assigned in spawn order from zero
    :ivar name: label used in traces and in :func:`repr`
    :ivar alive: ``False`` once the node has been crashed
    :ivar task: the running coroutine, or ``None`` before it is spawned
    """

    def __init__(self, net: Network, identifier: int, name: str) -> None:
        self.id = identifier
        self.name = name
        self.alive = True
        self.task: Task[Any] | None = None
        self._net = net
        self._inbox: Queue[Message] = Queue()

    def send(self, target: Node | int, payload: Any) -> None:
        """
        Hand *payload* to the network, addressed at *target*.

        Returns immediately. Whether the message arrives, when, and in what
        order relative to others is the network's decision, drawn from the run's
        seed. A message across a partition boundary is dropped silently, which
        is what a real network does.
        """
        if not self.alive:
            raise NodeCrashed("node %d is down and cannot send" % self.id, self.id)
        self._net.send(self.id, target.id if isinstance(target, Node) else target, payload)

    def broadcast(self, payload: Any) -> None:
        """
        Send *payload* to every other node, this one excluded.

        Each copy is an independent message and gets its own latency and its own
        chance of being dropped, so a broadcast routinely reaches some nodes and
        not others. That is the point of having it.
        """
        self._net.broadcast(self.id, payload)

    async def recv(self, timeout: float | None = None) -> Message | None:
        """
        Wait for the next message in this node's mailbox.

        With no *timeout* this waits indefinitely, and a node that waits for a
        message nothing will ever send is reported as a deadlock rather than
        hanging the suite. With a *timeout* in simulated seconds, returns
        ``None`` once it expires, which is the usual shape of an election or
        heartbeat loop::

            message = await node.recv(timeout=5)
            if message is None:
                node.leader = node.id
        """
        if timeout is None:
            return await self._inbox.get()
        try:
            async with async_timeout(timeout):
                return await self._inbox.get()
        except TimeoutError:
            return None

    def try_recv(self) -> Message | None:
        """
        Take the next message if one is already waiting, otherwise ``None``.

        Never yields to the loop, so it cannot be used to wait. Useful for
        draining a mailbox before deciding what to do with it.
        """
        try:
            return self._inbox.get_nowait()
        except QueueEmpty:
            return None

    @property
    def pending(self) -> int:
        """
        Messages waiting in this node's mailbox.
        """
        return self._inbox.qsize()

    def deliver(self, message: Message) -> None:
        """
        Place *message* in the mailbox.

        The network's entry point into a node, not something node code calls. A
        crashed node accepts nothing; its messages are dropped on arrival,
        matching a process that is no longer there to read its socket.
        """
        if self.alive:
            self._inbox.put_nowait(message)

    def __repr__(self) -> str:
        return "Node(%d, %r, alive=%r, pending=%d)" % (self.id, self.name, self.alive, self._inbox.qsize())