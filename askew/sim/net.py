"""
The network between nodes: latency, loss, reordering and partitions.

Nothing here moves a message directly. A send draws a delay from the run's
generator and schedules the delivery on the loop, which means every message
competes with every timer and every other message for its place in the order.
Two messages sent in the same tick routinely arrive in the opposite order, not
because the network shuffles them but because they were given different delays.

Reachability is checked twice, once when the message is sent and once when it is
about to be delivered. A partition that forms while a message is in flight
therefore swallows it, which is what a real one does and what makes the window
around a partition interesting to test.

Partitions divide the nodes into groups, and nodes in different groups cannot
reach each other. Any node not named in the call belongs to one implicit
remainder group, so ``partition({0, 1})`` cuts nodes zero and one off from
everybody else, and ``isolate(node)`` is the same statement about a single node.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..errors import Unreachable

if TYPE_CHECKING:
    from ..core.log import EventLog
    from ..core.loop import SimLoop
    from ..core.rng import Rng
    from .node import Node

class Message:
    """
    One message in flight, or one that has arrived.

    :ivar source: identifier of the sending node
    :ivar target: identifier of the receiving node
    :ivar payload: whatever the sender passed; askew never inspects it
    :ivar sent_at: virtual time the send was issued
    :ivar deliver_at: virtual time delivery was scheduled for
    :ivar seq: monotonic counter over every message the network has accepted
    """

    __slots__ = (
        "source",
        "target",
        "payload",
        "sent_at",
        "deliver_at",
        "seq"
    )

    def __init__(self, source: int, target: int, payload: Any, sent_at: float, deliver_at: float, seq: int) -> None:
        self.source = source
        self.target = target
        self.payload = payload
        self.sent_at = sent_at
        self.deliver_at = deliver_at
        self.seq = seq

    @property
    def latency(self) -> float:
        """
        Simulated seconds this message spent in flight.
        """
        return self.deliver_at - self.sent_at

    def __repr__(self) -> str:
        return "Message(#%d, %d -> %d, %r)" % (self.seq, self.source, self.target, self.payload)

class LinkConfig:
    """
    How one link behaves, or how every link behaves by default.

    Latency is drawn uniformly from the range on every send, which is what
    produces reordering: two messages leaving together arrive apart.

    :ivar min_latency: shortest possible delivery delay, in simulated seconds
    :ivar max_latency: longest possible delivery delay, in simulated seconds
    :ivar loss: probability a message is discarded instead of scheduled
    :ivar duplicate: probability a delivered message arrives a second time
    """

    __slots__ = (
        "min_latency",
        "max_latency",
        "loss",
        "duplicate"
    )

    def __init__(
            self,
            min_latency: float = 0.001,
            max_latency: float = 0.010,
            loss: float = 0.0,
            duplicate: float = 0.0
    ) -> None:
        self.min_latency = min_latency
        self.max_latency = max_latency
        self.loss = loss
        self.duplicate = duplicate

    def __repr__(self) -> str:
        return "LinkConfig(latency=%.4f..%.4f, loss=%.3f, duplicate=%.3f)" % (self.min_latency, self.max_latency, self.loss, self.duplicate)

class Partition:
    """
    A split in the network, active for as long as it is held.

    Returned by :meth:`Network.partition` and :meth:`Network.isolate`. Normally
    used as a context manager, which heals the split on the way out even if the
    body raised::

        async with world.partition({0, 1}, {2, 3, 4}):
            await world.clock.advance(seconds=30)

    :ivar groups: the split, as a mapping from node identifier to group number
    """

    __slots__ = ("_net", "groups", "_active")

    def __init__(self, net: Network, groups: dict[int, int]) -> None:
        self._net = net
        self.groups = groups
        self._active = False

    def apply(self) -> Partition:
        """
        Bring the split into effect, if it is not already.
        """
        if not self._active:
            self._net.activate(self)
            self._active = True
        return self

    def heal(self) -> None:
        """
        Remove this split. Messages already in flight across it still drop.
        """
        if self._active:
            self._net.deactivate(self)
            self._active = False

    def __enter__(self) -> Partition:
        return self.apply()

    def __exit__(self, *exc: Any) -> None:
        self.heal()

    async def __aenter__(self) -> Partition:
        return self.apply()

    async def __aexit__(self, *exc: Any) -> None:
        self.heal()

    def __repr__(self) -> str:
        return "Partition(%r, active=%r)" % (self.groups, self._active)

class Network:
    """
    Carries messages between nodes, badly and reproducibly.

    :ivar sent: messages accepted for delivery
    :ivar delivered: messages that reached a mailbox
    :ivar dropped: messages lost to loss, to a partition or to a dead node
    :ivar duplicated: extra copies delivered
    """

    __slots__ = (
        "_rng",
        "_loop",
        "_log",
        "_nodes",
        "_link",
        "_links",
        "_partitions",
        "_seq",
        "raise_on_partition",
        "sent",
        "delivered",
        "dropped",
        "duplicated"
    )

    def __init__(
            self,
            rng: Rng,
            loop: SimLoop,
            log: EventLog,
            nodes: list[Node],
            link: LinkConfig | None = None,
            raise_on_partition: bool = False
    ) -> None:
        self._rng = rng
        self._loop = loop
        self._log = log
        self._nodes = nodes
        self._link = link if link is not None else LinkConfig()
        self._links: dict[tuple[int, int], LinkConfig] = {}
        self._partitions: list[dict[int, int]] = []
        self._seq = 0
        self.raise_on_partition = raise_on_partition
        self.sent = 0
        self.delivered = 0
        self.dropped = 0
        self.duplicated = 0

    def configure(self, source: Node | int, target: Node | int, link: LinkConfig) -> None:
        """
        Give one direction of one link its own behaviour.

        Applies to *source* to *target* only. Call it twice to make a link slow
        or lossy in both directions, which is rarely what a real fault looks
        like anyway.
        """
        self._links[(self.identify(source), self.identify(target))] = link

    def reachable(self, source: int, target: int) -> bool:
        """
        Whether *source* can currently reach *target*.

        Cheap when nothing is partitioned, which is the common case: the loop
        below does not run at all.
        """
        for groups in self._partitions:
            if groups.get(source, -1) != groups.get(target, -1):
                return False
        return True

    def send(self, source: int, target: int, payload: Any) -> None:
        """
        Accept *payload* for delivery, or drop it.

        Decides loss and latency now and schedules the arrival; whether it truly
        arrives is settled again at that later moment.
        """
        self.sent += 1
        link = self._links.get((source, target), self._link) if self._links else self._link

        if not self.reachable(source, target):
            self.dropped += 1
            self._log.add("net", "%d -> %d dropped: partitioned", source, target)
            if self.raise_on_partition:
                raise Unreachable("node %d cannot reach node %d" % (source, target))
            return

        if link.loss > 0.0 and self._rng.chance(link.loss):
            self.dropped += 1
            self._log.add("net", "%d -> %d dropped: lost", source, target)
            return

        self._seq += 1
        now = self._loop.now
        delay = self._rng.uniform(link.min_latency, link.max_latency)
        message = Message(source, target, payload, now, now + delay, self._seq)
        self._loop.call_later(delay, self._deliver, message)

        if link.duplicate > 0.0 and self._rng.chance(link.duplicate):
            self.duplicated += 1
            extra = self._rng.uniform(link.min_latency, link.max_latency)
            self._loop.call_later(extra, self._deliver, message)

    def broadcast(self, source: int, payload: Any) -> None:
        """
        Send *payload* to every node other than *source*.

        Each copy is an independent message with its own delay and its own
        chance of being lost, so a broadcast regularly reaches some nodes and
        not others.
        """
        for node in self._nodes:
            if node.id != source:
                self.send(source, node.id, payload)

    def partition(self, *groups: set[Node | int] | frozenset[Node | int]) -> Partition:
        """
        Split the network so that nodes in different groups cannot reach each other.

        Nodes not named in any group form one further group of their own, which
        is why a single group is a meaningful call: ``partition({0, 1})`` cuts
        zero and one off from everything else.

        The split takes effect when the returned object is entered or applied,
        not when it is created.
        """
        mapping: dict[int, int] = {}
        for index, group in enumerate(groups):
            for member in group:
                mapping[self.identify(member)] = index
        return Partition(self, mapping)

    def isolate(self, node: Node | int) -> Partition:
        """
        Cut a single node off from every other node.
        """
        return self.partition({self.identify(node)})

    def heal(self) -> None:
        """
        Remove every active partition at once.

        A blunt instrument for the end of a scenario. Prefer letting each
        :class:`Partition` leave its own context.
        """
        self._partitions.clear()
        self._log.add("net", "all partitions healed")

    def activate(self, partition: Partition) -> None:
        """
        Register *partition* as in effect. Called by the partition itself.
        """
        self._partitions.append(partition.groups)
        self._log.add("net", "partitioned %r", partition.groups)

    def deactivate(self, partition: Partition) -> None:
        """
        Withdraw *partition*. Called by the partition itself.
        """
        try:
            self._partitions.remove(partition.groups)
        except ValueError:
            return
        self._log.add("net", "healed %r", partition.groups)

    @staticmethod
    def identify(node: Node | int) -> int:
        """
        Return the identifier of *node*, which may already be one.
        """
        return node if isinstance(node, int) else node.id

    def _deliver(self, message: Message) -> None:
        """
        Hand *message* to its target, or drop it. Runs as a loop callback.

        The reachability test is repeated here on purpose. A partition raised
        after the send but before the arrival takes the message with it.
        """
        target = self._nodes[message.target]
        if not self.reachable(message.source, message.target):
            self.dropped += 1
            self._log.add("net", "%d -> %d dropped in flight: partitioned", message.source, message.target)
            return
        if not target.alive:
            self.dropped += 1
            self._log.add("net", "%d -> %d dropped: node %d is down", message.source, message.target, message.target)
            return
        self.delivered += 1
        self._log.add("net", "%d -> %d delivered %r", message.source, message.target, message.payload)
        target.deliver(message)

    def __repr__(self) -> str:
        return "Network(sent=%d, delivered=%d, dropped=%d, partitions=%d)" % (self.sent, self.delivered, self.dropped, len(self._partitions))