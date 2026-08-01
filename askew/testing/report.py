"""
What one iteration produced.

A :class:`Report` is built after every iteration, passing or failing, and is the
only thing the runner keeps from a run it has finished with. It carries the
numbers a failure needs in order to be understood without being rerun: how far
virtual time got, how much work happened, what the network did, and the tail of
the trace.

It holds no reference to the world it came from, on purpose. A run of ten
thousand iterations builds ten thousand of these, and a report that kept its
world would keep every node, every mailbox and every closed loop alive with it.
Everything interesting is copied out at construction; the world is then free.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..sim.world import World

class Report:
    """
    The outcome of a single iteration.

    :ivar seed: the seed this iteration ran under
    :ivar iteration: its index within the run, counting from zero
    :ivar error: what went wrong, or ``None`` if it passed
    :ivar trace: formatted traceback of *error*, captured before teardown
    :ivar virtual_time: simulated seconds elapsed when it ended
    :ivar steps: callbacks the loop executed
    :ivar events: tail of the event log, empty unless tracing was on
    :ivar nodes: how many nodes were spawned
    :ivar crashed: how many of them were down at the end
    :ivar sent: messages the network accepted
    :ivar delivered: messages that reached a mailbox
    :ivar dropped: messages lost to loss, to a partition or to a dead node
    """

    __slots__ = (
        "seed",
        "iteration",
        "error",
        "trace",
        "virtual_time",
        "steps",
        "events",
        "nodes",
        "crashed",
        "sent",
        "delivered",
        "dropped"
    )

    def __init__(
            self,
            seed: int,
            iteration: int = 0,
            error: BaseException | None = None,
            trace: str = "",
            virtual_time: float = 0.0,
            steps: int = 0,
            events: tuple[str, ...] = (),
            nodes: int = 0,
            crashed: int = 0,
            sent: int = 0,
            delivered: int = 0,
            dropped: int = 0
    ) -> None:
        self.seed = seed
        self.iteration = iteration
        self.error = error
        self.trace = trace
        self.virtual_time = virtual_time
        self.steps = steps
        self.events = events
        self.nodes = nodes
        self.crashed = crashed
        self.sent = sent
        self.delivered = delivered
        self.dropped = dropped

    @classmethod
    def of(
            cls,
            world: World,
            iteration: int = 0,
            error: BaseException | None = None,
            trace: str = "",
            lines: int = 20
    ) -> Report:
        """
        Build a report from a world that has finished running.

        Reads everything it needs and keeps nothing, so the world can be torn
        down immediately afterwards.
        """
        return cls(
            world.seed,
            iteration,
            error,
            trace,
            world.loop.now,
            world.loop.steps,
            world.log.tail(lines),
            len(world.nodes),
            len(world.faults.crashed),
            world.net.sent,
            world.net.delivered,
            world.net.dropped
        )

    @property
    def ok(self) -> bool:
        """
        Whether the iteration passed.
        """
        return self.error is None

    def summary(self) -> str:
        """
        Return the one line that identifies this iteration.
        """
        return "seed=%d iteration=%d t=%.6fs steps=%d" % (self.seed, self.iteration, self.virtual_time, self.steps)

    def __str__(self) -> str:
        lines = [
            "%s  %s" % ("PASS" if self.ok else "FAIL", self.summary()),
            "  nodes %d, %d crashed" % (self.nodes, self.crashed),
            "  messages %d sent, %d delivered, %d dropped" % (self.sent, self.delivered, self.dropped)
        ]
        if self.error is not None:
            lines.append("  %s: %s" % (type(self.error).__name__, self.error))
        if self.events:
            lines.append("  recent events")
            lines.extend("    " + line for line in self.events)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return "Report(seed=%d, iteration=%d, ok=%r)" % (self.seed, self.iteration, self.ok)