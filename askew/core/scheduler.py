"""
Decides the order in which ready callbacks run within a tick.

This is the lever that makes askew explore rather than merely replay. Real
asyncio drains its ready queue first in, first out, which under virtual time
would produce exactly one interleaving per scenario -- deterministic, but blind
to every ordering bug that is not also a timing bug. A scheduler permutes that
queue instead, so the same scenario run under ten thousand seeds visits ten
thousand different interleavings.

There is a caveat worth knowing before reading a failure. asyncio documents
``call_soon`` as first in, first out, so code is entitled to rely on it, and
:class:`RandomScheduler` deliberately violates that guarantee. Most of what it
turns up is a genuine ordering assumption that was never true across tasks, but
not all of it. When a seed fails, rerun it under :class:`FifoScheduler`: if the
failure survives, ordering was not the cause and the bug is elsewhere; if it
disappears, the test depends on an order nothing promised it.

Schedulers permute in place and return nothing, so a tick costs no allocation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from asyncio import Handle
    from .rng import Rng

class Scheduler:
    """
    Base class for callback ordering strategies.

    Subclasses implement :meth:`order`, which rearranges a tick's batch of ready
    handles in place. The batch always holds at least two entries; the loop
    skips the call entirely below that.
    """

    __slots__ = ()

    reorders = True
    """
    Whether this strategy ever changes the order it is given.

    The loop reads this once at construction and, when it is ``False``, drops
    the per tick call altogether rather than paying for a method that returns
    immediately.
    """

    def order(self, batch: list[Handle], rng: Rng) -> None:
        """
        Rearrange *batch* in place.

        Draw every decision from *rng* and from nothing else, or the run stops
        being reproducible from its seed.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return "%s()" % type(self).__name__

class FifoScheduler(Scheduler):
    """
    Runs callbacks in registration order, exactly as real asyncio does.

    Useful as a control rather than as a way to find bugs: a scenario run under
    this scheduler visits one interleaving no matter how many iterations you
    give it. Its value is in the second half of a debugging session, where
    rerunning a failing seed under FIFO separates a real defect from a test that
    was relying on an order nothing guaranteed.

    Consumes no randomness, so a run under this scheduler and one under
    :class:`RandomScheduler` see the same latencies and the same faults.
    """

    __slots__ = ()

    reorders = False

    def order(self, batch: list[Handle], rng: Rng) -> None:
        """
        Do nothing. The batch already carries the order the loop queued it in.
        """

class RandomScheduler(Scheduler):
    """
    Permutes each tick uniformly over all orderings.

    The default, and the one that finds things. Every ready callback is equally
    likely to run first, which means a scenario repeated under enough seeds will
    eventually schedule any pair of tasks in either order.

    Costs one random draw per callback in the batch, making it the single
    largest consumer of randomness in a run.
    """

    __slots__ = ()

    def order(self, batch: list[Handle], rng: Rng) -> None:
        rng.shuffle(batch)

class ReverseScheduler(Scheduler):
    """
    Runs each tick's callbacks in exactly the opposite order.

    The adversarial counterpart to :class:`FifoScheduler`, and like it fully
    deterministic -- it consumes no randomness at all. Code that assumes
    registration order fails here on the first iteration rather than somewhere
    in the first few hundred, which makes it a fast smoke test before committing
    to a long run.
    """

    __slots__ = ()

    def order(self, batch: list[Handle], rng: Rng) -> None:
        batch.reverse()

class PartialScheduler(Scheduler):
    """
    Applies a few random transpositions and otherwise leaves the order alone.

    A middle ground between FIFO and a full permutation. The result stays close
    to registration order with local disturbances, which resembles what a real
    scheduler under load actually does -- occasional inversions between
    neighbours rather than a wholesale reshuffle.

    It is also cheaper. A full shuffle draws once per callback; this draws twice
    per swap, so at the default intensity a batch of sixty-four costs thirty-two
    draws instead of sixty-three. The tradeoff is coverage: distant callbacks
    rarely trade places, so an ordering bug between two tasks that are far apart
    in the queue takes far more iterations to surface.

    :ivar intensity: swaps performed per callback in the batch
    """

    __slots__ = ("intensity",)

    def __init__(self, intensity: float = 0.25) -> None:
        self.intensity = intensity

    def order(self, batch: list[Handle], rng: Rng) -> None:
        size = len(batch)
        below = rng.below
        for _ in range(int(size * self.intensity)):
            i = below(size)
            j = below(size)
            batch[i], batch[j] = batch[j], batch[i]

    def __repr__(self) -> str:
        return "PartialScheduler(intensity=%r)" % self.intensity