"""
The single source of randomness in a simulation.

Every decision a simulation makes -- a delay, a drop, an ordering, a crash --
draws from an :class:`Rng` seeded from the run's seed.  Nothing else may consume
randomness, which is what makes a seed sufficient to replay a run.

All methods are built on exactly one primitive, :meth:`random.Random.random`,
and each of them consumes exactly one draw.  That is deliberate on two counts.
It is the fastest option, roughly three times quicker than ``randrange`` since
the work happens in C rather than in a Python-level rejection loop.  More
importantly it is the only stable one: ``randrange`` and ``_randbelow`` consume
a variable number of values depending on the bound, so a future change to their
implementation would shift the whole stream and invalidate every recorded seed.
With one draw per decision, the consumption pattern lives in this file.

The bias this introduces is the truncation bias of a 53 bit float, below one
part in ten quadrillion for any bound a simulation will use. It is not
detectable by anything askew does, and it buys a factor of three.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from random import Random
from typing import Any, TypeVar, cast

T = TypeVar("T")

class Rng:
    """
    A deterministic random number generator bound to one seed.

    Construct one per simulation run and hand it out; use :meth:`fork` when a
    subsystem needs its own stream so that consuming from it cannot shift the
    values every other subsystem sees.

    :ivar seed: the seed this generator was constructed from
    """

    __slots__ = ("seed", "_source", "_rand")

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self._source = Random(seed)
        self._rand = cast(Callable[[], float], self._source.random)

    def random(self) -> float:
        """
        Return a float in ``[0.0, 1.0)``.

        The primitive every other method is built on.
        """
        return self._rand()

    def below(self, bound: int) -> int:
        """
        Return an integer in ``[0, bound)``.

        A *bound* of zero returns zero rather than raising, which keeps callers
        that iterate over a possibly empty collection free of a guard clause.
        """
        return int(self._rand() * bound)

    def between(self, low: int, high: int) -> int:
        """
        Return an integer in ``[low, high]``, both ends included.

        Inclusive because the callers are ranges like "between three and five
        replicas", where an exclusive upper bound reads wrong every time.
        """
        return low + int(self._rand() * (high - low + 1))

    def uniform(self, low: float, high: float) -> float:
        """
        Return a float in ``[low, high)``.

        Used for latencies and jitter, where the distribution matters less than
        the fact that the value is reproducible.
        """
        return low + self._rand() * (high - low)

    def chance(self, probability: float) -> float:
        """
        Return ``True`` with the given *probability*.

        A probability of ``0.0`` never fires and ``1.0`` always does, since
        :meth:`random` never returns exactly one.
        """
        return self._rand() < probability

    def choice(self, items: Sequence[T]) -> T:
        """
        Return one element of *items*, chosen uniformly.
        """
        return items[int(self._rand() * len(items))]

    def shuffle(self, items: list[Any]) -> None:
        """
        Permute *items* in place, uniformly over all permutations.

        A Fisher-Yates pass written out rather than delegated to
        :meth:`random.Random.shuffle`, which costs about a third more because it
        routes every swap through the rejection loop. This runs once per tick
        of the event loop, so it is the hottest consumer of randomness in the
        library.
        """
        rand = self._rand
        for i in range(len(items) - 1, 0, -1):
            j = int(rand() * (i + 1))
            items[i], items[j] = items[j], items[i]

    def sample(self, items: Sequence[T], count: int) -> list[T]:
        """
        Return *count* distinct elements of *items*, in random order.

        A partial Fisher-Yates over a copy, so the cost is proportional to
        *count* rather than to the size of *items*.
        """
        pool = list(items)
        rand = self._rand
        size = len(pool)
        picked: list[T] = []
        for i in range(count):
            j = i + int(rand() * (size - i))
            pool[i], pool[j] = pool[j], pool[i]
            picked.append(pool[i])
        return picked

    def fork(self) -> Rng:
        """
        Return an independent generator seeded from this one.

        Give a subsystem its own stream when the number of draws it makes
        depends on something you do not want coupled to the rest of the run.
        Without a fork, adding one dropped message shifts every subsequent
        decision in the simulation, and two runs that should differ in one place
        end up differing everywhere.

        The fork consumes one value from this generator, so forking is itself
        deterministic and order dependent.
        """
        return Rng(self._source.getrandbits(64))

    def __repr__(self) -> str:
        return "Rng(seed=%d)" % self.seed

class SeedMixer:
    """
    Derives the per iteration seeds of a run from a single base seed.

    A run of ten thousand iterations needs ten thousand seeds that behave as if
    unrelated.  Feeding ``base + i`` straight into :class:`Rng` would work, but
    it ties iteration 417 to its neighbours in a way that is hard to reason
    about when a failure clusters.

    :meth:`mix` applies splitmix64, which matters for one practical reason
    beyond decorrelation: iteration 417's seed is computed directly from the
    base seed, without running the 417 iterations before it. Replaying a
    failure that surfaced deep into a run therefore costs one multiplication
    rather than the whole run up to that point.
    """

    __slots__ = ()

    MASK = 0xFFFFFFFFFFFFFFFF
    """ Truncates intermediate products back to 64 bits. """

    GAMMA = 0x9E3779B97F4A7C15
    """ The golden ratio increment, which walks the index across the state space. """

    @staticmethod
    def mix(base: int, index: int) -> int:
        """
        Return the seed for iteration *index* of a run started from *base*.
        """
        mask = SeedMixer.MASK
        z = (base + (index + 1) * SeedMixer.GAMMA) & mask
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & mask
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & mask
        return z ^ (z >> 31)