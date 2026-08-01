"""
The simulated clock a test drives time with.

:class:`Clock` is a thin face on :class:`~askew.core.loop.SimLoop`, which owns
the actual time value. It exists so that a test says what it means -- letting
half an hour of simulated traffic happen is ``await world.clock.advance(minutes=30)``,
not a bare :func:`asyncio.sleep` that reads like the test is stalling.

The distinction that matters is that :meth:`Clock.advance` does not skip time.
It lets time pass. Every timer, retry, heartbeat and delivery scheduled inside
the interval fires, in the order the scheduler decides, before the call returns.
What makes it instant is that the loop never waits between those events, it jumps
from one deadline to the next.

Because advancing always schedules a timer of its own, it can never leave the
loop with nothing pending, and so it never trips the deadlock detector on its
own account. A deadlock reported during an advance is a real one.
"""

from __future__ import annotations

from asyncio import sleep as async_sleep
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .loop import SimLoop

class Clock:
    """
    Reads and advances the virtual time of one simulation run.

    :ivar now: absolute virtual time in seconds
    :ivar elapsed: virtual seconds since this clock was created
    """

    __slots__ = ("_loop", "_start")

    def __init__(self, loop: SimLoop) -> None:
        self._loop = loop
        self._start = loop.now

    @property
    def now(self) -> float:
        """
        Absolute virtual time, on the same scale as :meth:`asyncio.get_running_loop().time`.
        """
        return self._loop.now

    @property
    def elapsed(self) -> float:
        """
        Virtual seconds since this clock was created.

        Use this in assertions about duration. It stays correct if the run is
        later configured to start at a nonzero time, which absolute readings do
        not.
        """
        return self._loop.now - self._start

    async def advance(
            self,
            seconds: float = 0.0,
            milliseconds: float = 0.0,
            minutes: float = 0.0,
            hours: float = 0.0,
            days: float = 0.0
    ) -> float:
        """
        Let the given amount of simulated time pass, and return the new time.

        The units add up, so ``advance(minutes=1, seconds=30)`` is ninety
        seconds. A total of zero yields to the loop once, which is occasionally
        what you want in order to let a pending callback run without moving the
        clock.

        Everything scheduled within the interval happens before this returns.
        Cost is proportional to the number of events, not to the span, so
        ``advance(days=7)`` is no more expensive than ``advance(seconds=1)`` if
        nothing is scheduled in between.
        """
        total = seconds + milliseconds / 1000.0 + minutes * 60.0 + hours * 3600.0 + days * 86400.0
        await async_sleep(total)
        return self._loop.now

    async def sleep(self, seconds: float) -> float:
        """
        Let *seconds* of simulated time pass, and return the new time.

        The same thing :func:`asyncio.sleep` does on this loop. Prefer it inside
        node code, where the intent is that the node waits; prefer
        :meth:`advance` in the test body, where the intent is that the world
        moves on.
        """
        await async_sleep(seconds)
        return self._loop.now

    async def until(self, when: float) -> float:
        """
        Let time pass until the absolute virtual time *when*, and return it.

        Returns immediately if that moment has already passed, which makes it
        safe to use for a deadline computed earlier in the test.
        """
        remaining = when - self._loop.now
        if remaining > 0.0:
            await async_sleep(remaining)
        return self._loop.now

    def deadline(self, seconds: float) -> float:
        """
        Return the absolute virtual time *seconds* from now.

        Pairs with :meth:`until`, and with anything that wants an absolute
        instant rather than a duration.
        """
        return self._loop.now + seconds

    def __repr__(self) -> str:
        return "Clock(now=%.6fs, elapsed=%.6fs)" % (self._loop.now, self._loop.now - self._start)
