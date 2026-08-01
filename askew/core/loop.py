"""
An asyncio event loop with virtual time and a controlled callback order.

:class:`SimLoop` replaces two things and leaves the rest of asyncio alone: what
the clock reads, and in which order a tick's ready callbacks run. Task creation,
futures, cancellation and exception handling are the standard machinery, which
is why coroutines written against real asyncio run here unchanged.

Time never passes. It jumps. When the ready queue drains, the loop moves the
clock straight to the deadline of the earliest pending timer, so a coroutine
sleeping for thirty seconds resumes on the very next tick. That is where the
speed comes from, and it also produces something a real loop cannot observe: a
state with nothing ready and nothing scheduled is unambiguously a deadlock,
since there is no I/O that might still arrive. The loop reports it instead of
hanging.

The loop refuses the handful of asyncio APIs that would make a run
irreproducible: thread pools, name resolution, real sockets and readers on real
file descriptors. Anything else that needs a selector is not implemented by
:class:`~asyncio.BaseEventLoop` either and fails with its own error.

Subclassing :class:`~asyncio.BaseEventLoop` means inheriting its queues rather
than rebuilding them. ``call_soon`` fills ``_ready``, a deque of handles due
now; ``call_later`` and ``call_at`` fill ``_scheduled``, a heap of timer
handles; ``stop`` sets ``_stopping``, which ``run_forever`` reads; and
``_timer_handle_cancelled`` counts into ``_timer_cancelled_count``. None of that
is reimplemented here. Only what consumes those queues is, namely ``_run_once``,
together with ``time``, against which every deadline in them was computed.

That is a deliberate dependency on private attributes of the standard library.
It is the same set every third party loop implementation relies on and it has
been stable across releases, but it is not a promised interface. The test suite
asserts each of these attributes exists, so a rename in a future Python shows up
as one clear failure rather than as an AttributeError in the middle of a tick.
"""

from __future__ import annotations

from asyncio import BaseEventLoop, Handle, all_tasks
from heapq import heapify, heappop
from math import inf
from typing import TYPE_CHECKING, Any, NoReturn

from ..errors import (
    DeadlockError,
    NondeterminismError,
    StepLimitExceeded,
    TimeLimitExceeded
)

if TYPE_CHECKING:
    from .rng import Rng
    from .scheduler import Scheduler

class SimLoop(BaseEventLoop):
    """
    A deterministic event loop driving one simulation run.

    Construct one per run and drive it with
    :meth:`~asyncio.BaseEventLoop.run_until_complete`, exactly as you would a
    real loop. It is single use: once closed, build a new one for the next seed.

    The class declares ``__slots__`` for its own fields even though
    :class:`~asyncio.BaseEventLoop` carries a ``__dict__``. That does not save
    memory, but it turns access to the fields touched on every tick into a
    descriptor lookup instead of a dict lookup.

    :ivar now: current virtual time in seconds
    :ivar steps: callbacks executed so far, across the whole run
    """

    __slots__ = (
        "_now",
        "_rng",
        "_scheduler",
        "_reorders",
        "_steps",
        "_max_steps",
        "_max_time",
        "_detect_deadlock",
        "_batch"
    )

    PURGE_THRESHOLD = 100
    """
    Cancelled timers tolerated on the heap before it is rebuilt.

    A cancelled timer cannot be removed from a heap in place, so it lingers
    until it reaches the front. Code using :func:`asyncio.timeout` cancels one
    timer per successful operation, which without a periodic purge would grow
    the heap without bound over a long run.
    """

    def __init__(
            self,
            rng: Rng,
            scheduler: Scheduler,
            start_time: float = 0.0,
            max_steps: int = 0,
            max_time: float = 0.0,
            detect_deadlock: bool = True
    ) -> None:
        super().__init__()
        self._now = start_time
        self._rng = rng
        self._scheduler = scheduler
        self._reorders = scheduler.reorders
        self._steps = 0
        # Unlimited is stored as infinity rather than as zero with a guard, so
        # the per callback check stays a single comparison.
        self._max_steps: float = max_steps if max_steps > 0 else inf
        self._max_time: float = max_time if max_time > 0 else inf
        self._detect_deadlock = detect_deadlock
        self._batch: list[Handle] = []
        # Real asyncio widens every timer deadline by the resolution of the
        # system clock. Virtual time has no granularity, so a timer is due at
        # exactly the instant it says.
        self._clock_resolution = 0.0

    @property
    def now(self) -> float:
        """
        Current virtual time in seconds since the start of the run.
        """
        return self._now

    @property
    def steps(self) -> int:
        """
        Callbacks executed since the run began.

        A rough measure of how much work a scenario did, and the quantity
        :class:`~askew.errors.StepLimitExceeded` is counted against.
        """
        return self._steps

    def time(self) -> float:
        """
        Return the virtual clock, in place of :func:`time.monotonic`.
        """
        return self._now

    def _run_once(self) -> None:
        """
        Advance the simulation by one tick.

        A tick moves the clock to the earliest pending deadline if nothing is
        ready, promotes every timer that has come due, hands the resulting batch
        to the scheduler, and runs it. Callbacks queued while the batch runs land
        in the next tick, which is what keeps a tick a well defined unit for the
        scheduler to permute rather than a list growing underneath it.
        """
        scheduled = self._scheduled
        if self._timer_cancelled_count > self.PURGE_THRESHOLD:
            self._purge_cancelled_timers()
        else:
            while scheduled and scheduled[0]._cancelled:
                self._timer_cancelled_count -= 1
                handle = heappop(scheduled)
                handle._scheduled = False

        ready = self._ready
        if not ready:
            if scheduled:
                when = scheduled[0]._when
                if when > self._now:
                    if when > self._max_time:
                        raise TimeLimitExceeded(
                            "virtual time would reach %.6fs, past the limit of %.6fs" % (when, self._max_time)
                        )
                    self._now = when
            elif self._stopping:
                return
            elif self._detect_deadlock:
                raise DeadlockError(
                    "no callback is ready and no timer is pending at t=%.6fs, but the simulation has not finished" % self._now,
                    tuple(repr(task) for task in all_tasks(self))
                )
            else:
                self._stopping = True
                return

        now = self._now
        while scheduled and scheduled[0]._when <= now:
            handle = heappop(scheduled)
            handle._scheduled = False
            ready.append(handle)

        batch = self._batch
        batch.extend(ready)
        ready.clear()
        if self._reorders and len(batch) > 1:
            self._scheduler.order(batch, self._rng)

        steps = self._steps
        max_steps = self._max_steps
        for handle in batch:
            if handle._cancelled:
                continue
            steps += 1
            if steps > max_steps:
                self._steps = steps
                batch.clear()
                raise StepLimitExceeded(
                    "executed %d callbacks at t=%.6fs, past the limit of %d; a coroutine is most likely yielding without ever waiting"
                    % (steps, self._now, max_steps)
                )
            handle._run()
        self._steps = steps
        batch.clear()

    def _purge_cancelled_timers(self) -> None:
        """
        Rebuild the timer heap without its cancelled entries.

        The list is rewritten through a slice assignment so that the object
        :class:`~asyncio.BaseEventLoop` holds stays the same one.
        """
        scheduled = self._scheduled
        alive = []
        for handle in scheduled:
            if handle._cancelled:
                handle._scheduled = False
            else:
                alive.append(handle)
        scheduled[:] = alive
        heapify(scheduled)
        self._timer_cancelled_count = 0

    def _process_events(self, event_list: Any) -> None:
        """
        Do nothing. There is no selector and therefore no I/O to dispatch.
        """

    def _write_to_self(self) -> None:
        """
        Do nothing. Nothing can interrupt this loop, since nothing else runs.
        """

    def _refuse(self, what: str, instead: str) -> NoReturn:
        """
        Raise, explaining why *what* is unavailable and what to reach for.

        Declared :data:`~typing.NoReturn` so that callers need no ``raise`` of
        their own and a type checker still sees the method as terminating.
        """
        raise NondeterminismError(
            "%s is not available inside a simulation, because its result would not be reproducible from the seed. %s" % (what, instead)
        )

    def run_in_executor(self, executor: Any, func: Any, *args: Any) -> Any:
        self._refuse(
            "run_in_executor, and asyncio.to_thread with it",
            "Model the slow work as a coroutine that sleeps, or move it outside the simulation."
        )

    def add_reader(self, fd: Any, callback: Any, *args: Any) -> None:
        self._refuse(
            "add_reader",
            "Use the simulated network instead of a real file descriptor."
        )

    def add_writer(self, fd: Any, callback: Any, *args: Any) -> None:
        self._refuse(
            "add_writer",
            "Use the simulated network instead of a real file descriptor."
        )

    def remove_reader(self, fd: Any) -> bool:
        """
        Report that nothing was removed.

        Deliberately does not raise. These run in teardown paths, often inside a
        finally block, where an exception would mask the real failure.
        """
        return False

    def remove_writer(self, fd: Any) -> bool:
        """
        Report that nothing was removed, for the same reason as
        :meth:`remove_reader`.
        """
        return False

    async def getaddrinfo(self, host: Any, port: Any, **kwargs: Any) -> Any:
        self._refuse(
            "name resolution",
            "Address nodes by their identifier rather than by hostname."
        )

    async def getnameinfo(self, sockaddr: Any, flags: int = 0) -> Any:
        self._refuse(
            "name resolution",
            "Address nodes by their identifier rather than by hostname."
        )

    async def create_connection(self, *args: Any, **kwargs: Any) -> Any:
        self._refuse(
            "opening a real socket",
            "Send through the simulated network, which can delay, drop and reorder what you send."
        )

    async def create_server(self, *args: Any, **kwargs: Any) -> Any:
        self._refuse(
            "listening on a real socket",
            "Spawn a node and receive from its mailbox."
        )

    async def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> Any:
        self._refuse(
            "opening a real socket",
            "Send through the simulated network, which can delay, drop and reorder what you send."
        )

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        """
        Do nothing. No executor was ever created, since none can be.
        """

    def __repr__(self) -> str:
        return "SimLoop(t=%.6fs, steps=%d, scheduler=%r)" % (self._now, self._steps, self._scheduler)