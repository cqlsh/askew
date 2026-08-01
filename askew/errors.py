"""
Exceptions raised by askew.

Every error the simulator produces derives from :class:`AskewError`, so a test
harness can catch the entire family with a single except clause.  The errors
split into three groups:

* :class:`SimulationFailure` wraps whatever your test raised and carries the
  seed needed to replay the exact run that failed.
* :class:`DeadlockError`, :class:`StepLimitExceeded` and
  :class:`TimeLimitExceeded` are raised by the loop when a run cannot make
  progress, or refuses to stop making it.
* :class:`NondeterminismError` is raised when user code reaches for something
  the simulator cannot control, such as a thread pool or a real socket.

Three deliberate choices keep these cheap to raise.  Each class declares
``__slots__``, which does not drop the ``__dict__`` inherited from
:class:`BaseException` but does turn attribute access into a descriptor lookup.
The summary line is written straight into ``args`` rather than routed through
``BaseException.__init__``, and is read back through a property, so construction
performs one store fewer.  The formatted message is assembled in :meth:`__str__`
rather than in the constructor, so raising costs nothing beyond storing fields.

Sequence arguments are stored without copying.  These exceptions are only ever
constructed by askew itself and always with tuples, so the defensive copy would
buy nothing.
"""

from __future__ import annotations

from typing import Any

class AskewError(Exception):
    """
    Base class for all exceptions raised by askew.
    """

    __slots__ = ()

class SimulationFailure(AskewError):
    """
    A simulation run failed.

    Raised by the decorated test function once an iteration has produced an
    error.  The original exception is available as *cause* and is also chained
    onto this one, so a traceback shows both. Replay the exact run with::

        askew.Replay.run(test_leader_election, seed=failure.seed)

    :ivar summary: one line describing what went wrong, without the run details
    :ivar seed: the seed that produced this failure; feed it back to reproduce
    :ivar iteration: which iteration failed, counting from zero
    :ivar virtual_time: simulated seconds elapsed when the failure surfaced
    :ivar steps: callbacks the loop had executed when the failure surfaced
    :ivar cause: the original exception raised by the test, or ``None``
    :ivar trace: formatted traceback of *cause*, captured before teardown
    :ivar events: tail of the event log, as a tuple of formatted strings
    """

    __slots__ = (
        "seed",
        "iteration",
        "virtual_time",
        "steps",
        "cause",
        "trace",
        "events"
    )

    def __init__(
            self,
            summary: str,
            seed: int | None = None,
            iteration: int = 0,
            virtual_time: float = 0.0,
            steps: int = 0,
            cause: BaseException | None = None,
            trace: str = "",
            events: tuple[str, ...] = ()
    ) -> None:
        self.args = (summary,)
        self.seed = seed
        self.iteration = iteration
        self.virtual_time = virtual_time
        self.steps = steps
        self.cause = cause
        self.trace = trace
        self.events = events

    @property
    def summary(self) -> str:
        return self.args[0]

    def __str__(self) -> str:
        return "%s (seed=%r, iteration=%d, t=%.6fs, steps=%d)" % (
            self.args[0], self.seed, self.iteration, self.virtual_time, self.steps
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return self.__class__, (
            self.args[0], self.seed, self.iteration, self.virtual_time, self.steps, self.cause, self.trace, self.events
        )

class DeadlockError(AskewError):
    """
    The loop ran out of work while the simulation was still unfinished.

    No callback was ready to run and no timer was pending, yet the main
    coroutine had not completed. In a real event loop this is the state where
    the process would block on the selector forever; under virtual time there is
    nothing left to wait for, so the condition is reported immediately instead.

    :ivar pending: repr strings for the tasks that were still awaiting something
    """

    __slots__ = ("pending",)

    def __init__(self, summary: str, pending: tuple[str, ...] = ()) -> None:
        self.args = (summary,)
        self.pending = pending

    def __reduce__(self) -> tuple[Any, ...]:
        return self.__class__, (self.args[0], self.pending)

class StepLimitExceeded(AskewError):
    """
    The run executed more callbacks than ``max_steps`` permits.

    Usually a busy loop: a coroutine that yields without ever waiting on a timer
    spins forever, because virtual time only advances once the ready queue has
    drained.
    """

    __slots__ = ()

class TimeLimitExceeded(AskewError):
    """
    Virtual time advanced past ``max_time``.

    Raise the limit in :class:`~askew.testing.config.SimulationConfig` if the
    scenario legitimately spans simulated days; the clock jumps rather than
    ticks, so a long horizon costs nothing by itself.
    """

    __slots__ = ()

class NondeterminismError(AskewError):
    """
    User code reached for something the simulator cannot make deterministic.

    Thread pools, real sockets and the wall clock all break replay, since the
    same seed would no longer produce the same run. The loop refuses these up
    front rather than letting a test decay into a flaky one.
    """

    __slots__ = ()

class NodeCrashed(AskewError):
    """
    An operation was attempted on a node that has been crashed.

    The crash itself reaches node code as a :exc:`~asyncio.CancelledError`,
    which is what a process being killed looks like from inside. This is the
    other half: code that survived the cancellation and tried to keep working
    gets told plainly, instead of sending into a network that would drop it.

    :ivar node_id: identifier of the node that was crashed
    """

    __slots__ = ("node_id",)

    def __init__(self, summary: str, node_id: int | None = None) -> None:
        self.args = (summary,)
        self.node_id = node_id

    def __reduce__(self) -> tuple[Any, ...]:
        return self.__class__, (self.args[0], self.node_id)

class Unreachable(AskewError):
    """
    A send targeted a node the sender cannot currently reach.

    Only raised when the network is configured with ``raise_on_partition``. By
    default a message crossing a partition boundary is dropped silently, which
    is what a real network does.
    """

    __slots__ = ()