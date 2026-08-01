"""
The event trace shown when a run fails.

A failing seed is reproducible, which changes what a log has to be. There is no
need to record anything during the ten thousand runs that pass, because the one
that fails can simply be run a second time with recording switched on. Logging
draws nothing from the generator, so that second run is the same run, event for
event.

:class:`EventLog` is therefore disabled by default and costs a single branch per
call while it stays that way. The runner turns it on only for the replay pass
that produces the trace attached to a
:class:`~askew.errors.SimulationFailure`.

Records are stored unformatted, as a template and its arguments. Almost none of
them will ever be printed -- only the tail is, and only once -- so the string is
built when it is read rather than when it is written.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .loop import SimLoop

class EventLog:
    """
    A bounded, virtually timestamped record of what happened during a run.

    Older entries fall off the front once *limit* is reached. That bound is not
    a compromise: the tail is what explains a failure, and the ten thousand
    heartbeats before it are noise that would only push the interesting lines
    off the screen.

    :ivar enabled: whether :meth:`add` records anything
    :ivar limit: how many records are kept before the oldest are discarded
    """

    __slots__ = (
        "_loop",
        "_records",
        "enabled",
        "limit"
    )

    def __init__(self, loop: SimLoop, enabled: bool = False, limit: int = 256) -> None:
        self._loop = loop
        self._records: deque[tuple[float, str, str, tuple[Any, ...]]] = deque(maxlen=limit)
        self.enabled = enabled
        self.limit = limit

    def add(self, kind: str, template: str, *args: Any) -> None:
        """
        Record an event, stamped with the current virtual time.

        *template* is a percent format string and *args* are its arguments; they
        are stored as they arrive and only combined if the record is ever read.
        Pass values, not a finished string, or the saving is lost::

            log.add("net", "%d -> %d dropped: partitioned", source, target)
        """
        if self.enabled:
            self._records.append((self._loop.now, kind, template, args))

    def tail(self, count: int = 20) -> tuple[str, ...]:
        """
        Return the last *count* records, formatted, oldest first.
        """
        records = self._records
        start = len(records) - count
        if start < 0:
            start = 0
        return tuple(
            "%12.6f  %-6s  %s" % (when, kind, template % args if args else template)
            for when, kind, template, args in tuple(records)[start:]
        )

    def format(self, count: int = 20) -> str:
        """
        Return the last *count* records as one indented block, ready to print.
        """
        return "\n".join("  " + line for line in self.tail(count))

    def clear(self) -> None:
        """
        Discard every record kept so far.
        """
        self._records.clear()

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self) -> Any:
        return iter(self._records)

    def __repr__(self) -> str:
        return "EventLog(enabled=%r, records=%d, limit=%d)" % (self.enabled, len(self._records), self.limit)