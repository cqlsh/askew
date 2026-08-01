"""
Tests for the deterministic loop itself, without a simulation around it.

Two of these classes are unusual and deserve a word.
:class:`TestStandardLibraryContract` asserts that the private attributes of
:class:`~asyncio.BaseEventLoop` that :class:`~askew.core.loop.SimLoop` builds on
still exist. They are not a promised interface, so a future Python could rename
one; when that happens this fails with a sentence explaining what broke instead
of an AttributeError somewhere inside a tick.

:class:`TestAsyncioCompatibility` runs stock asyncio machinery that knows nothing
about askew. If a TaskGroup or a timeout stops working here, the claim that
existing code runs unchanged has stopped being true.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from askew.core.loop import SimLoop
from askew.core.rng import Rng
from askew.core.scheduler import FifoScheduler, RandomScheduler, ReverseScheduler, Scheduler
from askew.errors import (
    DeadlockError,
    NondeterminismError,
    StepLimitExceeded,
    TimeLimitExceeded
)

class Build:
    """
    Constructs loops for the tests, so the argument order lives in one place.
    """

    @staticmethod
    def loop(seed: int = 1337, scheduler: Scheduler | None = None, **options: Any) -> SimLoop:
        return SimLoop(Rng(seed), scheduler if scheduler is not None else RandomScheduler(), **options)

    @staticmethod
    def run(coroutine: Any, seed: int = 1337, scheduler: Scheduler | None = None, **options: Any) -> Any:
        loop = Build.loop(seed, scheduler, **options)
        try:
            return loop.run_until_complete(coroutine)
        finally:
            loop.close()

class TestVirtualTime:
    """
    Time jumps to the next deadline and never anywhere else.
    """

    def test_sleeping_costs_no_real_time(self) -> None:
        async def scenario() -> float:
            loop = asyncio.get_running_loop()
            await asyncio.sleep(86400)
            return loop.time()

        started = time.perf_counter()
        assert Build.run(scenario()) == 86400.0
        assert time.perf_counter() - started < 0.1

    def test_sleeps_accumulate(self) -> None:
        async def scenario() -> float:
            for _ in range(10):
                await asyncio.sleep(1.5)
            return asyncio.get_running_loop().time()

        assert Build.run(scenario()) == 15.0

    def test_starts_where_it_is_told(self) -> None:
        async def scenario() -> float:
            return asyncio.get_running_loop().time()

        assert Build.run(scenario(), start_time=1000.0) == 1000.0

    def test_time_never_moves_backwards(self) -> None:
        readings: list[float] = []

        async def watcher() -> None:
            loop = asyncio.get_running_loop()
            for _ in range(50):
                readings.append(loop.time())
                await asyncio.sleep(0.01)

        async def scenario() -> None:
            async with asyncio.TaskGroup() as group:
                for _ in range(5):
                    group.create_task(watcher())

        Build.run(scenario())
        assert readings == sorted(readings)

    def test_concurrent_sleepers_wake_at_their_own_deadlines(self) -> None:
        woke: list[tuple[str, float]] = []

        async def sleeper(name: str, seconds: float) -> None:
            await asyncio.sleep(seconds)
            woke.append((name, asyncio.get_running_loop().time()))

        async def scenario() -> None:
            async with asyncio.TaskGroup() as group:
                group.create_task(sleeper("late", 30.0))
                group.create_task(sleeper("early", 1.0))
                group.create_task(sleeper("middle", 5.0))

        Build.run(scenario())
        assert woke == [("early", 1.0), ("middle", 5.0), ("late", 30.0)]

class TestDeadlock:
    """
    Nothing ready and nothing scheduled, with work outstanding, is an error.
    """

    def test_waiting_on_a_future_nobody_resolves(self) -> None:
        async def scenario() -> None:
            await asyncio.get_running_loop().create_future()

        with pytest.raises(DeadlockError) as caught:
            Build.run(scenario())
        assert caught.value.pending

    def test_waiting_on_an_event_nobody_sets(self) -> None:
        async def scenario() -> None:
            await asyncio.Event().wait()

        with pytest.raises(DeadlockError):
            Build.run(scenario())

    def test_a_pending_timer_is_not_a_deadlock(self) -> None:
        async def scenario() -> None:
            await asyncio.sleep(5)

        Build.run(scenario())

    def test_detection_can_be_switched_off(self) -> None:
        async def scenario() -> None:
            await asyncio.get_running_loop().create_future()

        loop = Build.loop(detect_deadlock=False)
        try:
            loop.run_until_complete(scenario())
        except RuntimeError:
            pass
        finally:
            loop.close()

class TestLimits:
    """
    A run that will not end is stopped and told why.
    """

    def test_step_limit_catches_a_busy_loop(self) -> None:
        async def scenario() -> None:
            while True:
                await asyncio.sleep(0)

        with pytest.raises(StepLimitExceeded):
            Build.run(scenario(), max_steps=5000)

    def test_time_limit_catches_an_endless_wait(self) -> None:
        async def scenario() -> None:
            await asyncio.sleep(1e9)

        with pytest.raises(TimeLimitExceeded):
            Build.run(scenario(), max_time=100.0)

    def test_limits_are_off_by_default(self) -> None:
        async def scenario() -> float:
            await asyncio.sleep(1e6)
            return asyncio.get_running_loop().time()

        assert Build.run(scenario()) == 1e6

class TestRefusedApis:
    """
    Anything that cannot be replayed from a seed is refused up front.
    """

    def test_to_thread(self) -> None:
        async def scenario() -> None:
            await asyncio.to_thread(len, "x")

        with pytest.raises(NondeterminismError):
            Build.run(scenario())

    def test_run_in_executor(self) -> None:
        async def scenario() -> None:
            await asyncio.get_running_loop().run_in_executor(None, len, "x")

        with pytest.raises(NondeterminismError):
            Build.run(scenario())

    def test_name_resolution(self) -> None:
        async def scenario() -> None:
            await asyncio.get_running_loop().getaddrinfo("example.com", 80)

        with pytest.raises(NondeterminismError):
            Build.run(scenario())

    def test_opening_a_socket(self) -> None:
        async def scenario() -> None:
            await asyncio.get_running_loop().create_connection(asyncio.Protocol, "example.com", 80)

        with pytest.raises(NondeterminismError):
            Build.run(scenario())

    def test_watching_a_descriptor(self) -> None:
        loop = Build.loop()
        try:
            with pytest.raises(NondeterminismError):
                loop.add_reader(0, print)
        finally:
            loop.close()

    def test_removing_a_watch_stays_quiet(self) -> None:
        loop = Build.loop()
        try:
            assert loop.remove_reader(0) is False
            assert loop.remove_writer(0) is False
        finally:
            loop.close()

class TestSchedulers:
    """
    The scheduler decides the order, and says so.
    """

    @staticmethod
    def interleaving(seed: int, scheduler: Scheduler) -> list[int]:
        order: list[int] = []

        async def worker(index: int) -> None:
            for _ in range(4):
                await asyncio.sleep(0)
                order.append(index)

        async def scenario() -> None:
            async with asyncio.TaskGroup() as group:
                for index in range(5):
                    group.create_task(worker(index))

        Build.run(scenario(), seed, scheduler)
        return order

    def test_fifo_ignores_the_seed(self) -> None:
        assert TestSchedulers.interleaving(1, FifoScheduler()) == TestSchedulers.interleaving(2, FifoScheduler())

    def test_random_follows_the_seed(self) -> None:
        assert TestSchedulers.interleaving(1, RandomScheduler()) == TestSchedulers.interleaving(1, RandomScheduler())
        assert TestSchedulers.interleaving(1, RandomScheduler()) != TestSchedulers.interleaving(2, RandomScheduler())

    def test_reverse_is_not_fifo(self) -> None:
        assert TestSchedulers.interleaving(1, ReverseScheduler()) != TestSchedulers.interleaving(1, FifoScheduler())

class TestTimerHousekeeping:
    """
    Cancelled timers must not accumulate.
    """

    def test_heap_does_not_grow_without_bound(self) -> None:
        async def scenario() -> None:
            for _ in range(500):
                async with asyncio.timeout(10):
                    await asyncio.sleep(0.001)

        loop = Build.loop()
        try:
            loop.run_until_complete(scenario())
            assert len(loop._scheduled) < 50
        finally:
            loop.close()

class TestAsyncioCompatibility:
    """
    Stock asyncio machinery, none of which knows askew exists.
    """

    def test_task_group(self) -> None:
        async def scenario() -> int:
            total = 0
            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(asyncio.sleep(i, result=i)) for i in range(5)]
            for task in tasks:
                total += task.result()
            return total

        assert Build.run(scenario()) == 10

    def test_timeout_expires(self) -> None:
        async def scenario() -> bool:
            try:
                async with asyncio.timeout(1.0):
                    await asyncio.sleep(10.0)
            except TimeoutError:
                return True
            return False

        assert Build.run(scenario()) is True

    def test_gather(self) -> None:
        async def scenario() -> list[int]:
            return await asyncio.gather(*[asyncio.sleep(i, result=i) for i in range(4)])

        assert Build.run(scenario()) == [0, 1, 2, 3]

    def test_lock_serialises(self) -> None:
        async def scenario() -> int:
            lock = asyncio.Lock()
            peak = 0
            active = 0

            async def worker() -> None:
                nonlocal peak, active
                async with lock:
                    active += 1
                    peak = max(peak, active)
                    await asyncio.sleep(0.1)
                    active -= 1

            async with asyncio.TaskGroup() as group:
                for _ in range(10):
                    group.create_task(worker())
            return peak

        assert Build.run(scenario()) == 1

    def test_queue_moves_items(self) -> None:
        async def scenario() -> list[int]:
            queue: asyncio.Queue[int] = asyncio.Queue()
            received: list[int] = []

            async def producer() -> None:
                for value in range(5):
                    await queue.put(value)
                    await asyncio.sleep(0.01)

            async def consumer() -> None:
                for _ in range(5):
                    received.append(await queue.get())

            async with asyncio.TaskGroup() as group:
                group.create_task(producer())
                group.create_task(consumer())
            return received

        assert Build.run(scenario()) == [0, 1, 2, 3, 4]

    def test_cancellation_runs_cleanup(self) -> None:
        async def scenario() -> bool:
            cleaned = False

            async def worker() -> None:
                nonlocal cleaned
                try:
                    await asyncio.sleep(100)
                finally:
                    cleaned = True

            task = asyncio.get_running_loop().create_task(worker())
            await asyncio.sleep(1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return cleaned

        assert Build.run(scenario()) is True

class TestStandardLibraryContract:
    """
    The private parts of BaseEventLoop that SimLoop is built on.

    These are not a promised interface. If a future Python renames one, this is
    where it surfaces, with a name attached.
    """

    LOOP_ATTRIBUTES = (
        "_ready",
        "_scheduled",
        "_stopping",
        "_timer_cancelled_count",
        "_clock_resolution"
    )

    HANDLE_ATTRIBUTES = (
        "_when",
        "_cancelled",
        "_scheduled",
        "_run"
    )

    def test_loop_attributes_still_exist(self) -> None:
        loop = Build.loop()
        try:
            for name in TestStandardLibraryContract.LOOP_ATTRIBUTES:
                assert hasattr(loop, name), (
                        "asyncio.BaseEventLoop no longer has %s; SimLoop._run_once reads it" % name
                )
        finally:
            loop.close()

    def test_timer_handle_attributes_still_exist(self) -> None:
        loop = Build.loop()
        try:
            handle = loop.call_later(1.0, print)
            for name in TestStandardLibraryContract.HANDLE_ATTRIBUTES:
                assert hasattr(handle, name), (
                        "asyncio.TimerHandle no longer has %s; SimLoop._run_once reads it" % name
                )
            handle.cancel()
        finally:
            loop.close()