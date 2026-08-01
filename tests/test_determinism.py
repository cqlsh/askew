"""
The property everything else rests on: a seed describes a run completely.

If any of these fail, askew has stopped being what it claims to be. A report
that says seed 8149203 is no longer a bug report, it is a rumour.

:class:`TestTracingIsFree` is the one to look at first. The runner's whole
failure path assumes that rerunning a seed with the event log switched on
produces the same run, because logging draws nothing from the generator. If that
stops holding, every trace askew prints describes a run that never happened.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from askew.core.rng import Rng, SeedMixer
from askew.core.scheduler import FifoScheduler, RandomScheduler
from askew.errors import SimulationFailure
from askew.testing.config import SimulationConfig
from askew.testing.replay import Replay
from askew.testing.report import Report
from askew.testing.runner import Simulation, simulate

class Scenario:
    """
    One scenario exercising spawn, broadcast, partition and a crash.

    Deliberately busy. A determinism test that only sleeps proves nothing, since
    the parts most likely to leak nondeterminism are the ones that choose:
    latencies, drops, orderings.
    """

    @staticmethod
    async def gossip(node: Any, index: int) -> None:
        node.seen = 0
        node.broadcast(("hello", index))
        while True:
            message = await node.recv(timeout=1.0)
            if message is None:
                node.broadcast(("ping", node.id))
            else:
                node.seen += 1

    @staticmethod
    def collecting(sink: list[Any]) -> Any:
        async def run(world: Any) -> None:
            nodes = [world.spawn(Scenario.gossip, index) for index in range(4)]
            await world.clock.advance(seconds=5)
            async with world.partition({0, 1}, {2, 3}):
                await world.clock.advance(seconds=5)
            world.crash(nodes[0])
            await world.clock.advance(seconds=5)
            sink.append(tuple((node.id, node.seen, node.alive) for node in world.nodes))
        return run

class Runs:
    """
    Runs the scenario and reduces the outcome to something comparable.
    """

    @staticmethod
    def report(seed: int, trace: bool = True, **options: Any) -> tuple[Report, list[Any]]:
        sink: list[Any] = []
        config = SimulationConfig(trace=trace, **options)
        return Simulation(config, seed).run(Scenario.collecting(sink)), sink

    @staticmethod
    def fingerprint(report: Report) -> tuple[Any, ...]:
        """
        Everything about a run that is not the trace itself.
        """
        return (
            report.virtual_time,
            report.steps,
            report.nodes,
            report.crashed,
            report.sent,
            report.delivered,
            report.dropped
        )

class TestGenerator:
    """
    The generator is where every decision starts.
    """

    def test_same_seed_same_numbers(self) -> None:
        first = Rng(1337)
        second = Rng(1337)
        assert [first.random() for _ in range(100)] == [second.random() for _ in range(100)]

    def test_different_seeds_differ(self) -> None:
        assert [Rng(1).random() for _ in range(20)] != [Rng(2).random() for _ in range(20)]

    def test_shuffle_is_reproducible(self) -> None:
        first = list(range(50))
        second = list(range(50))
        Rng(99).shuffle(first)
        Rng(99).shuffle(second)
        assert first == second

    def test_forks_are_independent_of_each_other(self) -> None:
        parent = Rng(7)
        left = parent.fork()
        right = parent.fork()
        assert left.seed != right.seed
        assert [left.random() for _ in range(10)] != [right.random() for _ in range(10)]

    def test_forking_is_itself_reproducible(self) -> None:
        assert Rng(7).fork().seed == Rng(7).fork().seed

    def test_draining_a_fork_does_not_move_the_parent(self) -> None:
        parent = Rng(11)
        child = parent.fork()
        for _ in range(1000):
            child.random()
        untouched = Rng(11)
        untouched.fork()
        assert parent.random() == untouched.random()

class TestSeedDerivation:
    """
    Iteration seeds are computed, not counted.
    """

    def test_no_collisions_over_a_long_run(self) -> None:
        assert len({SeedMixer.mix(1337, index) for index in range(50_000)}) == 50_000

    def test_derivation_is_stable(self) -> None:
        assert SeedMixer.mix(1337, 8417) == SeedMixer.mix(1337, 8417)

    def test_late_iterations_need_no_earlier_ones(self) -> None:
        config = SimulationConfig(seed=1337)
        direct = config.seed_for(8417)
        walked = [config.seed_for(index) for index in range(8418)][-1]
        assert direct == walked

    def test_neighbouring_iterations_are_unrelated(self) -> None:
        first = SeedMixer.mix(1337, 0)
        second = SeedMixer.mix(1337, 1)
        assert abs(first - second) > 2 ** 32

class TestRunsRepeat:
    """
    The same seed produces the same run, down to the trace.
    """

    def test_traces_are_identical(self) -> None:
        first, _ = Runs.report(4242)
        second, _ = Runs.report(4242)
        assert first.events == second.events

    def test_counters_are_identical(self) -> None:
        first, _ = Runs.report(4242)
        second, _ = Runs.report(4242)
        assert Runs.fingerprint(first) == Runs.fingerprint(second)

    def test_node_state_is_identical(self) -> None:
        _, first = Runs.report(4242)
        _, second = Runs.report(4242)
        assert first == second

    def test_different_seeds_produce_different_runs(self) -> None:
        first, _ = Runs.report(4242)
        second, _ = Runs.report(9999)
        assert first.events != second.events

    def test_the_scheduler_changes_the_outcome(self) -> None:
        random, _ = Runs.report(4242, scheduler=RandomScheduler())
        fifo, _ = Runs.report(4242, scheduler=FifoScheduler())
        assert random.events != fifo.events

class TestTracingIsFree:
    """
    Switching the event log on must not change what happens.

    The runner reruns a failing seed with tracing enabled and presents the
    result as an explanation of the failure that already occurred. That is only
    honest if these pass.
    """

    def test_counters_match_with_and_without_tracing(self) -> None:
        traced, _ = Runs.report(4242, trace=True)
        silent, _ = Runs.report(4242, trace=False)
        assert Runs.fingerprint(traced) == Runs.fingerprint(silent)

    def test_node_state_matches_with_and_without_tracing(self) -> None:
        _, traced = Runs.report(4242, trace=True)
        _, silent = Runs.report(4242, trace=False)
        assert traced == silent

    def test_the_silent_run_records_nothing(self) -> None:
        silent, _ = Runs.report(4242, trace=False)
        assert silent.events == ()

class TestFailuresReproduce:
    """
    A reported seed brings the failure back.
    """

    @staticmethod
    def failing_test() -> Any:
        @simulate(seed=20260801, iterations=500)
        async def test_counter_never_exceeds_two(world: Any) -> None:
            active = 0
            peak = 0

            async def worker() -> None:
                nonlocal active, peak
                while active >= 2:
                    await asyncio.sleep(0.001)
                await asyncio.sleep(world.random.uniform(0.001, 0.01))
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.01)
                active -= 1

            async with asyncio.TaskGroup() as group:
                for _ in range(6):
                    group.create_task(worker())
            assert peak <= 2, "limit exceeded: %d" % peak

        return test_counter_never_exceeds_two

    def test_the_run_finds_the_bug(self) -> None:
        with pytest.raises(SimulationFailure) as caught:
            TestFailuresReproduce.failing_test()()
        assert caught.value.seed is not None

    def test_the_reported_seed_fails_again(self) -> None:
        test = TestFailuresReproduce.failing_test()
        with pytest.raises(SimulationFailure) as caught:
            test()
        with pytest.raises(SimulationFailure):
            Replay.check(test, caught.value.seed)

    def test_the_reported_seed_fails_the_same_way(self) -> None:
        test = TestFailuresReproduce.failing_test()
        with pytest.raises(SimulationFailure) as caught:
            test()
        first = Replay.run(test, caught.value.seed)
        second = Replay.run(test, caught.value.seed)
        assert str(first.error) == str(second.error)
        assert first.steps == second.steps