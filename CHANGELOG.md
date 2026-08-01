# Changelog

Notable changes to askew, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Until 1.0 the public API may change between minor versions. Pin an exact version
if you depend on it.

## [Unreleased]

### Added

**The deterministic loop.** `SimLoop` replaces asyncio's clock with a virtual
one that jumps to the next pending deadline instead of waiting, and replaces the
order of a tick's ready callbacks with one drawn from a seeded generator.
Everything else is stock asyncio, so existing coroutines run unchanged.

**Deadlock detection.** A loop with nothing ready and nothing scheduled, while
work is outstanding, is reported rather than left to hang. Under virtual time
there is no I/O that might still arrive, so the state is provably dead.

**Refusal of what cannot be replayed.** Thread pools, name resolution, real
sockets and readers on real file descriptors raise `NondeterminismError` with a
note on what to use instead, rather than quietly making a test flaky.

**Scheduling strategies.** `RandomScheduler` permutes each tick uniformly and is
the default. `FifoScheduler` matches real asyncio and exists as a control for
telling a genuine defect from a test relying on an order nothing guaranteed.
`ReverseScheduler` and `PartialScheduler` sit at the two extremes between them.

**A simulated world.** Nodes with mailboxes, a network with per link latency,
loss, duplication and reordering, partitions that heal on leaving their context,
and a fault injector that crashes nodes on request or at random.

**The `simulate` decorator.** Turns a scenario into an ordinary synchronous test
function, collected by plain pytest with no asyncio plugin. Runs it under many
seeds, stops at the first failure, and reports the seed that reproduces it.

**Traces without a cost.** Iterations run with the event log off. When one
fails, its seed runs a second time with the log on; logging draws nothing from
the generator, so that is the same run, and the trace explains the failure that
already happened.

**Replay.** `Replay.check` pins a seed that once failed as a permanent
regression test, costing one iteration and always exercising the interleaving
that broke. `ASKEW_SEED` and `ASKEW_ITERATIONS` do the same from the shell
without editing the test.

### Known limitations

**The global `random` module, `time` and `uuid` are not intercepted.** Code
reaching for them consumes randomness askew does not control, and a seed stops
being sufficient to replay the run. Draw from `world.random` and read
`world.now` instead. Intercepting these is planned.

**Continuous chaos suppresses deadlock detection.** A running `FaultInjector.chaos`
loop always holds a pending timer, so the loop is never idle and a genuine
deadlock elsewhere goes unnoticed. Reproduce a failing seed without chaos.

**`RandomScheduler` violates a documented guarantee.** asyncio specifies
`call_soon` as first in, first out, and permuting the queue breaks that on
purpose. Most of what it finds is a real ordering assumption, but not all;
rerun a failing seed under `FifoScheduler` to tell the two apart.

**No shrinking.** A failing seed is reported as it is, without any attempt to
find a smaller scenario that still fails.

[Unreleased]: https://github.com/cqlsh/askew/commits/main