<p align="center">
  <a href="https://pypi.org/project/askew-dst/">
    <img alt="PyPI" src="https://img.shields.io/pypi/v/askew-dst?style=for-the-badge&labelColor=161b22&color=21262d&logo=pypi&logoColor=e6edf3">
  </a>
  <a href="https://pypi.org/project/askew-dst/">
    <img alt="Python" src="https://img.shields.io/pypi/pyversions/askew-dst?style=for-the-badge&labelColor=161b22&color=21262d&logo=python&logoColor=e6edf3">
  </a>
  <a href="https://github.com/cqlsh/askew/actions/workflows/ci.yml">
    <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/cqlsh/askew/ci.yml?branch=main&style=for-the-badge&labelColor=161b22&color=21262d&logo=githubactions&logoColor=e6edf3&label=ci">
  </a>
  <a href="LICENSE">
    <img alt="License" src="https://img.shields.io/badge/apache_2.0-21262d?style=for-the-badge&labelColor=161b22&label=license">
  </a>
</p>

---

askew swaps out the asyncio event loop for one where **time is virtual** and **callback order is chosen by a seeded PRNG**. Your code runs unchanged â€” same `async def`, same `await`, same `TaskGroup`. What changes is that the scheduler stops being an accident of your machine and becomes an input you control.

One integer describes an entire run. Change it and you get a different interleaving. Keep it and you get the same bug, on every machine, forever.

## Why

Concurrency bugs are almost never bad logic. They are a rare ordering that your laptop happens not to produce, your CI happens to produce once a month, and production produces on a Friday.

- **Reproducible.** A failure is a seed. `seed=8149203` is the whole bug report.
- **Fast.** Time is simulated. `await asyncio.sleep(30)` returns immediately, so a scenario spanning ten simulated minutes runs in microseconds. Thousands of interleavings per second.
- **Hostile.** Partitions, dropped and reordered messages, latency, clock skew and node crashes are first-class â€” and driven by the same seed as the scheduler.

## Install

```bash
pip install askew-dst
```

Requires Python 3.11+. No dependencies.

## Quickstart

```python
import askew

async def node_main(node, index):
    node.leader = None
    while True:
        msg = await node.recv(timeout=5)
        if msg is None:
            node.leader = node.id            # timed out, claim leadership
            await node.broadcast(("claim", node.id))
        elif msg.payload[0] == "claim":
            node.leader = max(node.leader or -1, msg.payload[1])

@askew.simulate(seed=1337, iterations=10_000)
async def test_leader_election(world):
    nodes = [world.spawn(node_main, i) for i in range(5)]

    async with world.partition({0, 1}, {2, 3, 4}):
        await world.clock.advance(seconds=30)

    await world.clock.advance(seconds=30)
    assert len({n.leader for n in nodes}) == 1
```

Ten thousand runs. Ten thousand different schedules, latencies and message orders. Each one finishes in well under a millisecond of real time, because none of the thirty simulated seconds are actually spent.

Run it with plain `pytest` â€” the decorator returns an ordinary sync test function, so no asyncio plugin is involved.

## What happens under the hood

| | real asyncio | askew |
|---|---|---|
| clock | `time.monotonic()` | virtual, jumps straight to the next timer |
| `await asyncio.sleep(30)` | 30 seconds | 0 seconds |
| ready queue | FIFO | permuted every tick by the seeded PRNG |
| network | real sockets | in-memory links with latency, loss and reordering |
| a failing run | "flaky, hit retry" | a seed |

Because virtual time only advances once the ready queue has drained, askew also sees things a real loop cannot: a run with no ready callback and no pending timer is a **deadlock**, and it is reported as one instead of hanging your test suite.

## Reproducing a failure

When an iteration fails, askew tells you exactly which one:

```
askew.errors.SimulationFailure: assert len({n.leader for n in nodes}) == 1
  (seed=8149203, iteration=417, t=61.482913s, steps=20194)

  recent events
    60.001s  net     0 -> 2  ("claim", 0)   dropped: partitioned
    60.004s  net     3 -> 4  ("claim", 3)   delivered
    60.982s  node    2 timed out, claiming leadership
    61.482s  assert  2 leaders: {0, 3}
```

Pin the seed and you get that run back, byte for byte:

```python
askew.Replay.run(test_leader_election, seed=8149203)
```

Or from the shell, without touching the source:

```bash
ASKEW_SEED=8149203 ASKEW_ITERATIONS=1 pytest -k leader_election
```

## The world

Everything a test can do to the simulation hangs off the `world` handle it receives.

| | |
|---|---|
| `world.spawn(fn, *args)` | start a node; returns a handle whose attributes you can read from the test |
| `world.clock.advance(seconds=30)` | let simulated time pass, running everything scheduled inside it |
| `world.now` | current virtual time |
| `world.partition({0, 1}, {2, 3, 4})` | async context manager; heals on exit |
| `world.net.isolate(node)` | cut a single node off |
| `world.faults.crash(node)` | kill a node's task |
| `world.random` | the run's PRNG â€” use it so your test data is part of the seed too |
| `world.log` | the event trace shown above |

A spawned target is called as `fn(node, *args)`. The node handle doubles as its state: assign `node.leader` inside the coroutine, read `n.leader` from the test. Classes work too â€” `world.spawn(Replica, 3)` instantiates it and proxies attribute access through.

## Rules of determinism

askew can only replay what it controls. It raises `NondeterminismError` up front rather than letting a test rot into a flaky one, so these are hard errors inside a simulation:

- `run_in_executor`, `asyncio.to_thread`, or any other thread
- real sockets, `create_connection`, `add_reader`
- `time.time()` / `time.monotonic()` for logic (use `world.now`)

And two things askew cannot catch for you: the global `random` module (use `world.random`), and iteration order over sets of unhashable-but-unordered things. Both quietly differ between runs.

## Status

Early. The core â€” loop, clock, scheduler, network, faults, runner â€” is in place and the API above is what ships, but it is not frozen yet. Pin an exact version if you depend on it, and expect the occasional rename before 1.0.

Issues and PRs welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Prior art

askew stands on ideas from [FoundationDB's simulation testing](https://apple.github.io/foundationdb/testing.html), [TigerBeetle's VOPR](https://github.com/tigerbeetle/tigerbeetle), and [madsim](https://github.com/madsim-rs/madsim) in the Rust world. This is that idea, made native to asyncio.

## License

[Apache 2.0](LICENSE)
