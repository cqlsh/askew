# Contributing to askew

Thanks for taking the time. askew is small and early, which means changes land fast and the API is still soft — a good moment to shape it.

## The golden rule

**A bug report without a seed is a guess.** askew exists so that every failure collapses to one integer. If you found something, we want the seed:

```
askew.errors.SimulationFailure: ...
  (seed=8149203, iteration=417, t=61.482913s, steps=20194)
```

Open the issue with that seed, the askew version, your Python version, and the smallest test function that reproduces it. If a seed reproduces on your machine but not on ours, that is itself the bug — determinism is the product, so those reports get priority over everything else.

## Setup

```bash
git clone https://github.com/cqlsh/askew
cd askew
python -m venv .venv
.venv/Scripts/activate      # Linux and macOS: source .venv/bin/activate
pip install -e ".[dev]"
```

Run the suite:

```bash
pytest
```

askew's own tests are mostly meta-tests: they assert that a given seed produces byte-identical event logs across runs, that virtual time never moves backwards, and that a deliberately broken protocol is actually caught within N iterations. If you touch the loop, the scheduler or the RNG, expect those to be the ones that break.

## What askew will and will not take

**Yes:**

- new fault kinds (clock skew, partial writes, slow nodes, byzantine message corruption)
- new scheduling strategies beyond the current random permutation
- anything that makes a failure easier to read — shrinking, better traces, nicer diffs
- documentation and examples, especially real protocols

**No:**

- runtime dependencies. askew ships with zero and stays that way. Test and dev dependencies are fine.
- anything that reaches for a thread, a real socket or the wall clock inside a simulation. That is what `NondeterminismError` is for.
- support for Python versions below 3.11.

If you are unsure whether something fits, open an issue before writing the code. Nobody enjoys rejecting a finished PR.

## Determinism is a hard invariant

Every source of variation in a run must trace back to the seed. When you add code that makes a choice — a delay, an ordering, a probability, a pick from a set — draw it from `world.rng`, never from the global `random` module and never from anything derived from real time or object identity.

Two traps that look innocent and are not:

- **`id()` and default `hash()`.** Iterating a `set` of objects without `__hash__` gives you an order that changes between processes. Sort by node id instead.
- **`dict` ordering across a rebuild.** Insertion order is stable, but only if the insertions themselves were deterministic. If the insertion order came from a set, you have moved the problem, not solved it.

A PR that adds behaviour should add a test that runs it twice under the same seed and asserts the event logs match.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/), so a changelog can be generated from the history:

```
feat(net): add message duplication under partition heal
fix(loop): stop advancing the clock past a cancelled timer
docs: document the replay workflow
test(rng): assert seed forks stay independent
chore: bump ruff
```

Write a body that explains what changed and why. The subject line says which lever moved; the body says what was broken about the old behaviour, or what the new behaviour buys. Assume the reader is you in eight months with no memory of this week.

## Pull requests

1. Branch off `main`.
2. Keep one logical change per PR. Two unrelated fixes are two PRs.
3. Tests pass, and new behaviour comes with a test.
4. Update the README if you changed something documented there.
5. Describe what you changed and why in the PR body — a link to an issue is not a description.

Reviews are direct and about the code. Nobody here is fighting you.

## License

By contributing, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE), the same terms that cover the rest of the project. There is no CLA.