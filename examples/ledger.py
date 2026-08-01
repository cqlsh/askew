"""
A simulation test with no nodes, no network and no partitions.

The leader election example shows askew modelling a distributed system. This one
shows the other half of what it is for: ordinary concurrent code, where several
tasks touch shared state and the bug is a window between two lines.

:class:`Ledger` has a check-then-act race. It reads a balance, awaits, and only
then subtracts. Between those two statements another transfer can read the same
balance and reach the same conclusion, and both proceed. The account goes
negative while every individual transfer looks correct.

Whether that window opens depends on how long the audit takes and on which task
the loop resumes first, both of which come from the run's generator. A single
run of this scenario passes far more often than it fails, which is exactly why
running it once proves nothing. Five thousand runs take under a second.

Run it directly to see both halves::

    python examples/ledger.py
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable

import askew

class Ledger:
    """
    Accounts and transfers between them, with a race in the transfer.

    :ivar balances: account name to balance
    :ivar completed: transfers that went through
    """

    def __init__(self, balances: dict[str, int], audit_delay: Callable[[], float], guard: asyncio.Lock | None = None) -> None:
        self.balances = dict(balances)
        self.audit_delay = audit_delay
        self.guard = guard
        self.completed = 0

    async def transfer(self, source: str, target: str, amount: int) -> bool:
        """
        Move *amount* from *source* to *target*, refusing to overdraw.

        The refusal is the part that does not work. Pass a *guard* to the ledger
        to make it work, which is the whole difference between the two tests
        below.
        """
        if self.guard is not None:
            async with self.guard:
                return await self.apply(source, target, amount)
        return await self.apply(source, target, amount)

    async def apply(self, source: str, target: str, amount: int) -> bool:
        """
        The critical section, race and all.

        The await between the check and the subtraction is the window. Anything
        that yields would do; an audit call is simply the most believable reason
        for one to be there.
        """
        if self.balances[source] < amount:
            return False
        await asyncio.sleep(self.audit_delay())
        self.balances[source] -= amount
        self.balances[target] += amount
        self.completed += 1
        return True

    @property
    def total(self) -> int:
        """
        Sum of every balance. Transfers move money, so this never changes.
        """
        return sum(self.balances.values())

class Scenario:
    """
    The body both tests run, with and without the ledger's guard.
    """

    NAMES = ["alice", "bob", "carol", "dave"]
    """ Account holders. Four is enough for transfers to collide. """

    OPENING = 100
    """ What every account starts with. """

    TRANSFERS = 8
    """ Concurrent transfers per run. """

    @staticmethod
    async def run(world: Any, locked: bool) -> None:
        """
        Run the scenario once, with or without the ledger's guard.

        Everything variable is drawn from ``world.random``: who pays whom, how
        much, and how long the audit takes. That is what puts the test data
        inside the seed, so a failing seed reproduces the amounts as well as the
        ordering.
        """
        rng = world.random
        names = Scenario.NAMES
        ledger = Ledger(
            dict.fromkeys(names, Scenario.OPENING),
            lambda: rng.uniform(0.001, 0.020),
            asyncio.Lock() if locked else None
        )

        async with asyncio.TaskGroup() as group:
            for _ in range(Scenario.TRANSFERS):
                source = rng.choice(names)
                target = rng.choice([name for name in names if name != source])
                group.create_task(ledger.transfer(source, target, rng.between(10, 45)))

        assert ledger.total == len(names) * Scenario.OPENING, \
            "money appeared or vanished: %r" % ledger.balances
        assert min(ledger.balances.values()) >= 0, \
            "account went negative: %r" % ledger.balances

@askew.simulate(seed=20260801, iterations=5000)
async def test_unguarded_ledger_goes_negative(world):
    """
    Fails, and says on which seed. The ledger has no guard, so two transfers can
    read the same balance before either subtracts from it.
    """
    await Scenario.run(world, locked=False)

@askew.simulate(seed=20260801, iterations=5000)
async def test_guarded_ledger_holds(world):
    """
    Passes five thousand times. The only difference is one asyncio.Lock.
    """
    await Scenario.run(world, locked=True)

class Demonstration:
    """
    Runs both tests and prints what happened, for use from the command line.
    """

    @staticmethod
    def main() -> None:
        for label, test in (("unguarded", test_unguarded_ledger_goes_negative), ("guarded  ", test_guarded_ledger_holds)):
            try:
                test()
                print("%s  5000 iterations passed" % label)
            except askew.SimulationFailure as failure:
                print("%s  failed at iteration %d" % (label, failure.iteration))
                print("             %s" % failure.summary.splitlines()[0])
                print("             reproduce with ASKEW_SEED=%d" % failure.seed)

if __name__ == "__main__":
    Demonstration.main()