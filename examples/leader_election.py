"""
The example from the README, runnable, in both its broken and its correct form.

Five replicas elect a leader. Each waits a randomised timeout for news, and on
hearing nothing concludes that the leader is gone and stands for the job. The
question the example answers is what "stands for the job" should mean.

:class:`Replica` implements both answers. The naive one simply declares itself
and announces the fact. The other collects grants and only takes office once it
holds more than half of them. Under a healthy network the two behave the same,
which is exactly why a single run tells you nothing.

Then the network splits, three nodes on one side and two on the other, and the
difference becomes the whole point. The naive election produces a leader on each
side, because neither side can tell a partition from four dead peers. The quorum
election produces one, because two nodes cannot reach a majority of five, and
two majorities of five cannot both exist.

Both tests run three hundred iterations. The naive one fails on the first, since
the split is guaranteed by the scenario. The quorum one survives all of them,
under three hundred different timeout draws, message orderings and latencies.

Run it directly to see both::

    python examples/leader_election.py
"""

from __future__ import annotations

from typing import Any

import askew

class Replica:
    """
    A node that elects, or stands for election.

    The state a test reads afterwards lives on the node handle: ``node.leader``
    is who this replica currently believes leads, and ``node.term`` is the
    election it belongs to. Terms only ever rise, and a replica adopts any
    leader announced for a term at least as recent as its own.
    """

    NODES = 5
    """ Replicas in the cluster. Five splits three against two. """

    @staticmethod
    async def run(node: Any, total: int, quorum: bool, timeout: float) -> None:
        """
        Run one replica until it is cancelled.

        With *quorum* false this is the broken election: on a timeout the
        replica declares itself and announces it, which is a decision no other
        node had a say in.

        With *quorum* true it broadcasts that it is standing, collects grants,
        and takes office only once it holds more than half of them. That is the
        entire fix, and it is four lines.

        The *timeout* is drawn per replica from the run's generator. Without
        that spread every replica stands at the same instant, every election
        ties, and the cluster never settles.
        """
        node.leader = None
        node.term = 0
        votes: set[int] = set()

        while True:
            message = await node.recv(timeout=timeout)

            if message is None:
                node.term += 1
                votes = {node.id}
                if not quorum:
                    node.leader = node.id
                    node.broadcast(("leader", node.term, node.id))
                else:
                    node.broadcast(("standing", node.term, node.id))
                continue

            kind, term, sender = message.payload

            if kind == "standing" and term > node.term:
                node.term = term
                node.send(sender, ("granted", term, node.id))
            elif kind == "granted" and term == node.term:
                votes.add(sender)
                if len(votes) * 2 > total:
                    node.leader = node.id
                    node.broadcast(("leader", node.term, node.id))
            elif kind == "leader" and term >= node.term:
                node.term = term
                node.leader = sender

class Scenario:
    """
    The body both tests run: spawn a cluster, split it, and look.
    """

    @staticmethod
    async def run(world: Any, quorum: bool) -> None:
        """
        Spawn five replicas, partition three against two, let thirty seconds pass.

        The assertion is the safety property an election owes you: at no point
        may two nodes each believe they are in charge. It is checked while the
        partition is still healing rather than long afterwards, because a
        cluster that agrees eventually has still been wrong in the meantime, and
        it is the meantime that corrupts data.

        Thirty simulated seconds cost nothing here. Every timer inside them
        fires, and the loop spends no real time between them.
        """
        rng = world.random
        nodes = [
            world.spawn(Replica.run, Replica.NODES, quorum, rng.uniform(1.0, 3.0))
            for _ in range(Replica.NODES)
        ]

        async with world.partition({0, 1}, {2, 3, 4}):
            await world.clock.advance(seconds=30)

        claimants = sorted(node.id for node in nodes if node.leader == node.id)
        assert len(claimants) <= 1, "split brain, nodes %r each believe they lead" % claimants

@askew.simulate(seed=1337, iterations=300)
async def test_naive_election_splits(world):
    """
    Fails, and hands back the seed. Both sides of the partition elect.
    """
    await Scenario.run(world, quorum=False)

@askew.simulate(seed=1337, iterations=300)
async def test_quorum_election_holds(world):
    """
    Passes three hundred times. Two nodes cannot reach a majority of five.
    """
    await Scenario.run(world, quorum=True)

class Demonstration:
    """
    Runs both tests and prints what happened, for use from the command line.
    """

    @staticmethod
    def main() -> None:
        for label, test in (("naive ", test_naive_election_splits),
                            ("quorum", test_quorum_election_holds)):
            try:
                test()
                print("%s  300 iterations passed" % label)
            except askew.SimulationFailure as failure:
                print("%s  failed at iteration %d" % (label, failure.iteration))
                print("          %s" % failure.summary.splitlines()[0])
                print("          reproduce with ASKEW_SEED=%d" % failure.seed)

if __name__ == "__main__":
    Demonstration.main()