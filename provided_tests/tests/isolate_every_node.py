# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from provided_tests.utils.util import Logger
from tests.utils.redirect_client import RedirectAwareClient


def isolate_every_node(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        clients = [RedirectAwareClient(c.base_url, conductor) for c in fx.clients]
        fx.broadcast_view(conductor.get_view())

        # Establish baseline state with everyone connected
        r = clients[0].put("key1", "hello")
        assert r.status_code == 200, f"baseline put: {r.status_code}"

        for i, c in enumerate(clients):
            r = c.get("key1")
            assert r.status_code == 200, f"baseline get from {i}: {r.status_code}"
            assert r.text == "hello", f"baseline node {i}: '{r.text}'"

        # Drop every node onto its own private network so they can't talk to
        # each other. We intentionally do NOT send a new view -- each node
        # still believes the cluster is {0,1,2}.
        log("\n> ISOLATE EVERY NODE")
        conductor.partition([0], "p0")
        conductor.partition([1], "p1")
        conductor.partition([2], "p2")

        # Spec: when nodes are partitioned, AT LEAST ONE node must still reply
        # to GETs. Node 0 (the primary) has the data locally and is reachable
        # from the host. GETs to any node either succeed directly (primary) or
        # via 307 -> primary (backups still serve through the redirect since
        # the host can reach every container regardless of inter-node partition).
        log("\n> GETs UNDER FULL PARTITION")
        for i, c in enumerate(clients):
            r = c.get("key1", timeout=5)
            assert r.status_code == 200, f"node {i} GET under partition: {r.status_code}"
            assert r.text == "hello", f"node {i} returned '{r.text}'"

        # Strong consistency forbids ack-ing a PUT before all replicas store it.
        # With every node isolated, the primary cannot reach its backups to sync,
        # so the PUT MUST hang until the client times out (408).
        log("\n> PUT UNDER FULL PARTITION (must time out)")
        r = clients[0].put("key2", "should-not-ack", timeout=5)
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT should have timed out under full partition, got {r.status_code}"
        )
        log(f"  PUT correctly timed out (status={r.status_code})")

        # Heal: put all three back on the base network and re-broadcast view.
        log("\n> HEAL + REBROADCAST VIEW")
        conductor.partition([0, 1, 2], None)
        fx.broadcast_view(conductor.get_view())

        # The original data must still be readable everywhere
        for i, c in enumerate(clients):
            r = c.get("key1")
            assert r.status_code == 200, f"post-heal get key1 from {i}: {r.status_code}"
            assert r.text == "hello", f"post-heal node {i} key1: '{r.text}'"

        # New PUTs work and replicate everywhere
        r = clients[0].put("key3", "post-heal")
        assert r.status_code == 200, f"post-heal put: {r.status_code}"

        for i, c in enumerate(clients):
            r = c.get("key3")
            assert r.status_code == 200, f"post-heal get key3 from {i}: {r.status_code}"
            assert r.text == "post-heal", f"post-heal node {i} key3: '{r.text}'"

        conductor.dump_all_container_logs(dir)
    return True, "ok"
