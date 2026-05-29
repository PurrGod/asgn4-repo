# designed by iyyam/ajagathe/brcalcan
# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from provided_tests.utils.util import Logger
from ..utils.redirect_client import RedirectAwareClient


def timeout_then_put(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        clients = [RedirectAwareClient(c.base_url, conductor) for c in fx.clients]
        fx.broadcast_view(conductor.get_view())

        # Partition node 2 (a backup) off. The primary (node 0) can still
        # reach node 1, but the sync loop must hear from ALL view members,
        # so any PUT will hang waiting on node 2's ack.
        log("\n> PARTITION NODE 2 OFF")
        conductor.partition([2], "isolated")

        # PUT val1 to key1 -- must time out (408) because node 2 is
        # unreachable and the primary can't complete syncToReplicas.
        log("\n> PUT val1 (must time out)")
        r = clients[0].put("key1", "val1", timeout=5)
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT val1 should have timed out (408), got {r.status_code}"
        )

        # Heal the partition. Note: the val1 PUT handler is still running
        # server-side; once node 2 is reachable, val1 will eventually replicate
        # and commit. That's fine -- val2 below will have a strictly higher
        # clock and must end up as the final value.
        log("\n> HEAL PARTITION")
        conductor.partition([0, 1, 2], None)

        # PUT val2 to key1 -- must succeed now that all replicas are reachable.
        log("\n> PUT val2 (must succeed)")
        r = clients[0].put("key1", "val2")
        assert r.status_code == 200, f"PUT val2: {r.status_code}"

        # GET key1 from every node -- must be 'val2'. Catches a bug where the
        # ordering of (eventually-completed val1 sync, val2 sync) leaves a
        # backup holding val1, or where the primary's pendingPuts queue
        # commits val1 *after* val2 locally.
        log("\n> GET key1 FROM ALL NODES (must be val2)")
        for i, c in enumerate(clients):
            r = c.get("key1")
            assert r.status_code == 200, f"GET key1 from node {i}: {r.status_code}"
            assert r.text == "val2", (
                f"node {i} key1: expected 'val2', got '{r.text}'"
            )

        conductor.dump_all_container_logs(dir)
    return True, "ok"
