# designed by iyyam/ajagathe/brcalcan
# Written by Claude

import time

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture, REQUEST_TIMEOUT_STATUS_CODE
from provided_tests.utils.util import Logger
from ..utils.redirect_client import RedirectAwareClient

def view_change_unblocks_put(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        clients = [RedirectAwareClient(c.base_url, conductor) for c in fx.clients]
        fx.broadcast_view(conductor.get_view())

        # Partition node 2 off so the primary can't reach it for sync.
        log("\n> PARTITION NODE 2 OFF")
        conductor.partition([2], "isolated")

        # PUT val1 -- client times out (408). Server's handler keeps retrying
        # the sync to node 2 in the background; nothing observable yet.
        log("\n> PUT val1 (must time out)")
        r = clients[0].put("key1", "val1", timeout=5)
        assert r.status_code == REQUEST_TIMEOUT_STATUS_CODE, (
            f"PUT val1 should have timed out (408), got {r.status_code}"
        )

        # Wait 5s -- server is still retrying. Confirms nothing weird happens
        # to the in-flight PUT during a quiet period.
        log("\n> WAIT 5s (server retrying val1 sync in background)")
        time.sleep(5)

        # View change: shrink to {0, 1}. The pending val1 sync loop pulls
        # nodeIDsSet via a getter each iteration, so it picks up the shrunk
        # view; once node 2 is no longer required, the loop exits and val1
        # commits server-side on nodes 0 and 1.
        log("\n> SHRINK VIEW TO {0, 1} (unblocks pending val1 sync)")
        node0 = conductor.get_node(0)
        node1 = conductor.get_node(1)
        two_node_view = {"defaultShard": [node0.get_view(), node1.get_view()]}
        for i in (0, 1, 2):
            r = fx.clients[i].send_view(two_node_view)
            assert r.status_code == 200, (
                f"node {i} shrink view ack: {r.status_code}"
            )

        # Wait 5s -- gives the server time to drain its pendingPuts queue
        # and finish committing val1 locally.
        log("\n> WAIT 5s (let val1 commit locally on the primary)")
        time.sleep(5)

        # Heal. Node 2 reconnects on the network but still believes the view
        # is {0, 1, 2} (the shrink broadcast didn't reach it).
        log("\n> UNPARTITION")
        conductor.partition([0, 1, 2], None)

        # The data that was PUT must be accessible from every node:
        #   - node 0 / node 1: serve val1 locally (in view, val1 committed).
        #   - node 2: still thinks node 0 is primary; 307s to node 0, which
        #     holds val1. RedirectAwareClient follows the redirect.
        log("\n> GET val1 FROM ALL NODES (must be accessible)")
        for i, c in enumerate(clients):
            r = c.get("key1")
            assert r.status_code == 200, (
                f"GET key1 from node {i}: {r.status_code}"
            )
            assert r.text == "val1", (
                f"node {i} key1: expected 'val1', got '{r.text}'"
            )

        conductor.dump_all_container_logs(dir)
    return True, "ok"
