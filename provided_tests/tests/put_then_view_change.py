# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture
from provided_tests.utils.util import Logger
from tests.utils.redirect_client import RedirectAwareClient


def put_then_view_change(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        clients = [RedirectAwareClient(c.base_url, conductor) for c in fx.clients]

        # --- Scenario A: PUT then immediately add a node to the view ---
        log("\n> SCENARIO A: PUT then add node")

        # Start with node 2 partitioned off; only nodes 0 and 1 in the view
        conductor.partition([2], "isolated")
        node0 = conductor.get_node(0)
        node1 = conductor.get_node(1)
        two_node_view = {"defaultShard": [node0.get_view(), node1.get_view()]}
        for i in (0, 1):
            r = fx.clients[i].send_view(two_node_view)
            assert r.status_code == 200, f"node {i} view ack: {r.status_code}"

        # PUT a few keys, each acked before next
        for i in range(5):
            r = clients[0].put(f"a{i}", f"val-a{i}")
            assert r.status_code == 200, f"put a{i}: {r.status_code}"

        # Immediately bring node 2 back and broadcast 3-node view
        log("\n> HEAL + 3-NODE VIEW (no delay)")
        conductor.partition([0, 1, 2], None)
        three_node_view = conductor.get_view()
        fx.broadcast_view(three_node_view)

        # All keys must be visible from every node (including the newcomer)
        for i in range(5):
            for node_i, client in enumerate(clients):
                r = client.get(f"a{i}")
                assert r.status_code == 200, (
                    f"get a{i} from node {node_i}: {r.status_code}"
                )
                assert r.text == f"val-a{i}", (
                    f"node {node_i} a{i}: expected 'val-a{i}', got '{r.text}'"
                )

        # --- Scenario B: PUT then immediately remove a non-primary ---
        log("\n> SCENARIO B: PUT then remove non-primary")

        # PUT a fresh batch with all 3 nodes in view
        for i in range(5):
            r = clients[0].put(f"b{i}", f"val-b{i}")
            assert r.status_code == 200, f"put b{i}: {r.status_code}"

        # Immediately drop node 2 from view (no crash, just remove from membership)
        log("\n> SHRINK TO 2-NODE VIEW")
        shrink_view = {"defaultShard": [node0.get_view(), node1.get_view()]}
        for i in (0, 1):
            r = fx.clients[i].send_view(shrink_view)
            assert r.status_code == 200, f"node {i} shrink view ack: {r.status_code}"

        # b-keys must still be readable from the remaining nodes
        for i in range(5):
            for node_i in (0, 1):
                r = clients[node_i].get(f"b{i}")
                assert r.status_code == 200, (
                    f"get b{i} from node {node_i}: {r.status_code}"
                )
                assert r.text == f"val-b{i}", (
                    f"node {node_i} b{i}: expected 'val-b{i}', got '{r.text}'"
                )

        # PUT a new key after view shrink, verify it lands on remaining nodes
        r = clients[0].put("after", "post-shrink")
        assert r.status_code == 200, f"put after: {r.status_code}"

        for node_i in (0, 1):
            r = clients[node_i].get("after")
            assert r.status_code == 200, f"get after from node {node_i}: {r.status_code}"
            assert r.text == "post-shrink", (
                f"node {node_i} after: expected 'post-shrink', got '{r.text}'"
            )

        conductor.dump_all_container_logs(dir)
    return True, "ok"
