# designed by iyyam/ajagathe/brcalcan
# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture
from provided_tests.utils.util import Logger
from ..utils.redirect_client import RedirectAwareClient


def add_lowest_id_node(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        clients = [RedirectAwareClient(c.base_url, conductor) for c in fx.clients]

        # Partition node 0 off so it isn't part of the initial cluster.
        # We'll later add it as the new lowest-ID node (and thus new primary).
        log("\n> PARTITION NODE 0 OFF")
        conductor.partition([0], "isolated")

        # Broadcast a 2-node view with just nodes 1 and 2. Node 1 is the primary.
        node1 = conductor.get_node(1)
        node2 = conductor.get_node(2)
        two_node_view = {"defaultShard": [node1.get_view(), node2.get_view()]}
        log(f"\n> BROADCAST 2-NODE VIEW (primary=node 1): {two_node_view}")
        for i in (1, 2):
            r = fx.clients[i].send_view(two_node_view)
            assert r.status_code == 200, f"node {i} view ack: {r.status_code}"

        # PUT some keys while node 1 is primary
        for i in range(5):
            r = clients[1].put(f"key{i}", f"val{i}")
            assert r.status_code == 200, f"put key{i}: {r.status_code}"

        # Sanity check reads from both alive nodes
        for i in range(5):
            for ni in (1, 2):
                r = clients[ni].get(f"key{i}")
                assert r.status_code == 200, (
                    f"pre-join get key{i} from node {ni}: {r.status_code}"
                )
                assert r.text == f"val{i}", (
                    f"pre-join node {ni} key{i}: got '{r.text}'"
                )

        # Heal partition and broadcast 3-node view.
        # Node 0 joins with the lowest ID -- becomes the new primary.
        log("\n> HEAL + 3-NODE VIEW (node 0 joins as new primary)")
        conductor.partition([0, 1, 2], None)
        full_view = conductor.get_view()
        log(f"  new view: {full_view}")
        fx.broadcast_view(full_view)

        # All previous data must be readable from every node.
        # GETs from non-primaries should 307 to node 0 (new primary),
        # which received the data via sync from nodes 1 and 2.
        for i in range(5):
            for ni in range(3):
                r = clients[ni].get(f"key{i}")
                assert r.status_code == 200, (
                    f"post-join get key{i} from node {ni}: {r.status_code}"
                )
                assert r.text == f"val{i}", (
                    f"post-join node {ni} key{i}: got '{r.text}'"
                )

        # A new PUT (routed through the new primary, node 0) should land everywhere
        r = clients[0].put("after", "post-join")
        assert r.status_code == 200, f"post-join put: {r.status_code}"

        for ni in range(3):
            r = clients[ni].get("after")
            assert r.status_code == 200, f"post-join get 'after' from {ni}: {r.status_code}"
            assert r.text == "post-join", (
                f"post-join node {ni} 'after': got '{r.text}'"
            )

        conductor.dump_all_container_logs(dir)
    return True, "ok"
