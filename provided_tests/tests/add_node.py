# designed by iyyam/ajagathe/brcalcan
# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSTestFixture
from provided_tests.utils.util import Logger
from tests.utils.redirect_client import RedirectAwareClient


def add_node(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        client0 = RedirectAwareClient(fx.clients[0].base_url, conductor)
        client1 = RedirectAwareClient(fx.clients[1].base_url, conductor)
        client2 = RedirectAwareClient(fx.clients[2].base_url, conductor)

        # Partition node 2 off so it's not part of the initial cluster
        log("\n> PARTITION NODE 2 OFF")
        conductor.partition([2], "isolated")

        # Broadcast view with only nodes 0 and 1 to those two
        node0 = conductor.get_node(0)
        node1 = conductor.get_node(1)
        two_node_view = {"defaultShard": [node0.get_view(), node1.get_view()]}
        log(f"\n> BROADCAST 2-NODE VIEW: {two_node_view}")
        for i in (0, 1):
            r = fx.clients[i].send_view(two_node_view)
            assert r.status_code == 200, f"expected view ack 200 from node {i}, got {r.status_code}"

        # PUT some keys (only nodes 0 and 1 have these)
        r = client0.put("test1", "hello")
        assert r.status_code == 200, f"expected put 200, got {r.status_code}"
        r = client0.put("test2", "world")
        assert r.status_code == 200, f"expected put 200, got {r.status_code}"

        # Heal the partition -- put node 2 back on the base network
        log("\n> HEAL PARTITION (node 2 rejoins base network)")
        conductor.partition([0, 1, 2], None)

        # Broadcast the full 3-node view to all -- this should sync data to node 2
        full_view = conductor.get_view()
        log(f"\n> BROADCAST 3-NODE VIEW: {full_view}")
        fx.broadcast_view(full_view)

        # GET both keys from node 2 -- should have data via sync (or redirect)
        r = client2.get("test1")
        assert r.status_code == 200, f"expected get 200 on test1 from node 2, got {r.status_code}"
        assert r.text == "hello", f"expected 'hello' from node 2, got '{r.text}'"

        r = client2.get("test2")
        assert r.status_code == 200, f"expected get 200 on test2 from node 2, got {r.status_code}"
        assert r.text == "world", f"expected 'world' from node 2, got '{r.text}'"

        # PUT a new key after node 2 joined; verify it lands on all nodes
        r = client0.put("test3", "foo")
        assert r.status_code == 200, f"expected put 200 after add, got {r.status_code}"

        for i, client in enumerate([client0, client1, client2]):
            r = client.get("test3")
            assert r.status_code == 200, f"expected get 200 on test3 from node {i}, got {r.status_code}"
            assert r.text == "foo", f"expected 'foo' from node {i}, got '{r.text}'"

        conductor.dump_all_container_logs(dir)
    return True, "ok"
