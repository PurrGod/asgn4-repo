# designed by iyyam/ajagathe/brcalcan
# Written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSClient, KVSTestFixture
from provided_tests.utils.util import Logger
from tests.utils.redirect_client import RedirectAwareClient


def remove_non_primary(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=3) as fx:
        fx.broadcast_view(conductor.get_view())

        client0 = RedirectAwareClient(fx.clients[0].base_url, conductor)
        client1 = RedirectAwareClient(fx.clients[1].base_url, conductor)
        client2 = RedirectAwareClient(fx.clients[2].base_url, conductor)

        # PUT a key via primary (node 0)
        r = client0.put("test1", "hello")
        assert r.status_code == 200, f"expected put 200, got {r.status_code}"

        # GET it back from each node
        for i, client in enumerate([client0, client1, client2]):
            r = client.get("test1")
            assert r.status_code == 200, f"expected get 200 from node {i}, got {r.status_code}"
            assert r.text == "hello", f"expected 'hello' from node {i}, got '{r.text}'"

        # Kill node 2 (a non-primary backup)
        log("\n> CRASH NODE 2 (BACKUP)")
        conductor.crash_machine(2)

        # Broadcast new view (only nodes 0 and 1) to the surviving nodes
        node0 = conductor.get_node(0)
        node1 = conductor.get_node(1)
        new_view = {"defaultShard": [node0.get_view(), node1.get_view()]}
        log(f"\n> BROADCAST NEW VIEW: {new_view}")

        for i in (0, 1):
            r = fx.clients[i].send_view(new_view)
            assert r.status_code == 200, f"expected view ack 200 from node {i}, got {r.status_code}"

        # PUT a new key after the view change
        r = client0.put("test2", "world")
        assert r.status_code == 200, f"expected put 200 after view change, got {r.status_code}"

        # GET both keys from both remaining nodes
        for i, client in enumerate([client0, client1]):
            r = client.get("test1")
            assert r.status_code == 200, f"expected get 200 on test1 from node {i}, got {r.status_code}"
            assert r.text == "hello", f"expected 'hello' from node {i}, got '{r.text}'"

            r = client.get("test2")
            assert r.status_code == 200, f"expected get 200 on test2 from node {i}, got {r.status_code}"
            assert r.text == "world", f"expected 'world' from node {i}, got '{r.text}'"

        conductor.dump_all_container_logs(dir)
    return True, "ok"
