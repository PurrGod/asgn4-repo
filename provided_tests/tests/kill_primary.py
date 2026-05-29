# written by Claude

from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSClient, KVSTestFixture
from provided_tests.utils.util import Logger


def kill_primary(conductor: ClusterConductor, dir, log: Logger):
    with KVSTestFixture(conductor, dir, log, node_count=2) as fx:
        fx.broadcast_view(conductor.get_view())

        client0 = fx.clients[0]
        client1 = fx.clients[1]

        # PUT a key via primary (node 0)
        r = client0.put("test1", "hello")
        assert r.status_code == 200, f"expected put 200, got {r.status_code}"

        # GET it back from primary
        r = client0.get("test1")
        assert r.status_code == 200, f"expected get 200, got {r.status_code}"
        assert r.text == "hello", f"expected 'hello', got '{r.text}'"

        # Kill node 0 (the primary)
        log("\n> CRASH NODE 0 (PRIMARY)")
        conductor.crash_machine(0)

        # Broadcast new view with only node 1 to node 1
        node1 = conductor.get_node(1)
        new_view = {"defaultShard": [node1.get_view()]}
        log(f"\n> BROADCAST NEW VIEW TO NODE 1: {new_view}")
        r = client1.send_view(new_view)
        assert r.status_code == 200, f"expected view ack 200, got {r.status_code}"

        # GET from node 1 -- it's now the primary and should have the data
        r = client1.get("test1")
        assert r.status_code == 200, f"expected get 200 from new primary, got {r.status_code}"
        assert r.text == "hello", f"expected 'hello', got '{r.text}'"

        conductor.dump_all_container_logs(dir)
    return True, "ok"
