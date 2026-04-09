from ..utils.containers import ClusterConductor
from ..utils.kvs_api import KVSClient
from ..utils.util import Logger


def hello_cluster(conductor: ClusterConductor, dir, log: Logger):
    # create a cluster
    log("\n> SPAWN CLUSTER")
    conductor.spawn_cluster(node_count=1)

    # describe cluster
    log("\n> DESCRIBE CLUSTER")
    conductor.describe_cluster()

    # talk to node 0 in the cluster
    log("\n> TALK TO NODE 0")
    n0_ep = conductor.node_external_endpoint(0)
    n0_client = KVSClient(n0_ep)
    n0_client.ping().raise_for_status()
    log(f"  - node 0 is up at {n0_ep}")

    conductor.dump_all_container_logs(dir)
    # clean up
    log("\n> DESTROY CLUSTER")
    conductor.destroy_cluster()

    # return score/reason
    return True, "ok"
