import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, List

import requests

from .util import Logger, run_cmd_bg

if TYPE_CHECKING:
    from .kvs_api import KVSTestFixture

CONTAINER_ENGINE = os.getenv("ENGINE", "docker")
REBROADCAST_VIEW = os.getenv("REBROADCAST_VIEW", "false")


class ContainerBuilder:
    def __init__(self, project_dir: str, image_id: str):
        self.project_dir = project_dir
        self.image_id = image_id

    def build_image(self, log: Logger) -> None:
        # ensure we are able to build the container image
        log(f"building container image {self.image_id}...")

        cmd = [CONTAINER_ENGINE, "build", "-t", self.image_id, self.project_dir]
        run_cmd_bg(cmd, verbose=True, error_prefix="failed to build container image")

        # ensure the image exists
        log(f"inspecting container image {self.image_id}...")
        cmd = [CONTAINER_ENGINE, "image", "inspect", self.image_id]
        run_cmd_bg(cmd, verbose=True, error_prefix="failed to inspect container image")


@dataclass
class ClusterNode:
    name: str  # container name
    index: int  # container global id/index
    ip: str  # container ip on current/primary network
    port: int  # container http service port
    external_port: (
        int  # host's mapped external port forwarded to container's service port
    )
    networks: List[str]  # networks the container is attached to

    def get_view(self) -> dict[str, str | int]:
        return {"address": f"{self.ip}:{self.port}", "id": self.index}

    def internal_endpoint(self) -> str:
        return f"http://{self.ip}:{self.port}"

    def external_endpoint(self) -> str:
        return f"http://localhost:{self.external_port}"


DEFAULT_SHARD_NAME = "defaultShard"


class ClusterConductor:
    if TYPE_CHECKING:
        _parent: KVSTestFixture

    def __init__(
        self,
        group_id: str,
        thread_id: str,
        base_image: str,
        log: Logger,
        external_port_base: int = 8081,
    ):
        self.group_id = group_id
        self.thread_id = thread_id
        self.base_image = base_image
        self.base_port = external_port_base
        self.nodes: List[ClusterNode] = []
        self.shards: dict[str, List[ClusterNode]] = {DEFAULT_SHARD_NAME: []}

        # naming patterns
        self.group_ctr_prefix = f"kvs_{group_id}_{thread_id}_node"
        self.group_net_prefix = f"kvs_{group_id}_{thread_id}_net"

        # base network
        self.base_net_name = f"{self.group_net_prefix}_base"

        self.log = log

    def _list_containers(self) -> List[str]:
        # get list of all container names
        try:
            output = subprocess.check_output(
                [CONTAINER_ENGINE, "ps", "-a", "--format", "{{.Names}}"]
            )
            return output.decode().strip().split("\n")
        except subprocess.CalledProcessError as e:
            self.log("failed to list containers")
            self.log(e.stderr.decode())
            raise

    def _list_networks(self) -> List[str]:
        # get list of all network names
        try:
            output = subprocess.check_output(
                [CONTAINER_ENGINE, "network", "ls", "--format", "{{.Name}}"]
            )
            return output.decode().strip().split("\n")
        except subprocess.CalledProcessError as e:
            self.log("failed to list networks")
            self.log(e.stderr.decode())
            raise

    def _make_remove_cmd(self, name: str) -> List[str]:
        if CONTAINER_ENGINE == "podman":
            return [CONTAINER_ENGINE, "rm", "-f", "-t", "0", name]
        else:
            return [CONTAINER_ENGINE, "rm", "-f", name]

    def dump_all_container_logs(self, dir):
        self.log("dumping logs of kvs containers")
        container_pattern = f"^kvs_{self.group_id}_{self.thread_id}_.*"
        container_regex = re.compile(container_pattern)

        containers = self._list_containers()
        for container in containers:
            if container and container_regex.match(container):
                self._dump_container_logs(dir, container)

    def _dump_container_logs(self, dir, name: str) -> None:
        log_file = os.path.join(dir, f"{name}.log")
        self.log(f"Dumping logs for container {name} to file {log_file}")

        # Construct the logs command. Docker and Podman both support the "logs" command.
        logs_cmd = [CONTAINER_ENGINE, "logs", name]

        try:
            logs_output = subprocess.check_output(logs_cmd, stderr=subprocess.STDOUT)
            with open(log_file, "wb") as f:
                f.write(logs_output)
            self.log(f"Successfully wrote logs for container {name} to {log_file}")
        except subprocess.CalledProcessError as e:
            self.log(
                f"Error dumping logs for container {name}: {e.output.decode().strip()}"
            )
        except Exception as e:
            self.log(f"Unexpected error dumping logs for container {name}: {e}")

    def _remove_container(self, name: str) -> None:
        # remove a single container
        self.log(f"removing container {name}")
        run_cmd_bg(
            self._make_remove_cmd(name),
            verbose=True,
            error_prefix=f"failed to remove container {name}",
        )

    def _remove_containers(self, names: list[str]) -> None:
        if len(names) == 0:
            return
        self.log(f"removing containers {names}")
        run_cmd_bg(
            self._make_remove_cmd(names[0]) + names[1:],
            verbose=True,
            error_prefix=f"failed to remove containers {names}",
        )

    def _remove_network(self, name: str) -> None:
        # remove a single network
        self.log(f"removing network {name}")
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "rm", name],
            verbose=True,
            error_prefix=f"failed to remove network {name}",
            check=False,
        )

    def _create_network(self, name: str) -> None:
        # create a single network
        self.log(f"creating network {name}")
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "create", name],
            verbose=True,
            error_prefix=f"failed to create network {name}",
        )

    def _network_exists(self, name: str) -> bool:
        return name in self._list_networks()

    def cleanup_hanging(self, group_only: bool = True) -> None:
        # if group_only, only clean up stuff for this group
        # otherwise clean up anything kvs related
        if group_only:
            self.log(f"cleaning up group {self.group_id}")
            container_pattern = f"^kvs_{self.group_id}_{self.thread_id}_.*"
            network_pattern = f"^kvs_{self.group_id}_{self.thread_id}_net_.*"
        else:
            self.log("cleaning up all kvs containers and networks")
            container_pattern = "^kvs_.*"
            network_pattern = "^kvs_net_.*"

        # compile regex patterns
        container_regex = re.compile(container_pattern)
        network_regex = re.compile(network_pattern)

        # cleanup containers
        self.log(f"  cleaning up {'group' if group_only else 'all'} containers")
        containers = self._list_containers()
        containers_to_remove = [
            container
            for container in containers
            if container and container_regex.match(container)
        ]
        self._remove_containers(containers_to_remove)

        # cleanup networks
        self.log(f"  cleaning up {'group' if group_only else 'all'} networks")
        networks = self._list_networks()
        for network in networks:
            if network and network_regex.match(network):
                self._remove_network(network)

    # we can check if a node is online by GET /ping
    def _is_online(self, node: ClusterNode) -> bool:
        try:
            r = requests.get(f"{node.external_endpoint()}/ping")
            return r.status_code == 200
        except requests.exceptions.RequestException as e:
            self.log(f"node {node.name} is not online: {e}")
            return False

    def _node_name(self, index: int) -> str:
        return f"kvs_{self.group_id}_{self.thread_id}_node_{index}"

    def node_external_endpoint(self, index: int) -> str:
        return self.nodes[index].external_endpoint()

    # create a cluster of nodes on the base network
    def spawn_cluster(self, node_count: int) -> None:
        self.log(f"spawning cluster of {node_count} nodes")

        # delete base network if it exists
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "rm", self.base_net_name],
            check=False,
            log=self.log,
        )

        # create base network
        run_cmd_bg(
            [CONTAINER_ENGINE, "network", "create", self.base_net_name],
            verbose=True,
            error_prefix="failed to create base network",
            log=self.log,
        )

        # spawn the nodes
        for i in range(node_count):
            node_name = self._node_name(i)
            port = 8081  # internal port

            node = self._spawn_node(i, port)
            # store container metadata
            self.nodes.append(node)
            self.shards[DEFAULT_SHARD_NAME].append(node)

            self.log(f"    container {node_name} spawned, base_net_ip={node.ip}")

        # wait for the nodes to come online (sequentially)
        self.log("waiting for nodes to come online...")
        wait_online_start = time.time()
        # wait_online_timeout = 10
        for i in range(node_count):
            node = self.nodes[i]
            while not self._is_online(node):
                if time.time() - wait_online_start > 10:
                    raise RuntimeError(f"node {node.name} did not come online")
                time.sleep(0.2)

            self.log(f"  node {node.name} online")

        self.log("all nodes online")

    def _spawn_node(
        self,
        i: int,
        port: int = 8081,
    ) -> ClusterNode:
        node_name = self._node_name(i)
        # start container detached from networks
        run_cmd = [
            CONTAINER_ENGINE,
            "run",
            "-d",
            "--name",
            node_name,
            "--env",
            f"NODE_IDENTIFIER={i}",
            "-p",
            f"{port}",
            self.base_image,
        ]
        if CONTAINER_ENGINE == "podman":
            run_cmd.insert(2, "--log-driver=k8s-file")
        run_cmd_bg(
            run_cmd,
            verbose=True,
            error_prefix=f"failed to start container {node_name}",
            log=self.log,
        )

        res = run_cmd_bg(
            [CONTAINER_ENGINE, "port", node_name, str(port)],
            verbose=True,
            log=self.log,
        )
        external_port = int(res.stdout.rsplit(":", 1)[1])

        # attach container to base network
        self.log(f"    attaching container {node_name} to base network")
        run_cmd_bg(
            [
                CONTAINER_ENGINE,
                "network",
                "connect",
                self.base_net_name,
                node_name,
            ],
            verbose=True,
            error_prefix=f"failed to attach container {node_name} to base network",
            log=self.log,
        )

        # inspect the container to get ip, etc.
        self.log(f"    inspecting container {node_name}")
        try:
            inspect = subprocess.run(
                [CONTAINER_ENGINE, "inspect", node_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            self.log(f"failed to inspect container {node_name}")
            self.log(e.stderr.decode())
            raise
        info = json.loads(inspect.stdout)[0]

        container_ip = info["NetworkSettings"]["Networks"][self.base_net_name][
            "IPAddress"
        ]

        # store container metadata
        node = ClusterNode(
            name=node_name,
            index=i,
            ip=container_ip,
            port=port,
            external_port=external_port,
            networks=[self.base_net_name],
        )
        return node

    def destroy_cluster(self) -> None:
        # clean up after this group
        self.cleanup_hanging(group_only=True)

        # clear nodes
        self.nodes.clear()

    # if partition_id is not specified then it will default to the default network
    # all nodes a…re in during the initial spawn
    def partition(self, node_ids: List[int], partition_id: str | None) -> None:
        partition_id = partition_id if partition_id is not None else self.base_net_name
        net_name = f"kvs_{self.group_id}_{self.thread_id}_net_{partition_id}"

        self.log(f"creating partition {partition_id} with nodes {node_ids}")
        # create partition network if it doesn't exist
        if not self._network_exists(net_name):
            self._create_network(net_name)

        # disconnect specified nodes from all other networks
        self.log("  disconnecting nodes from other networks")
        for i in node_ids:
            node = self.nodes[i]
            for network in node.networks:
                if network != net_name:
                    self.log(f"    disconnecting {node.name} from network {network}")
                    run_cmd_bg(
                        [CONTAINER_ENGINE, "network", "disconnect", network, node.name],
                        verbose=True,
                        error_prefix=f"failed to disconnect {node.name} from network {
                            network
                        }",
                    )
                    node.networks.remove(network)

        # connect nodes to partition network, and update node ip
        self.log(f"  connecting nodes to partition network {net_name}")
        view_changed = False
        for i in node_ids:
            node = self.nodes[i]

            self.log(f"node.networks: {node.networks}")
            if net_name in node.networks:
                self.log("network alr exists!")
                continue

            self.log(f"    connecting {node.name} to network {net_name}")
            run_cmd_bg(
                [
                    CONTAINER_ENGINE,
                    "network",
                    "connect",
                    net_name,
                    node.name,
                ],
                verbose=True,
                error_prefix=f"failed to connect {node.name} to network {net_name}",
            )
            node.networks.append(net_name)

            # update node ip on the new network
            inspect = subprocess.run(
                [CONTAINER_ENGINE, "inspect", node.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            info = json.loads(inspect.stdout)[0]
            container_ip = info["NetworkSettings"]["Networks"][net_name]["IPAddress"]
            self.log(f"    node {node.name} ip in network {net_name}: {container_ip}")

            # update node ip

            if container_ip != node.ip:
                self.log(
                    f"Warning: Node {i} IP addr changed from {node.ip} to {container_ip}"
                )
                node.ip = container_ip
                if CONTAINER_ENGINE == "podman":
                    if hasattr(self, "_parent"):
                        self._parent.clients[
                            node.index
                        ].base_url = self.node_external_endpoint(node.index)
                    view_changed = True
        if CONTAINER_ENGINE == "podman" and view_changed and hasattr(self, "_parent"):
            self._parent.rebroadcast_view(self.get_view())

        # connect nodes to partition network, and update node ip
        self.log(f"  connecting nodes to partition network {net_name}")
        view_changed = False
        for i in node_ids:
            node = self.nodes[i]
            self.log(f"    connecting {node.name} to network {net_name}")
            run_cmd_bg(
                [
                    CONTAINER_ENGINE,
                    "network",
                    "connect",
                    net_name,
                    node.name,
                ],
                verbose=True,
                error_prefix=f"failed to connect {node.name} to network {net_name}",
            )
            node.networks.append(net_name)

            # update node ip on the new network
            inspect = subprocess.run(
                [CONTAINER_ENGINE, "inspect", node.name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            info = json.loads(inspect.stdout)[0]
            container_ip = info["NetworkSettings"]["Networks"][net_name]["IPAddress"]
            self.log(f"    node {node.name} ip in network {net_name}: {container_ip}")

            # update node ip
            if container_ip != node.ip:
                self.log(
                    f"Warning: Node {i} IP addr changed from {node.ip} to {container_ip}"
                )
                node.ip = container_ip
                if CONTAINER_ENGINE == "podman" or REBROADCAST_VIEW == "true":
                    if hasattr(self, "_parent"):
                        self._parent.clients[
                            node.index
                        ].base_url = self.node_external_endpoint(node.index)
                    view_changed = True
        if (
            (CONTAINER_ENGINE == "podman" or REBROADCAST_VIEW == "true")
            and view_changed
            and hasattr(self, "_parent")
        ):
            self._parent.rebroadcast_view(self.get_view())

    def crash_machine(self, node_id: int) -> None:
        node = self.nodes[node_id]
        self._remove_container(node.name)

    def restore_machine(self, node_id: int) -> None:
        node = self.nodes[node_id]
        self._parent.send_view(node_id, self.get_view())
        self._spawn_node(node.index)

    def describe_cluster(self) -> None:
        self.log(f"TOPOLOGY: group {self.group_id}")
        self.log("nodes:")
        for node in self.nodes:
            self.log(
                f"  {node.name}: {node.ip}:{node.port} <-> localhost:{
                    node.external_port
                }"
            )

        # now log the partitions and the nodes they contain
        partitions = {}
        for node in self.nodes:
            for network in node.networks:
                if network not in partitions:
                    partitions[network] = []
                partitions[network].append(node.index)

        self.log("partitions:")
        for net, nodes in partitions.items():
            part_name = net[len(self.group_net_prefix) + 1 :]
            self.log(f"  {part_name}: {nodes}")

    def get_node(self, index):
        return self.nodes[index]

    def get_nodes(self, node_indexes: List[int]):
        node_list = []
        for i in node_indexes:
            node_list.append(self.nodes[i])
        return node_list

    # from `start` to `end`. Note that `end` is not inclusive
    def get_nodes_seq(self, start=0, end=None):
        if start < 0:
            raise ValueError("Start index cannot be negative.")
        if end is not None:
            if end < 0 or end > len(self.nodes):
                raise ValueError(f"End index must be between 0 and {len(self.nodes)}.")
            if start > end:
                raise ValueError("Start index cannot be greater than end index.")

        if end is None:
            return self.nodes[start:]
        return self.nodes[start:end]

    def add_shard(self, shard_name: str, nodes: List[ClusterNode]):
        if shard_name == DEFAULT_SHARD_NAME:
            raise ValueError(f"shard_name cannot be f{DEFAULT_SHARD_NAME}")
        if shard_name not in self.shards:
            self.shards[shard_name] = []
        self.shards[shard_name] = nodes

    def remove_shard(self, shard_name: str):
        if shard_name == DEFAULT_SHARD_NAME:
            raise ValueError(f"shard_name cannot be f{DEFAULT_SHARD_NAME}")
        del self.shards[shard_name]

    def add_node_to_shard(self, shard_name: str, node: ClusterNode):
        if shard_name not in self.shards:
            self.shards[shard_name] = []
        self.shards[DEFAULT_SHARD_NAME].remove(node)
        self.shards[shard_name].append(node)

    def remove_node_from_shard(self, shard_name: str, node: ClusterNode):
        if shard_name not in self.shards:
            return
        self.shards[shard_name].remove(node)
        self.shards[DEFAULT_SHARD_NAME].append(node)

    def get_view(self) -> dict:
        return {
            shard: [node.get_view() for node in nodes]
            for shard, nodes in self.shards.items()
        }

    def get_partition_view(self, partition_id: str):
        net_name = f"kvs_{self.group_id}_{self.thread_id}_net_{partition_id}"
        view = []
        for node in self.nodes:
            if net_name in node.networks:
                view.append(node.get_view())
        return view

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # clean up automatically
        self.destroy_cluster()
