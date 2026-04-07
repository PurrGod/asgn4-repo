import asyncio
from typing import Any, Dict, List

import aiohttp
import requests

from .containers import ClusterConductor
from .util import Logger

"""
Request Timeout status code.
Technically not proper as the server should return the 408 in traditional usage,
but it is a good enough indicator for our purposes
"""
REQUEST_TIMEOUT_STATUS_CODE = 408
DEFAULT_TIMEOUT = 10


def create_json(metadata, value=None):
    result = {"causal-metadata": {}}
    if metadata is not None:
        result["causal-metadata"] = metadata
    if value is not None:
        result["value"] = value
    return result


class KVSTestFixture:
    def __init__(self, conductor: ClusterConductor, dir, log: Logger, node_count: int):
        self.conductor = conductor
        self.dir = dir
        self.node_count = node_count
        self.clients: list[KVSClient] = []
        self.log = log

    def spawn_cluster(self):
        self.log("\n> SPAWN CLUSTER")
        self.conductor.spawn_cluster(node_count=self.node_count)

        for i in range(self.node_count):
            ep = self.conductor.node_external_endpoint(i)
            self.clients.append(KVSClient(ep))

            r = self.clients[i].ping()
            assert r.status_code == 200, f"expected 200 for ping, got {r.status_code}"

            self.log(f"  - node {i} is up")

    def broadcast_view(self, view: Dict[str, List[Dict[str, Any]]]):
        self.log(f"\n> SEND VIEW: {view}")
        for i, client in enumerate(self.clients):
            r = client.send_view(view)
            assert r.status_code == 200, (
                f"expected 200 to ack view, got {r.status_code}"
            )
            self.log(f"view sent to node {i}: {r.status_code} {r.text}")

    async def parallel_broadcast_view(self, view: Dict[str, List[Dict[str, Any]]]):
        self.log(f"\n> SEND VIEW: {view}")

        async def send_view(client: KVSClient, i: int):
            r = await client.async_send_view(view)
            assert r.status == 200, f"expected 200 to ack view, got {r.status}"
            self.log(f"view sent to node {i}: {r.status} {r.text}")

        tasks = [send_view(client, i) for i, client in enumerate(self.clients)]
        await asyncio.gather(*tasks)

    def rebroadcast_view(self, new_view: Dict[str, List[Dict[str, Any]]]):
        for i, client in enumerate(self.clients):
            self.log(f"rebroadcasting view for node {i}")
            r = client.resend_last_view_with_ips_from_new_view(new_view, self.log)
            if r is None:
                return
            assert r.status_code == 200, (
                f"expected 200 to ack view, got {r.status_code}"
            )
            self.log(f"view resent to node {i}: {r.status_code} {r.text}")

    def send_view(self, node_id: int, view: Dict[str, List[Dict[str, Any]]]):
        r = self.clients[node_id].send_view(view)
        assert r.status_code == 200, f"expected 200 to ack view, got {r.status_code}"
        self.log(f"view sent to node {node_id}: {r.status_code} {r.text}")

    def destroy_cluster(self):
        self.conductor.dump_all_container_logs(self.dir)
        self.log("\n> DESTROY CLUSTER")
        self.conductor.destroy_cluster()

    def __enter__(self):
        self.spawn_cluster()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.destroy_cluster()


class KVSClient:
    def __init__(self, base_url: str):
        # set base url without trailing slash
        self.base_url = base_url if not base_url.endswith("/") else base_url[:-1]
        self.keys_edited = set()

    def ping(self, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
        if timeout is not None:
            try:
                return requests.get(f"{self.base_url}/ping", timeout=timeout)
            except requests.exceptions.Timeout:
                r = requests.Response()
                r.status_code = REQUEST_TIMEOUT_STATUS_CODE
                return r
        else:
            return requests.get(f"{self.base_url}/ping")

    def get(self, key: str, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")
        try:
            return requests.get(
                f"{self.base_url}/data/{key}",
                timeout=timeout,
            )
        except requests.exceptions.Timeout:
            r = requests.Response()
            r.status_code = REQUEST_TIMEOUT_STATUS_CODE
            return r

    def put(
        self, key: str, value: str, timeout: float = DEFAULT_TIMEOUT
    ) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")

        self.keys_edited.add(key)
        try:
            return requests.put(
                f"{self.base_url}/data/{key}",
                data=value,
                headers={"Content-Type": "text/plain"},
                timeout=timeout,
            )
        except requests.exceptions.Timeout:
            r = requests.Response()
            r.status_code = REQUEST_TIMEOUT_STATUS_CODE
            return r

    def send_view(
        self,
        view: Dict[str, List[Dict[str, int | str]]],
        timeout: float = DEFAULT_TIMEOUT,
    ) -> requests.Response:
        self.last_view = view
        return requests.put(f"{self.base_url}/view", json=view, timeout=timeout)

    async def async_send_view(
        self, view: dict[str, List[Dict[str, Any]]], timeout: float | None = None
    ) -> aiohttp.ClientResponse:
        self.last_view = view
        request_body = {"view": view}

        async with aiohttp.ClientSession() as session:
            async with session.put(
                f"{self.base_url}/view",
                json=request_body,
                timeout=aiohttp.ClientTimeout(timeout),
            ) as response:
                return response

    def resend_last_view_with_ips_from_new_view(
        self,
        current_view: Dict[str, List[Dict[str, Any]]],
        log: Logger,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> requests.Response | None:
        if not hasattr(self, "last_view"):
            return
            raise LookupError("Must have sent at least one view before calling resend.")
        flattened_current_view = {}
        for shard_key in current_view:
            for node in current_view[shard_key]:
                flattened_current_view[node["id"]] = node["address"]

        for shard_key in self.last_view:
            for node in self.last_view[shard_key]:
                node["address"] = flattened_current_view[node["id"]]

        request_body = {"view": self.last_view}
        log(f"Sending new view: {self.last_view}")
        return requests.put(f"{self.base_url}/view", json=request_body, timeout=timeout)
