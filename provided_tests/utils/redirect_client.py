# written by Claude

import requests
from provided_tests.utils.containers import ClusterConductor
from provided_tests.utils.kvs_api import KVSClient, DEFAULT_TIMEOUT, REQUEST_TIMEOUT_STATUS_CODE


class RedirectAwareClient(KVSClient):
    """
    Wraps KVSClient to translate 307 redirect locations from internal Docker
    network addresses to localhost external ports before following them.
    """

    def __init__(self, base_url: str, conductor: ClusterConductor):
        super().__init__(base_url)
        self.conductor = conductor

    def _resolve(self, url: str) -> str:
        for node in self.conductor.nodes:
            internal = f"http://{node.ip}:{node.port}"
            if url.startswith(internal):
                return url.replace(internal, node.external_endpoint(), 1)
        return url

    def get(self, key: str, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")
        try:
            r = requests.get(
                f"{self.base_url}/data/{key}",
                timeout=timeout,
                allow_redirects=False,
            )
            if r.status_code == 307:
                r = requests.get(self._resolve(r.headers["Location"]), timeout=timeout)
            return r
        except requests.exceptions.Timeout:
            r = requests.Response()
            r.status_code = REQUEST_TIMEOUT_STATUS_CODE
            return r

    def put(self, key: str, value: str, timeout: float = DEFAULT_TIMEOUT) -> requests.Response:
        if not key:
            raise ValueError("key cannot be empty")
        self.keys_edited.add(key)
        try:
            r = requests.put(
                f"{self.base_url}/data/{key}",
                data=value,
                headers={"Content-Type": "text/plain"},
                timeout=timeout,
                allow_redirects=False,
            )
            if r.status_code == 307:
                r = requests.put(
                    self._resolve(r.headers["Location"]),
                    data=value,
                    headers={"Content-Type": "text/plain"},
                    timeout=timeout,
                )
            return r
        except requests.exceptions.Timeout:
            r = requests.Response()
            r.status_code = REQUEST_TIMEOUT_STATUS_CODE
            return r