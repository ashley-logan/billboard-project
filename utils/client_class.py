from httpx import Timeout, AsyncClient
from httpx_retries import RetryTransport, Retry


class ThrottledClient(AsyncClient):
    def __init__(self, timeout_config: dict, retry_config: dict, client_config: dict):
        self.timeout = Timeout(**timeout_config)
        self.retry = Retry(**retry_config)

        self.transport = RetryTransport(retry=self.retry)

        super().__init__(
            transport=self.transport, timeout=self.timeout, **client_config
        )
