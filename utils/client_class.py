from httpx import Timeout, AsyncClient
from httpx_retries import RetryTransport, Retry


class ThrottledClient(AsyncClient):
    def __init__(
        self, TimeoutConfig: dict, RetryConfig: dict, ConnectionClientConfig: dict
    ):
        self.timeout = Timeout(**TimeoutConfig)
        self.retry = Retry(**RetryConfig)

        self.transport = RetryTransport(retry=self.retry)

        super().__init__(
            transport=self.transport, timeout=self.timeout, **ConnectionClientConfig
        )
