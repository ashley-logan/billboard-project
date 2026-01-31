from asyncio import Lock


class AsyncCounter:
    def __init__(
        self, auto_start: bool = True, start_at: int = 0, stop_at: None | int = None
    ):
        self.count: int = start_at
        self.lock: Lock = Lock()
        # by default the counter is ready for use at initialization
        self.is_active: bool = auto_start
        # don't allow starting count == stop at
        if stop_at == start_at:
            raise Exception("cannot create AsyncCounter where start_at == stop_at")
        self.stop_at: None | int = stop_at

    async def add(self, qty: int = 1):
        async with self.lock:
            if not self.is_active:
                raise Exception("counter must be active to call add()")
            # if attempt is made to increment counter past stop_at raise exception
            if (
                self.stop_at is not None
                and abs(self.count + qty) - abs(self.stop_at) > 0
            ):
                raise Exception(
                    f"attempted to update counter from {self.count} -> {self.count + qty} when stop_at value is {self.stop_at}"
                )
            self.count += qty
            # remain active while count is less than stop_at
            self.is_active = self.count < self.stop_at

    async def get(self) -> int:
        async with self.lock:
            return self.count

    async def start(self):
        async with self.lock:
            if self.is_active:
                raise Exception("cannot call start() on an active counter ")
            self.is_active = True

    async def stop(self):
        async with self.lock:
            if not self.is_active:
                raise Exception("cannot call stop() on an inactive counter ")
            self.is_active = False
