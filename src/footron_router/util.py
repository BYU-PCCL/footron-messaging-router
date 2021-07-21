import asyncio
import time
from typing import Callable


async def asyncio_interval(fn: Callable, interval: float):
    while True:
        loop_start = time.time()

        await fn()

        # Subtract loop time to keep interval reasonably consistent
        sleep_time = max(interval - (time.time() - loop_start), 0)
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)
