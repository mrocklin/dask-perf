from bench_comm import run_bench
import asyncio
import cupy

x = cupy.random.randint(0, 255, 100_000_000, dtype='uint8')

loop = asyncio.get_event_loop()
loop.run_until_complete(run_bench('ucx', 0, 50, data=x))

