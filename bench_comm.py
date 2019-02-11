"""
Benchmark for ...

Adapted from https://gist.github.com/pitrou/245e24de52dec34e03cfc4148c001466
"""
import argparse
from time import perf_counter as clock

from tornado.ioloop import IOLoop
from tornado import gen

#import asyncio
#import uvloop
#asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import numpy as np

from distributed.comm import connect, listen
import distributed.comm.ucx
from distributed.protocol import (to_serialize, Serialized, serialize,
                                  deserialize)


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--protocol", choices=['tcp', 'ucx'],
                        default='tcp')
    parser.add_argument("-n", "--n-mb", type=int,
                        default=100)

    return parser.parse_args(args)


async def server_handle_comm(comm):
    msg = await comm.read()
    assert msg['op'] == 'ping'
    msg['op'] = 'pong'
    await comm.write(msg)
    await comm.close()


async def run_bench(protocol, nbytes, niters):
    data = np.random.randint(0, 255, size=nbytes, dtype=np.uint8)
    item = Serialized(*serialize(data))

    import pdb; pdb.set_trace()
    if protocol == 'tcp':
        listener = listen('tcp://127.0.0.1', server_handle_comm,
                          deserialize=False)
    else:
        listener = listen(distributed.comm.ucx.ADDRESS,
                          server_handle_comm, deserialize=False)
    listener.start()

    start = clock()

    for i in range(niters):
        comm = await connect(listener.contact_address, deserialize=False)
        await comm.write({'op': 'ping', 'item': item})
        msg = await comm.read()
        assert msg['op'] == 'pong'
        assert isinstance(msg['item'], Serialized)
        await comm.close()
        print('.', end='', flush=True)
    print()

    end = clock()

    listener.stop()

    dt = end - start
    rate = len(data) * niters / dt
    print("duration: %s => rate: %d MB/s"
          % (dt, rate / 1e6))


async def main(args=None):
    args = parse_args(args)
    await run_bench(args.protocol, args.n_mb * 1000**2, 10)  # 100 MB
    # yield run_bench(10 * 1000**2, 100)  # 10 MB
    # yield run_bench(1 * 1000**2, 1000)  # 1 MB


if __name__ == '__main__':
    IOLoop().run_sync(main)
