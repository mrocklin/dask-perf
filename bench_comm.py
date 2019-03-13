"""
Benchmark for ...

Adapted from https://gist.github.com/pitrou/245e24de52dec34e03cfc4148c001466
"""
import argparse
from time import perf_counter as clock

import numpy as np
import ucp_py as ucp
from tornado.ioloop import IOLoop
from distributed.comm import connect, listen
from distributed.protocol import Serialized, serialize
from distributed.utils import parse_bytes

import base


def parse_args(args=None):
    parser = argparse.ArgumentParser(parents=[base.protocol])
    parser.add_argument("-n", "--n-bytes", type=parse_bytes,
                        default="100 Mb",
                        help="Message size, default '100 Mb'.")

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

    if protocol == 'tcp':
        listener = listen('tcp://127.0.0.1', server_handle_comm,
                          deserialize=False)
    else:
        listener = listen('ucx://' + ucp.get_address(),
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
    await run_bench(args.protocol, args.n_bytes, 10)  # 100 MB
    # yield run_bench(10 * 1000**2, 100)  # 10 MB
    # yield run_bench(1 * 1000**2, 1000)  # 1 MB


if __name__ == '__main__':
    IOLoop().run_sync(main)
