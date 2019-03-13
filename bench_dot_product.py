import argparse
from time import perf_counter as clock

import dask.array as da
from distributed import Client
from distributed.utils import format_bytes

import base


def parse_args(args):
    parser = argparse.ArgumentParser(parents=[base.scheduler])
    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)
    client = Client(address=args.scheduler_address)
    protocol = client.scheduler_info()['address'].split(":")[0]
    ctx = base.maybe_setup_profile(args.profile, 'dot-product', protocol)

    print(f"Connected to {client}")
    N = 1_000_000
    P = 1_000
    X = da.random.uniform(size=(N, P), chunks=(N//100, P))
    print(format_bytes(X.nbytes))

    result = X.T.dot(X)
    with ctx:
        start = clock()
        result.compute()
        stop = clock()

    print(f"\tTook {stop - start:0.2f}s")


if __name__ == '__main__':
    main()
