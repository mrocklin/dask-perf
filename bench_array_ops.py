import argparse
from time import perf_counter as clock

import dask
import dask.array as da
from distributed.utils import format_bytes

import base


def parse_args(args):
    parser = argparse.ArgumentParser(parents=[base.scheduler])
    return parser.parse_args(args)


def main(args=None):
    args = parse_args(args)
    client = Client(args.scheduler_address)  # noqa
    X = da.random.random(size=(100_000, 10_000), chunks=1_000)

    protocol = client.scheduler_info()['address'].split(":")[0]
    ctx = base.maybe_setup_profile(args.profile, 'bench-array-ops', protocol)
    x = X[:10].dot(X.T).sum(1)

    print("Array size:", format_bytes(X.nbytes))
    print("Client    :", client)
    print("Profile?  :", "yes" if args.profile else "no")
    print("-" * 80)

    with ctx:
        start = clock()
        dask.compute(x.sum(), x.mean(), x.std())
        stop = clock()
    print(f"\t Took {stop - start:0.2f}s")


if __name__ == '__main__':
    main()
