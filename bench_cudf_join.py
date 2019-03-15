import argparse
from time import perf_counter as clock

import cudf
import dask
import dask.array as da
import dask.dataframe as dd
from distributed import Client, wait

import base


def parse_args(args):
    parser = argparse.ArgumentParser(parents=[base.scheduler])
    parser.add_argument('--n-keys', default=5_000, type=int,
                        help='Number of unique keys.')
    parser.add_argument('--left-rows', default=1_000_000, type=int,
                        help='Number of rows in left frame.')
    parser.add_argument('--right-rows', default=10_000, type=int,
                        help='Number of rows in right frame.')
    return parser.parse_args(args)


def setup():
    import distributed.protocol.cudf  # noqa
    import distributed.protocol.numba  # noqa


def make_data(n_keys, n_rows_l, n_rows_r):
    left = dd.concat([
        da.random.random(n_rows_l).to_dask_dataframe(columns='x'),
        da.random.randint(
            0, n_keys, size=n_rows_l).to_dask_dataframe(columns='id'),
    ], axis=1)

    right = dd.concat([
        da.random.random(n_rows_r).to_dask_dataframe(columns='y'),
        da.random.randint(
            0, n_keys, size=n_rows_r).to_dask_dataframe(columns='id'),
    ], axis=1)
    gleft = left.map_partitions(cudf.from_pandas)
    gright = right.map_partitions(cudf.from_pandas)
    return gleft, gright


def main(args=None):
    args = parse_args(args)

    client = Client(args.scheduler_address)  # noqa
    setup()
    client.run_on_scheduler(setup)
    client.run(setup)

    n_keys = args.n_keys
    n_rows_l = args.left_rows
    n_rows_r = args.left_rows

    gleft, gright = make_data(n_keys, n_rows_l, n_rows_r)

    t0 = clock()
    gleft, gright = dask.persist(gleft, gright)
    wait([gleft, gright])
    t1 = clock()

    print("Persit", t1 - t0)
    out = gleft.merge(gright, on=['id'])
    t2 = clock()
    out.compute()
    t3 = clock()

    print("Finished")
    print("Schedule:", t2 - t1)
    print("Compute :", t3 - t2)


if __name__ == '__main__':
    main()
