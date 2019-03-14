#!/usr/bin/env bash
set -eo pipefail

# This is mostly not working right now. Stop with
# kill $(ps aux | grep dask- | awk '{print $2}')
# though be careful...

PROTOCOL=ucx
HOST=10.33.225.160
PORT=13337
NTHREADS=10
NWORKERS=8

usage()
{
        echo "usage: start [[[-p protocol ] | [-h]]"
}

echo $0 $1

while [ "$1" != "" ]; do
    case $1 in
        -p | --protocol )       shift
                                PROTOCOL=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done



PIDS=()

function handle_sigint()
{
    for proc in `jobs -p`
    do
        kill $proc
    done
}

dask-scheduler \
    --host=${PROTOCOL}://${HOST}:${PORT} \
    --pid-file=scheduler.pid \
    --scheduler-file=scheduler.json&
PIDS+=$!

sleep 2
echo "PIDS: ${PIDS}"
NWORKESR2=`expr ${NWORKERS} - 1`

for i in `seq 0 ${NWORKESR2}`; do
    echo "starting worker ${i}"
    CUDA_VISIBLE_DEVICES=${i} dask-worker \
        --scheduler-file=scheduler.json \
        --host=${PROTOCOL}://${HOST} \
        --nthreads=${NTHREADS}&
    PIDS+=$!
done

trap handle_sigint SIGINT

# echo ${PIDS}

# for pid in ${PIDS}; do
#     wait $pid
# done

# echo "[Running bench_array_ops.py]"
# python bench_array_ops.py "ucx://${HOST}:${PORT}"

# echo "[Running bench_dot_product.py]"
# python bench_array_ops.py "ucx://${HOST}:${PORT}"

# echo "[Killing cluster]"
# for pid in ${PIDS}; do
#     kill ${pid}
# done
