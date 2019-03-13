import argparse
import contextlib

from distributed import get_task_stream


profile = argparse.ArgumentParser(add_help=False)
profile.add_argument("--profile", action="store_true",
                     help="Whether to collect and save task information.")

protocol = argparse.ArgumentParser(add_help=False, parents=[profile])
protocol.add_argument("-p", "--protocol", choices=['ucx', 'tcp', 'inproc'],
                      default="ucx")

scheduler = argparse.ArgumentParser(add_help=False, parents=[profile])
scheduler.add_argument("scheduler_address",
                       help="Address for a running scheduler.")


def maybe_setup_profile(profile, benchmark, protocol):
    if profile:
        name = f'{benchmark}-{protocol}.html'
        ctx = get_task_stream(
            plot='save',
            filename=name,
        )
    else:
        ctx = contextlib.nullcontext()

    return ctx
