#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

"""Mount the fixture only inside its container, run a command, then unmount.

Readiness comes from FUSE INIT through a pipe, not a sleep or a polling timer.
The backing directory can be a task-owned volume to preserve files on restart.
"""

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mount", action="append", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a command is required after --")
    profile = os.environ.get("WEAVER_FS_PROFILE", "unraid")
    if profile not in ("unraid", "portable"):
        parser.error("WEAVER_FS_PROFILE must be unraid or portable")
    mounted = []
    daemons = []
    child = None
    result = 1

    def terminate(signum, _frame):
        if child is not None:
            child.send_signal(signum)
        else:
            raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        for index, value in enumerate(args.mount):
            target = Path(value)
            if not target.is_absolute() or str(target) in ("/", "/data", "/config"):
                raise ValueError(f"use a dedicated absolute download/test directory: {target}")
            target.mkdir(parents=True, exist_ok=True)
            if any(target.iterdir()):
                raise ValueError(f"refusing to hide existing files under {target}")
            backing = Path("/fs-fixture/backing") / str(index)
            backing.mkdir(parents=True, exist_ok=True)
            os.chown(backing, int(os.environ.get("PUID", "1000")), int(os.environ.get("PGID", "1000")))
            read_fd, write_fd = os.pipe()
            env = dict(os.environ, WEAVER_FS_PROFILE=profile, WEAVER_FS_READY_FD=str(write_fd))
            daemon = subprocess.Popen(
                ["/fs-fixture/filesystem", str(backing), str(target), "-f", "-s",
                 "-o", "allow_other,default_permissions"],
                env=env, pass_fds=(write_fd,),
            )
            daemons.append(daemon)
            os.close(write_fd)
            try:
                ready = os.read(read_fd, 1)
            finally:
                os.close(read_fd)
            if ready != b"1":
                raise RuntimeError(f"FUSE mount failed for {target}; /dev/fuse and SYS_ADMIN are required")
            mounted.append(target)
            print(f"filesystem-fixture ready: {profile} {target}", flush=True)
        child = subprocess.Popen(command)
        result = child.wait()
    finally:
        # No pre-existing container, mount, process, or directory is torn down.
        if child is not None and child.poll() is None:
            child.terminate()
            child.wait()
        if child is not None and result:
            print(f"filesystem-fixture: command exited {result}", file=sys.stderr, flush=True)
        unmounted = set()
        for target in reversed(mounted):
            unmount = subprocess.run(["fusermount3", "-u", str(target)])
            if unmount.returncode:
                print(f"filesystem-fixture: unmounting {target} exited {unmount.returncode}", file=sys.stderr, flush=True)
                result = 1
            else:
                unmounted.add(target)
        for index, daemon in enumerate(daemons):
            target = Path(args.mount[index])
            if target in unmounted:
                # The unmount ends the helper's loop, so it exits by itself,
                # and 0 is the only clean end: a crash or an error exit fails
                # the run. The wait has no deadline of its own; a helper that
                # never ends is a hang the gate's own bound reports.
                code = daemon.wait()
                if code != 0:
                    print(f"filesystem-fixture: helper for {target} exited {code} after its unmount", file=sys.stderr, flush=True)
                    result = 1
                continue
            # The mount never came up, or its unmount failed: nothing will end
            # the helper's loop, so it is stopped. The run has already failed
            # for that reason; how the stopped helper exits does not change it.
            if daemon.poll() is None:
                daemon.terminate()
            code = daemon.wait()
            print(f"filesystem-fixture: helper for {target} was not unmounted and exited {code}", file=sys.stderr, flush=True)
    return result


if __name__ == "__main__":
    sys.exit(main())
