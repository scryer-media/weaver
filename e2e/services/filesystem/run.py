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
        for target in reversed(mounted):
            unmount = subprocess.run(["fusermount3", "-u", str(target)])
            if unmount.returncode:
                result = 1
        for daemon in daemons:
            if daemon.poll() is None:
                daemon.terminate()
            if daemon.wait() not in (0, -signal.SIGTERM):
                result = 1
    return result


if __name__ == "__main__":
    sys.exit(main())
