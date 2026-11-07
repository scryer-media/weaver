#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

"""Prove the mock's capabilities before trusting a product test on it."""

import ctypes
import errno
import os
from pathlib import Path
import struct
import sys

root = Path(sys.argv[1])
portable = os.environ["WEAVER_FS_PROFILE"] == "portable"
libc = ctypes.CDLL(None, use_errno=True)
source, destination = root / "source", root / "destination"
source.write_bytes(b"source")
libc.renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
assert libc.renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1) == -1
assert ctypes.get_errno() == errno.EINVAL
assert source.read_bytes() == b"source" and not destination.exists()

if portable:
    for operation in [lambda: os.link(source, destination), lambda: os.chmod(source, 0o600),
                      lambda: os.chown(source, os.getuid(), os.getgid())]:
        try:
            operation()
        except OSError as error:
            assert error.errno in (errno.EOPNOTSUPP, errno.EPERM)
        else:
            raise AssertionError("a refused capability unexpectedly succeeded")
else:
    os.link(source, destination)
    source.unlink()
    assert destination.read_bytes() == b"source"
    destination.rename(source)

with source.open("r+b") as file:
    file.seek(1 << 20)
    file.write(b"tail")
    file.flush()
    assert os.fstat(file.fileno()).st_size == (1 << 20) + 4
    if portable:
        assert os.fstat(file.fileno()).st_blocks * 512 >= 1 << 20

# With an open handle libfuse must retain a hidden name, just as network
# filesystems retain a name for a live handle. Observe deletion through
# inotify on the backing directory, tied to this exact hidden entry.
backing = Path("/fs-fixture/backing/0")
watch = libc.inotify_init1(os.O_CLOEXEC)
assert watch >= 0
assert libc.inotify_add_watch(watch, os.fsencode(backing), 0x200) >= 0  # IN_DELETE
with source.open("rb") as file:
    source.unlink()
    hidden = list(root.glob(".fuse_hidden*"))
    assert len(hidden) == 1
    assert file.read(6) == b"source"
name = hidden[0].name
deleted = False
while not deleted:
    events = os.read(watch, 65536)
    offset = 0
    while offset < len(events):
        _, mask, _, size = struct.unpack_from("iIII", events, offset)
        entry = events[offset + 16:offset + 16 + size].rstrip(b"\0").decode()
        deleted |= bool(mask & 0x200) and entry == name
        offset += 16 + size
os.close(watch)
assert not list(root.iterdir())
print(f"filesystem fixture verified: {os.environ['WEAVER_FS_PROFILE']}")
