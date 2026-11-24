#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

"""Build current source, then exercise storage and delivery through real FUSE.

Run in the fixture image built with WEAVER_IMAGE=rust:<repo toolchain>-bookworm,
with the checkout mounted read-only at /workspace and the cargo cache volumes
at /cache. Fixture data stay in the disposable container. No archive schedule matrix is executed.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

# The regression tests, by the test target that holds each: (package, target
# kind, target name) and the full test names inside it.
TARGETS = [
    ("weaver-server-pipeline", "test", "pipeline_graph", [
        "pipeline::graph::tests::completion_releases_consumed_storage_before_unlink_and_publication",
        "pipeline::graph::tests::a_delivery_with_a_damaged_member_settles_to_partial",
        "pipeline::graph::tests::a_partial_delivery_restarted_after_its_move_stays_partial",
        "pipeline::graph::tests::two_plain_files_are_written_at_their_names_and_the_assembly_writes_nothing",
        "pipeline::graph::tests::a_hex_named_member_bound_by_its_prefix_is_renamed_and_its_file_moves",
        "pipeline::graph::namespace::tests::delivery_releases_partial_and_retired_files_without_changing_their_bytes",
        "pipeline::graph::engine::tests::dropping_storage_releases_handles_even_when_a_reader_keeps_the_shelf_alive",
        "pipeline::graph::leaf::tests::moving_a_missing_owned_file_keeps_the_old_name_and_intent",
        "pipeline::graph::leaf::tests::moving_an_unwritten_leaf_never_takes_someone_elses_file",
        "pipeline::graph::leaf::tests::removing_an_unwritten_leaf_preserves_another_owners_file",
        "pipeline::graph::leaf::tests::a_write_does_not_preallocate_the_declared_unwritten_tail",
        "pipeline::graph::leaf::tests::truncation_propagates_inspection_errors",
        "pipeline::graph::checkpoint::tests::a_restore_inspection_error_stops_cleanup_before_any_file_is_removed",
        "pipeline::graph::checkpoint::tests::a_restore_keeps_a_file_that_moved_before_its_checkpoint_said_so",
    ]),
    ("weaver-server-disk", "lib", "weaver_server_disk", [
        "runtime::fs::tests::a_fallback_move_through_an_alias_never_unlinks_its_only_entry",
        "runtime::fs::tests::a_mount_without_exclusive_rename_or_links_copies_without_overwriting",
        "runtime::fs::tests::a_mount_without_links_recognizes_an_alias_before_copying",
        "runtime::fs::tests::a_copy_that_cannot_open_its_source_never_cleans_an_existing_destination",
        "runtime::fs::tests::a_refused_exclusive_rename_never_replaces_an_existing_file",
        "runtime::fs::tests::a_refused_exclusive_rename_never_replaces_an_existing_directory",
        "runtime::file_cache::tests::large_copy_helper_preserves_contents",
    ]),
    ("weaver-server-pipeline", "test", "pipeline_units", [
        "pipeline::completion::finalize::output::tests::copy_cleanup_does_not_remove_replaced_parent_destination",
        "pipeline::completion::finalize::output::tests::final_move_does_not_overwrite_existing_destination_file",
        "pipeline::completion::finalize::output::tests::final_move_removes_the_working_directory_with_its_chunk_workspace",
    ]),
]
TESTS = {target: names for _, _, target, names in TARGETS}


def run(binaries):
    root = Path("/fs/cases")
    subprocess.run(["setpriv", "--reuid=1000", "--regid=1000", "--clear-groups", "mkdir", str(root)], check=True)
    try:
        return run_cases(root, binaries)
    finally:
        # Whatever ended the run, the next one starts on an empty mount.
        if root.exists():
            shutil.rmtree(root)


def run_cases(root, binaries):
    oracle = Path("/tmp/fs-oracle")
    oracle.mkdir(exist_ok=True)
    os.chown(oracle, 1000, 1000)
    assert root.stat().st_dev != oracle.stat().st_dev, "the tests must actually cross the FUSE mount"
    assert set(binaries) == set(TESTS), "a regression test target has no binary"
    for target, binary in binaries.items():
        available = subprocess.check_output([binary, "--list"], text=True)
        missing = [name for name in TESTS[target] if name + ": test" not in available]
        assert not missing, f"regression tests missing from {target}: {missing}"
    failures = []
    env = dict(os.environ, TMPDIR=str(root), WEAVER_TEST_FIXTURE_DIR=str(oracle))
    for target, binary in binaries.items():
        for name in TESTS[target]:
            result = subprocess.run(
                ["setpriv", "--reuid=1000", "--regid=1000", "--clear-groups", binary, "--exact", name, "--nocapture"],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            print(result.stdout, end="", flush=True)
            if result.returncode or "1 passed;" not in result.stdout:
                failures.append(f"{target}: {name}")
            # The child has exited. Any remaining hidden handle is a fixture or
            # teardown failure, never something to mask by deleting it first.
            assert not list(root.rglob(".fuse_hidden*")), "a live handle survived child exit"
            for entry in root.iterdir():
                assert entry.name.startswith(".tmp"), entry
                if entry.is_dir() and not entry.is_symlink():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
    total = sum(len(names) for names in TESTS.values())
    print(json.dumps({"profile": os.environ["WEAVER_FS_PROFILE"], "tests": total, "failures": failures}), flush=True)
    return bool(failures)


CARGO_HOME = Path("/cache/cargo-home")
CARGO_TARGET = Path("/cache/cargo-target")
# The build folder keeps artifacts of every dependency and toolchain version it
# has seen. Past this size it starts over empty, so the volume stays bounded.
CARGO_TARGET_LIMIT = 25 * 1024**3


def folder_size(root):
    return sum(os.lstat(os.path.join(folder, name)).st_size
               for folder, _, names in os.walk(root) for name in names)


def bound_build_folder():
    if not CARGO_TARGET.exists():
        return
    size = folder_size(CARGO_TARGET)
    if size > CARGO_TARGET_LIMIT:
        print(f"filesystem-gate: build cache holds {size} bytes, over {CARGO_TARGET_LIMIT}; starting it over", flush=True)
        for entry in CARGO_TARGET.iterdir():
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink()


def build_and_run():
    bound_build_folder()
    env = dict(os.environ, CARGO_TARGET_DIR=str(CARGO_TARGET), CARGO_HOME=str(CARGO_HOME),
               CARGO_PROFILE_TEST_DEBUG="line-tables-only")
    artifacts = Path("/tmp/fs-build.jsonl")
    packages = {}
    for package, kind, target, _ in TARGETS:
        packages.setdefault(package, []).extend(["--lib"] if kind == "lib" else ["--test", target])
    with artifacts.open("w") as output:
        for package, selection in packages.items():
            subprocess.run(
                ["cargo", "test", "--locked", "-p", package, *selection, "--no-run", "--message-format=json"],
                cwd="/workspace", env=env, stdout=output, check=True,
            )
    built = [json.loads(line) for line in artifacts.read_text().splitlines() if line.startswith("{")]
    binaries = {}
    for _, kind, target, _ in TARGETS:
        binaries[target] = next(item["executable"] for item in reversed(built)
                                if item.get("reason") == "compiler-artifact"
                                and item.get("target", {}).get("name") == target
                                and kind in item.get("target", {}).get("kind", [])
                                and item.get("profile", {}).get("test") and item.get("executable"))
    arguments = [f"{target}={binary}" for target, binary in binaries.items()]
    failed = False
    for profile in ("unraid", "portable"):
        for name, command in (("smoke", [sys.executable, "/fs-fixture/smoke.py", "/fs"]),
                              ("regressions", [sys.executable, __file__, "--run", *arguments])):
            result = subprocess.run(
                [sys.executable, "/fs-fixture/run.py", "--mount", "/fs", "--", *command],
                env=dict(os.environ, WEAVER_FS_PROFILE=profile),
            )
            print(f"filesystem-gate: {profile} {name} exited {result.returncode}", flush=True)
            failed |= result.returncode != 0
    return failed


def parse_binaries(arguments):
    return dict(argument.split("=", 1) for argument in arguments)


if __name__ == "__main__":
    sys.exit(run(parse_binaries(sys.argv[2:])) if len(sys.argv) > 2 and sys.argv[1] == "--run" else build_and_run())
