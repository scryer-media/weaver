#!/usr/bin/env python3
"""Collect and run prebuilt Rust test binaries for CI fanout jobs."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path


PAR2_SLOW_TARGETS = {
    "par2cmdline_turbo_functional",
    "real_world_generated",
    "repair_stress",
}
PAR2_SLOW_FILTERS = {
    "weaver_par2": ["crate_fixture_missing_volume_repairs_and_reverifies_clean"],
}

UNRAR_SLOW_TARGETS = {
    "api_parity",
    "failure_paths",
    "generated_multivolume_matrix",
}
UNRAR_SLOW_FILTERS = {
    "imported_corpus": ["imported_ppmd_transition_fixture_extracts_successfully"],
    "integration": [
        "test_rar5_encrypted_multivolume_video_batch",
        "test_rar5_encrypted_multivolume_video_streaming",
        "test_rar4_encrypted_multivolume_video_batch",
        "test_rar5_encrypted_store_batch",
        "test_rar5_encrypted_store_streaming",
        "test_rar5_encrypted_lz_batch",
        "test_rar5_encrypted_lz_streaming",
        "test_rar5_encrypted_multivolume_store_batch",
        "test_rar5_encrypted_multivolume_store_streaming",
        "test_rar5_encrypted_wrong_password_fails",
        "test_rar4_encrypted_store_batch",
        "test_rar4_encrypted_lz_batch",
        "test_rar4_encrypted_multivolume_store_batch",
    ],
}


def load_json_lines(path: Path) -> list[dict]:
    messages = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return messages


def load_package_map(metadata_path: Path, workspace: Path) -> dict[str, dict[str, str]]:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    workspace_members = set(metadata["workspace_members"])
    packages = {}
    for package in metadata["packages"]:
        if package["id"] not in workspace_members:
            continue
        manifest_path = Path(package["manifest_path"]).resolve()
        package_root = manifest_path.parent
        packages[package["id"]] = {
            "name": package["name"],
            "root": str(package_root.relative_to(workspace)),
        }
    return packages


def safe_part(value: str) -> str:
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in value)


def collect(args: argparse.Namespace) -> None:
    workspace = Path(args.workspace).resolve()
    package_map = load_package_map(Path(args.metadata), workspace)
    cargo_messages = load_json_lines(Path(args.cargo_json))
    archive_path = Path(args.archive)
    stage_dir = archive_path.parent / "test-binaries-stage"
    bin_dir = stage_dir / "bin"

    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    bin_dir.mkdir(parents=True)

    entries = []
    seen = set()
    for message in cargo_messages:
        if message.get("reason") != "compiler-artifact":
            continue
        executable = message.get("executable")
        if not executable or not message.get("profile", {}).get("test"):
            continue
        package = package_map.get(message.get("package_id"))
        if not package:
            continue

        target = message["target"]["name"]
        key = (package["name"], target, Path(executable).name)
        if key in seen:
            continue
        seen.add(key)

        rel_bin = (
            Path("bin")
            / safe_part(package["name"])
            / safe_part(target)
            / Path(executable).name
        )
        dest = stage_dir / rel_bin
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(executable, dest)
        dest.chmod(dest.stat().st_mode | 0o111)

        entries.append(
            {
                "package": package["name"],
                "package_root": package["root"],
                "target": target,
                "artifact": rel_bin.as_posix(),
            }
        )

    if not entries:
        raise SystemExit("no Rust test executables were collected")

    entries.sort(key=lambda entry: (entry["package"], entry["target"], entry["artifact"]))
    manifest_path = stage_dir / "manifest.json"
    manifest_path.write_text(json.dumps({"tests": entries}, indent=2) + "\n", encoding="utf-8")

    if archive_path.exists():
        archive_path.unlink()
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(manifest_path, arcname="manifest.json")
        tar.add(bin_dir, arcname="bin")

    print(f"collected {len(entries)} Rust test executables into {archive_path}")


def slow_filters_for(entry: dict) -> list[str]:
    package = entry["package"]
    target = entry["target"]
    if package == "weaver-par2":
        return PAR2_SLOW_FILTERS.get(target, [])
    if package == "weaver-unrar":
        return UNRAR_SLOW_FILTERS.get(target, [])
    return []


def is_whole_slow_target(entry: dict) -> bool:
    package = entry["package"]
    target = entry["target"]
    return (package == "weaver-par2" and target in PAR2_SLOW_TARGETS) or (
        package == "weaver-unrar" and target in UNRAR_SLOW_TARGETS
    )


def commands_for_lane(entry: dict, lane: str) -> list[list[str]]:
    package = entry["package"]
    target = entry["target"]
    if lane == "regular":
        if is_whole_slow_target(entry):
            return []
        args = []
        for test_name in slow_filters_for(entry):
            args.extend(["--skip", test_name])
        return [args]

    if lane == "par2-slow":
        if package != "weaver-par2":
            return []
        if target in PAR2_SLOW_TARGETS:
            return [[]]
        return [[test_name, "--exact"] for test_name in PAR2_SLOW_FILTERS.get(target, [])]

    if lane == "unrar-slow":
        if package != "weaver-unrar":
            return []
        if target in UNRAR_SLOW_TARGETS:
            return [[]]
        return [[test_name, "--exact"] for test_name in UNRAR_SLOW_FILTERS.get(target, [])]

    raise SystemExit(f"unknown lane: {lane}")


def run_lane(args: argparse.Namespace) -> None:
    workspace = Path(args.workspace).resolve()
    manifest_path = Path(args.manifest).resolve()
    artifact_root = manifest_path.parent
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    failures = 0
    ran = 0
    for entry in manifest["tests"]:
        artifact = artifact_root / entry["artifact"]
        cwd = workspace / entry["package_root"]
        for test_args in commands_for_lane(entry, args.lane):
            ran += 1
            label = f"{entry['package']}::{entry['target']}"
            if test_args:
                label = f"{label} {' '.join(test_args)}"
            print(f"::group::{label}", flush=True)
            result = subprocess.run([str(artifact), *test_args], cwd=cwd, check=False)
            print("::endgroup::", flush=True)
            if result.returncode != 0:
                failures += 1
                print(f"FAILED: {label} exited with {result.returncode}", file=sys.stderr)

    if ran == 0:
        raise SystemExit(f"lane {args.lane} did not select any test binaries")
    if failures:
        raise SystemExit(f"{failures} test command(s) failed in lane {args.lane}")
    print(f"lane {args.lane} completed {ran} test command(s)")


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--metadata", required=True)
    collect_parser.add_argument("--cargo-json", required=True)
    collect_parser.add_argument("--archive", required=True)
    collect_parser.add_argument("--workspace", default=os.getcwd())
    collect_parser.set_defaults(func=collect)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--manifest", required=True)
    run_parser.add_argument("--lane", required=True, choices=["regular", "par2-slow", "unrar-slow"])
    run_parser.add_argument("--workspace", default=os.getcwd())
    run_parser.set_defaults(func=run_lane)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
