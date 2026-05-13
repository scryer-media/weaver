#[path = "support/par2cmdline_turbo_support.rs"]
mod support;

use support::{UPSTREAM_FUNCTIONAL_CASE_COUNT, UPSTREAM_UNIT_CASE_COUNT};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportStatus {
    Runnable,
    Unsupported,
}

#[derive(Debug, Clone, Copy)]
struct ImportedCase {
    id: &'static str,
    source: &'static str,
    status: ImportStatus,
    note: &'static str,
}

const FUNCTIONAL_CASES: &[ImportedCase] = &[
    ImportedCase {
        id: "test1",
        source: "tests/test1",
        status: ImportStatus::Unsupported,
        note: "Weaver currently implements PAR2 only, not PAR1 verify flows.",
    },
    ImportedCase {
        id: "test2",
        source: "tests/test2",
        status: ImportStatus::Runnable,
        note: "Healthy PAR2 verification against upstream flatdata fixtures.",
    },
    ImportedCase {
        id: "test3",
        source: "tests/test3",
        status: ImportStatus::Unsupported,
        note: "Weaver currently implements PAR2 only, not PAR1 repair flows.",
    },
    ImportedCase {
        id: "test4",
        source: "tests/test4",
        status: ImportStatus::Runnable,
        note: "Repair missing PAR2 files from upstream flatdata fixtures.",
    },
    ImportedCase {
        id: "test5",
        source: "tests/test5",
        status: ImportStatus::Unsupported,
        note: "Requires PAR2 creation coverage; Weaver has no create path yet.",
    },
    ImportedCase {
        id: "test5rk",
        source: "tests/test5rk",
        status: ImportStatus::Unsupported,
        note: "Requires PAR2 creation coverage and legacy -rk/-n CLI semantics.",
    },
    ImportedCase {
        id: "test6",
        source: "tests/test6",
        status: ImportStatus::Runnable,
        note: "Repair upstream subdirectory data with Unix-style PAR2 paths.",
    },
    ImportedCase {
        id: "test7",
        source: "tests/test7",
        status: ImportStatus::Runnable,
        note: "Repair upstream subdirectory data with Windows-style PAR2 paths.",
    },
    ImportedCase {
        id: "test8",
        source: "tests/test8",
        status: ImportStatus::Unsupported,
        note: "Requires recursive PAR2 creation before the moved-root verify step.",
    },
    ImportedCase {
        id: "test9",
        source: "tests/test9",
        status: ImportStatus::Runnable,
        note: "Repair after data files were renamed in place.",
    },
    ImportedCase {
        id: "test10",
        source: "tests/test10",
        status: ImportStatus::Runnable,
        note: "Repair a deleted subdirectory from existing PAR2 fixtures.",
    },
    ImportedCase {
        id: "test11",
        source: "tests/test11",
        status: ImportStatus::Unsupported,
        note: "Requires PAR2 creation and 100-block generation coverage.",
    },
    ImportedCase {
        id: "test12",
        source: "tests/test12",
        status: ImportStatus::Runnable,
        note: "Repair truncated file size against existing upstream fixture.",
    },
    ImportedCase {
        id: "test13",
        source: "tests/test13",
        status: ImportStatus::Runnable,
        note: "Repair file truncated at EOF against vendored PAR2 fixture.",
    },
    ImportedCase {
        id: "test14",
        source: "tests/test14",
        status: ImportStatus::Runnable,
        note: "Repair file truncated at BOF against vendored PAR2 fixture.",
    },
    ImportedCase {
        id: "test15",
        source: "tests/test15",
        status: ImportStatus::Runnable,
        note: "Existing upstream crash-regression repair fixture.",
    },
    ImportedCase {
        id: "test16",
        source: "tests/test16",
        status: ImportStatus::Unsupported,
        note: "Create-path base-dir filtering and warning semantics are not present.",
    },
    ImportedCase {
        id: "test17",
        source: "tests/test17",
        status: ImportStatus::Runnable,
        note: "Repairs the upstream bug44 removed-subdirectory fixture.",
    },
    ImportedCase {
        id: "test18",
        source: "tests/test18",
        status: ImportStatus::Runnable,
        note: "Repair a truncated single-file archive against vendored PAR2 fixture.",
    },
    ImportedCase {
        id: "test19",
        source: "tests/test19",
        status: ImportStatus::Unsupported,
        note: "Requires legacy skip-data/skip-leaway repair options.",
    },
    ImportedCase {
        id: "test20",
        source: "tests/test20",
        status: ImportStatus::Runnable,
        note: "Repair a missing primary file by scanning split fragment inputs.",
    },
    ImportedCase {
        id: "test21",
        source: "tests/test21",
        status: ImportStatus::Unsupported,
        note: "Requires PAR2 creation with external base-dir layout.",
    },
    ImportedCase {
        id: "test22",
        source: "tests/test22",
        status: ImportStatus::Unsupported,
        note: "Requires PAR2 creation from a different process working directory.",
    },
    ImportedCase {
        id: "test23",
        source: "tests/test23",
        status: ImportStatus::Runnable,
        note: "Verify from an arbitrary caller directory against existing fixtures.",
    },
    ImportedCase {
        id: "test24",
        source: "tests/test24",
        status: ImportStatus::Runnable,
        note: "Repair from an arbitrary caller directory against existing fixtures.",
    },
    ImportedCase {
        id: "test25",
        source: "tests/test25",
        status: ImportStatus::Unsupported,
        note: "Requires create support for full-path primary input behavior.",
    },
    ImportedCase {
        id: "test26",
        source: "tests/test26",
        status: ImportStatus::Unsupported,
        note: "Requires create support for subdirectory full-path behavior.",
    },
    ImportedCase {
        id: "test27",
        source: "tests/test27",
        status: ImportStatus::Unsupported,
        note: "Requires create support for symlinked working-path behavior.",
    },
    ImportedCase {
        id: "test28",
        source: "tests/test28",
        status: ImportStatus::Unsupported,
        note: "Requires legacy -qq CLI noise-level behavior.",
    },
    ImportedCase {
        id: "test29",
        source: "tests/test29",
        status: ImportStatus::Runnable,
        note: "Repair the upstream issue-190 single-bit-flip regression fixture.",
    },
    ImportedCase {
        id: "test30",
        source: "tests/test30",
        status: ImportStatus::Runnable,
        note: "Existing upstream zero-byte extra file regression fixture.",
    },
    ImportedCase {
        id: "test31",
        source: "tests/test31",
        status: ImportStatus::Unsupported,
        note: "Requires recursive create-path behavior with mixed root and subdir files.",
    },
    ImportedCase {
        id: "test32",
        source: "tests/test32",
        status: ImportStatus::Unsupported,
        note: "Requires create-path output placement and -B semantics.",
    },
    ImportedCase {
        id: "test33",
        source: "tests/test33",
        status: ImportStatus::Runnable,
        note: "Repair renamed files using current Weaver scan/rename behavior.",
    },
    ImportedCase {
        id: "test34",
        source: "tests/test34",
        status: ImportStatus::Runnable,
        note: "Repair with a damaged renamed candidate present.",
    },
    ImportedCase {
        id: "test35",
        source: "tests/test35",
        status: ImportStatus::Unsupported,
        note: "Requires create-path symbolic-link semantics and dead-link handling.",
    },
    ImportedCase {
        id: "testMem",
        source: "tests/testMem",
        status: ImportStatus::Unsupported,
        note: "Valgrind-only memory check is out of scope for cargo test.",
    },
];

const UNIT_CASES: &[ImportedCase] = &[
    ImportedCase {
        id: "commandline_test.cpp",
        source: "src/commandline_test.cpp",
        status: ImportStatus::Unsupported,
        note: "Weaver does not expose the legacy par2cmdline create/flag parser surface.",
    },
    ImportedCase {
        id: "crc_test.cpp",
        source: "src/crc_test.cpp",
        status: ImportStatus::Runnable,
        note: "Runs in the compatibility unit suite; sliding-window internals remain ignored.",
    },
    ImportedCase {
        id: "criticalpacket_test.cpp",
        source: "src/criticalpacket_test.cpp",
        status: ImportStatus::Unsupported,
        note: "No equivalent public packet-ordering comparator API exists in Weaver.",
    },
    ImportedCase {
        id: "descriptionpacket_test.cpp",
        source: "src/descriptionpacket_test.cpp",
        status: ImportStatus::Runnable,
        note: "Runs in the compatibility unit suite where Weaver exposes matching path translation.",
    },
    ImportedCase {
        id: "diskfile_test.cpp",
        source: "src/diskfile_test.cpp",
        status: ImportStatus::Runnable,
        note: "Runs in the compatibility unit suite where Weaver exposes matching disk access APIs.",
    },
    ImportedCase {
        id: "galois_test.cpp",
        source: "src/galois_test.cpp",
        status: ImportStatus::Runnable,
        note: "Runs in the compatibility unit suite against Weaver GF(2^16) arithmetic.",
    },
    ImportedCase {
        id: "letype_test.cpp",
        source: "src/letype_test.cpp",
        status: ImportStatus::Unsupported,
        note: "Weaver has no equivalent little-endian wrapper type API.",
    },
    ImportedCase {
        id: "libpar2_test.cpp",
        source: "src/libpar2_test.cpp",
        status: ImportStatus::Unsupported,
        note: "Exercises PAR2 creation file-count logic that Weaver does not implement.",
    },
    ImportedCase {
        id: "md5_test.cpp",
        source: "src/md5_test.cpp",
        status: ImportStatus::Runnable,
        note: "Runs in the compatibility unit suite against Weaver hashing surfaces.",
    },
    ImportedCase {
        id: "reedsolomon_test.cpp",
        source: "src/reedsolomon_test.cpp",
        status: ImportStatus::Runnable,
        note: "Runs in the compatibility unit suite through Weaver's verify, plan, and repair pipeline.",
    },
    ImportedCase {
        id: "utf8_test.cpp",
        source: "src/utf8_test.cpp",
        status: ImportStatus::Unsupported,
        note: "Rust string/UTF-8 handling removes the need for the upstream adapter layer.",
    },
];

#[test]
fn par2cmdline_turbo_inventory_accounts_for_upstream_cases() {
    assert_eq!(FUNCTIONAL_CASES.len(), UPSTREAM_FUNCTIONAL_CASE_COUNT);
    assert_eq!(UNIT_CASES.len(), UPSTREAM_UNIT_CASE_COUNT);

    for case in FUNCTIONAL_CASES.iter().chain(UNIT_CASES.iter()) {
        assert!(
            !case.id.is_empty() && !case.source.is_empty() && !case.note.is_empty(),
            "case inventory entry must be populated: {case:?}"
        );
    }
}

#[test]
fn par2cmdline_turbo_inventory_has_runnable_functional_subset() {
    let runnable = FUNCTIONAL_CASES
        .iter()
        .filter(|case| case.status == ImportStatus::Runnable)
        .count();
    assert!(
        runnable >= 10,
        "expected a meaningful runnable subset, found {runnable}"
    );
}
