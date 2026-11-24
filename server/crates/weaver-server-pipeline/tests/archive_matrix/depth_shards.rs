// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Shards of a campaign named in decades, so a campaign's shard count is ten
// times the decades it lists. Each shard calls `$run` with the bracketed
// arguments and its selection, and is independently replayable.
macro_rules! depth_shards {
    ($run:path, $args:tt, $shards:expr; [$($decade:tt)+]) => {
        const _: () = assert!(10 * [$($decade),+].len() == $shards);
        $(depth_shards!(@decade $run, $args; $decade);)+
    };
    (@decade $run:path, $args:tt; 0) => {
        depth_shards!(@each $run, $args;
            shard_00 0, shard_01 1, shard_02 2, shard_03 3, shard_04 4,
            shard_05 5, shard_06 6, shard_07 7, shard_08 8, shard_09 9);
    };
    (@decade $run:path, $args:tt; 1) => {
        depth_shards!(@each $run, $args;
            shard_10 10, shard_11 11, shard_12 12, shard_13 13, shard_14 14,
            shard_15 15, shard_16 16, shard_17 17, shard_18 18, shard_19 19);
    };
    (@decade $run:path, $args:tt; 2) => {
        depth_shards!(@each $run, $args;
            shard_20 20, shard_21 21, shard_22 22, shard_23 23, shard_24 24,
            shard_25 25, shard_26 26, shard_27 27, shard_28 28, shard_29 29);
    };
    (@decade $run:path, $args:tt; 3) => {
        depth_shards!(@each $run, $args;
            shard_30 30, shard_31 31, shard_32 32, shard_33 33, shard_34 34,
            shard_35 35, shard_36 36, shard_37 37, shard_38 38, shard_39 39);
    };
    (@decade $run:path, $args:tt; 4) => {
        depth_shards!(@each $run, $args;
            shard_40 40, shard_41 41, shard_42 42, shard_43 43, shard_44 44,
            shard_45 45, shard_46 46, shard_47 47, shard_48 48, shard_49 49);
    };
    (@decade $run:path, $args:tt; 5) => {
        depth_shards!(@each $run, $args;
            shard_50 50, shard_51 51, shard_52 52, shard_53 53, shard_54 54,
            shard_55 55, shard_56 56, shard_57 57, shard_58 58, shard_59 59);
    };
    (@decade $run:path, $args:tt; 6) => {
        depth_shards!(@each $run, $args;
            shard_60 60, shard_61 61, shard_62 62, shard_63 63, shard_64 64,
            shard_65 65, shard_66 66, shard_67 67, shard_68 68, shard_69 69);
    };
    (@decade $run:path, $args:tt; 7) => {
        depth_shards!(@each $run, $args;
            shard_70 70, shard_71 71, shard_72 72, shard_73 73, shard_74 74,
            shard_75 75, shard_76 76, shard_77 77, shard_78 78, shard_79 79);
    };
    (@decade $run:path, $args:tt; 8) => {
        depth_shards!(@each $run, $args;
            shard_80 80, shard_81 81, shard_82 82, shard_83 83, shard_84 84,
            shard_85 85, shard_86 86, shard_87 87, shard_88 88, shard_89 89);
    };
    (@decade $run:path, $args:tt; 9) => {
        depth_shards!(@each $run, $args;
            shard_90 90, shard_91 91, shard_92 92, shard_93 93, shard_94 94,
            shard_95 95, shard_96 96, shard_97 97, shard_98 98, shard_99 99);
    };
    (@decade $run:path, $args:tt; 10) => {
        depth_shards!(@each $run, $args;
            shard_100 100, shard_101 101, shard_102 102, shard_103 103, shard_104 104,
            shard_105 105, shard_106 106, shard_107 107, shard_108 108, shard_109 109);
    };
    (@decade $run:path, $args:tt; 11) => {
        depth_shards!(@each $run, $args;
            shard_110 110, shard_111 111, shard_112 112, shard_113 113, shard_114 114,
            shard_115 115, shard_116 116, shard_117 117, shard_118 118, shard_119 119);
    };
    (@decade $run:path, $args:tt; 12) => {
        depth_shards!(@each $run, $args;
            shard_120 120, shard_121 121, shard_122 122, shard_123 123, shard_124 124,
            shard_125 125, shard_126 126, shard_127 127, shard_128 128, shard_129 129);
    };
    (@decade $run:path, $args:tt; 13) => {
        depth_shards!(@each $run, $args;
            shard_130 130, shard_131 131, shard_132 132, shard_133 133, shard_134 134,
            shard_135 135, shard_136 136, shard_137 137, shard_138 138, shard_139 139);
    };
    (@decade $run:path, $args:tt; 14) => {
        depth_shards!(@each $run, $args;
            shard_140 140, shard_141 141, shard_142 142, shard_143 143, shard_144 144,
            shard_145 145, shard_146 146, shard_147 147, shard_148 148, shard_149 149);
    };
    (@decade $run:path, $args:tt; 15) => {
        depth_shards!(@each $run, $args;
            shard_150 150, shard_151 151, shard_152 152, shard_153 153, shard_154 154,
            shard_155 155, shard_156 156, shard_157 157, shard_158 158, shard_159 159);
    };
    (@each $run:path, $args:tt; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                depth_shards!(@call $run, $args, Selection::Shard($shard));
            }
        )+
    };
    (@call $run:path, [$($arg:expr),*], $selection:expr) => {
        $run($($arg,)* $selection).await
    };
}
