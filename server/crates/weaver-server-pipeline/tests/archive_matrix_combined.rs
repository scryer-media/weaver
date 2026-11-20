// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[macro_use]
#[path = "archive_matrix/combined.rs"]
mod combined;

mod pipeline {
    mod tests {
        mod direct_store {
            mod scenarios {
                mod archive_schedules {
                    mod graph {
                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{Selection, CompressedFormat, Format};
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::graph::{graph_campaign, graph_compressed_campaign};

                        combined_campaign!(combined_rar4, Format::Rar4, graph_campaign);
                        combined_campaign!(combined_rar5, Format::Rar5, graph_campaign);
                        combined_campaign!(
                            combined_rar4_encrypted,
                            Format::Rar4Encrypted,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_unsalted,
                            Format::Rar4Unsalted,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_encrypted,
                            Format::Rar5Encrypted,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_keyed_checksum,
                            Format::Rar5KeyedChecksum,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_header_encrypted,
                            Format::Rar5EncryptedHeaders,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_unchecked_header,
                            Format::Rar5UncheckedHeaders,
                            graph_campaign
                        );
                        combined_campaign!(combined_quick_open, Format::QuickOpen, graph_campaign);
                        combined_campaign!(combined_blake2, Format::Blake2, graph_campaign);
                        combined_campaign!(
                            combined_rar4_four_volume,
                            Format::Rar4FourVolumes,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_four_volume,
                            Format::Rar5FourVolumes,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_encrypted_four_volume,
                            Format::Rar4EncryptedFourVolumes,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_obfuscated,
                            Format::Rar5Obfuscated,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_bare,
                            Format::Rar5HexBare,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_bare,
                            Format::Rar4HexBare,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_self_described,
                            Format::Rar5HexSelfDescribed,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_self_described,
                            Format::Rar4HexSelfDescribed,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_late_index,
                            Format::Rar5HexLateIndex,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_late_index,
                            Format::Rar4HexLateIndex,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_misnumbered,
                            Format::Rar5HexMisnumbered,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_misnumbered,
                            Format::Rar4HexMisnumbered,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_swapped_rar,
                            Format::Rar5HexSwappedRar,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_swapped_rar,
                            Format::Rar4HexSwappedRar,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_scattered,
                            Format::Rar5HexScattered,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_scattered,
                            Format::Rar4HexScattered,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar5_hex_single,
                            Format::Rar5HexSingle,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_rar4_hex_single,
                            Format::Rar4HexSingle,
                            graph_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_mixed,
                            CompressedFormat::Rar4Mixed,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_lz,
                            CompressedFormat::Rar4Lz,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_encrypted,
                            CompressedFormat::Rar4Encrypted,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_headers,
                            CompressedFormat::Rar4Headers,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_mixed,
                            CompressedFormat::Rar5Mixed,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_lz,
                            CompressedFormat::Rar5Lz,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_encrypted,
                            CompressedFormat::Rar5Encrypted,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_headers,
                            CompressedFormat::Rar5Headers,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_solid,
                            CompressedFormat::Rar5Solid,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_solid_encrypted,
                            CompressedFormat::Rar5SolidEncrypted,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_solid_headers,
                            CompressedFormat::Rar5SolidHeaders,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_solid,
                            CompressedFormat::Rar4Solid,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_solid_encrypted,
                            CompressedFormat::Rar4SolidEncrypted,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_solid_headers,
                            CompressedFormat::Rar4SolidHeaders,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_ppmd,
                            CompressedFormat::Rar4Ppmd,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_ppmd_encrypted,
                            CompressedFormat::Rar4PpmdEncrypted,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_ppmd_headers,
                            CompressedFormat::Rar4PpmdHeaders,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_lz_two_volume,
                            CompressedFormat::Rar4LzTwoVolumes,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_lz_two_volume,
                            CompressedFormat::Rar5LzTwoVolumes,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar4_lz_four_volume,
                            CompressedFormat::Rar4LzFourVolumes,
                            graph_compressed_campaign
                        );
                        combined_campaign!(
                            combined_compressed_rar5_lz_four_volume,
                            CompressedFormat::Rar5LzFourVolumes,
                            graph_compressed_campaign
                        );
                        mod two_sets {
                            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{Selection, HexTwoSets};
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::graph::two_sets::{TwoSets, two_set_campaign};

                            combined_campaign!(
                                combined_two_sets,
                                TwoSets::Stored,
                                two_set_campaign
                            );
                            combined_campaign!(
                                combined_rar5_hex_two_set,
                                TwoSets::Hex(HexTwoSets::Rar5),
                                two_set_campaign
                            );
                            combined_campaign!(
                                combined_rar4_hex_two_set,
                                TwoSets::Hex(HexTwoSets::Rar4),
                                two_set_campaign
                            );
                        }
                    }
                }
                mod sevenz_store {
                    mod schedules {
                        mod graph {
                            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{Selection};
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::sevenz_store::schedules::{Shape};
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::sevenz_store::schedules::graph::{campaign};

                            combined_campaign!(combined_copy, Shape::Copy, campaign);
                            combined_campaign!(
                                combined_copy_four_volume,
                                Shape::CopyFourVolumes,
                                campaign
                            );
                            combined_campaign!(combined_copy_single, Shape::CopySingle, campaign);
                            combined_campaign!(combined_multiple, Shape::Multiple, campaign);
                            combined_campaign!(combined_empty_entry, Shape::EmptyEntry, campaign);
                            combined_campaign!(combined_nested, Shape::Nested, campaign);
                            combined_campaign!(combined_lzma2, Shape::Lzma2Fallback, campaign);
                            combined_campaign!(
                                combined_encrypted_copy,
                                Shape::EncryptedCopy,
                                campaign
                            );
                            combined_campaign!(
                                combined_encrypted_header,
                                Shape::EncryptedHeaders,
                                campaign
                            );
                            combined_campaign!(
                                combined_encrypted_lzma2,
                                Shape::EncryptedLzma2,
                                campaign
                            );
                            combined_campaign!(combined_solid, Shape::Solid, campaign);
                            combined_campaign!(
                                combined_solid_encrypted,
                                Shape::SolidEncrypted,
                                campaign
                            );
                            combined_campaign!(
                                combined_solid_headers,
                                Shape::SolidHeaders,
                                campaign
                            );
                            combined_campaign!(
                                combined_copy_obfuscated,
                                Shape::CopyObfuscated,
                                campaign
                            );
                            combined_campaign!(
                                combined_copy_single_obfuscated,
                                Shape::CopySingleObfuscated,
                                campaign
                            );
                        }
                    }
                }
            }
        }
    }
}
