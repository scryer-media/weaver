// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2 as fixture;
sync_cases!(tier2_sizing,);
mod breadth {
    mod lies {
        mod combined_lies_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::lies::combined_lies_smoke as fixture;
            cases!(file_hash_exponent, slice_size_one_volume, truthful_uniform,);
        }
    }
    mod naming {
        mod combined_naming_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::naming::combined_naming_smoke as fixture;
            cases!(
                gap_repaired,
                mixed_case_sevenzip,
                narrow_padding_rar4,
                upper_extension_par2,
            );
        }
    }
    mod nesting {
        mod combined_nesting_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::nesting::combined_nesting_smoke as fixture;
            cases!(
                rar_inside_sevenzip,
                sevenzip_inside_loss,
                three_deep_locked,
                two_deep_four_volumes_par2,
            );
        }
    }
    mod password {
        mod combined_password_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::password::combined_password_smoke as fixture;
            cases!(
                missing_rar4_data,
                right_rar5_headers,
                wrong_on_plain_loss,
                wrong_sevenzip_data,
            );
        }
    }
    mod scale {
        mod combined_scale_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::scale::combined_scale_smoke as fixture;
            cases!(
                big_articles_loss,
                many_articles_par3,
                many_volumes_par2,
                tiny_unprotected,
            );
        }
    }
    mod sets {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::sets as fixture;
        cases!(
            crashed_sevenz_extracts_beside_healthy_neighbour,
            damaged_neighbour_leaves_healthy_set_delivered,
            lost_neighbour_leaves_healthy_set_delivered,
        );
        mod combined_sets_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::sets::combined_sets_smoke as fixture;
            cases!(
                furniture_par2,
                loose_media_and_sample,
                two_sets_sevenzip,
                two_sets_stale_sfv_loss,
            );
        }
    }
    mod wire {
        mod combined_wire_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::wire::combined_wire_smoke as fixture;
            cases!(
                long_lines_many,
                short_lines_single_part,
                single_part_loss_par2,
            );
        }
    }
    mod withheld {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::withheld as fixture;
        cases!(a_rebuilt_withheld_volume_reaches_its_member,);
    }
}
mod damage {
    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::damage as fixture;
    cases!(
        encrypted_par3_duplicate_restart_settles,
        encrypted_par3_readback_crosses_missing_member_boundary,
        encrypted_par3_restart_retains_the_first_volume,
        par3_crash_refetch_reassesses_changed_source,
        par3_restart_retains_the_decoded_prefix_as_repair_input,
        swapped_encrypted_headers_repaired_after_restart,
    );
    mod combined_damage_loss_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::damage::combined_damage_loss_smoke as fixture;
        cases!(crc_wrong_mid_volume_par2, truncated_straddle_par3,);
    }
    mod combined_damage_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::damage::combined_damage_smoke as fixture;
        cases!(
            crc_wrong_mid_volume_par3,
            crc_wrong_straddle_unprotected,
            duplicate_recovery_volume,
            no_checksum_index_absent,
            no_checksum_middle_head_par2,
            overlong_mid_volume_short,
            recomputed_mid_volume_par2,
            swapped_last_tail_par3,
            trailing_junk_middle_hundred,
            truncated_first_head_exact,
            truncated_index_exact,
        );
    }
}
mod headers {
    mod field {
        mod field_smoke {
            use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::headers::field::field_smoke as fixture;
            cases!(
                every_part_arrives,
                inner_parts_lost_fails,
                inner_parts_lost_repaired,
            );
        }
    }
    mod header_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::headers::header_smoke as fixture;
        cases!(
            end_below_begin_plain,
            every_header_small_size_rar5,
            overlap_repaired_sevenzip,
            single_part_large_size_loss_par2,
        );
    }
}
mod par2_realism {
    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::par2_realism as fixture;
    cases!(
        index_absent_restart_fetches_remaining_recovery,
        repaired_encrypted_restart_finishes_member_edges,
    );
    mod combined_par2_realism_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::par2_realism::combined_par2_realism_smoke as fixture;
        cases!(
            creator_pars_only,
            duplicate_desc_member,
            exponent_short,
            headers_encrypted_with,
            index_absent_most,
            index_damaged_exact,
            plain_two_with,
            split_packets_two,
            thousands_band_two,
            two_files_one_over,
            unicode_names_hundred,
        );
    }
}
mod par3_breadth {
    mod combined_par3_breadth_loss_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::par3_breadth::combined_par3_breadth_loss_smoke as fixture;
        cases!(sidecar_fft_rar5,);
    }
    mod combined_par3_breadth_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::par3_breadth::combined_par3_breadth_smoke as fixture;
        cases!(
            sidecar_cauchy_rar5,
            sidecar_fft_sevenz_two_entries,
            sidecar_fft_two_sets_present,
            sidecar_index_damaged_short,
            sidecar_two_sets_index_absent,
        );
    }
}
mod recovery {
    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::recovery as fixture;
    sync_cases!(
        distinct_par2_exponents_need_not_be_independent,
        missing_short_tail_can_be_supplied_by_an_intact_source,
        recovery_packet_counts_do_not_require_an_aligned_archive_tail,
        zero_row_par2_keeps_metadata_redundancy_without_repair_power,
    );
}
mod uu {
    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::uu as fixture;
    cases!(truncated_sevenz_part_reaches_par2_repair,);
    mod combined_uu_loss_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::uu::combined_uu_loss_smoke as fixture;
        cases!(bare_lf_set_unprotected, standard_set_par2,);
    }
    mod combined_uu_smoke {
        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::uu::combined_uu_smoke as fixture;
        cases!(
            bare_lf_duplicate,
            lenient_combo_par2,
            missing_end_bad_byte,
            missing_header_set_par2,
            no_mode_set,
            no_terminator_overlong,
            preamble_mixed,
            short_lines_single,
            space_zero_par2_as_uu,
            standard_single,
            unpadded_tail_sevenz,
        );
    }
}
