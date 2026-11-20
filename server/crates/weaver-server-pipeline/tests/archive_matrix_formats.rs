// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[macro_use]
#[path = "archive_matrix/shards.rs"]
mod shards;
mod pipeline {
    mod tests {
        mod direct_store {
            mod scenarios {
                mod archive_schedules {
                    mod extended {
                        mod tier2 {
                            mod breadth {
                                mod lies {
                                    mod combined_lies {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::lies::combined_lies::run_shard;
                                        tier2_shards!(run_shard, 300; h0 0 h1 1 h2 2);
                                    }
                                }
                                mod nesting {
                                    mod combined_nesting {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::nesting::combined_nesting::run_shard;
                                        tier2_shards!(run_shard, 300; h0 0 h1 1 h2 2);
                                    }
                                }
                                mod scale {
                                    mod combined_scale {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::scale::combined_scale::run_shard;
                                        tier2_shards!(run_shard, 300; h0 0 h1 1 h2 2);
                                    }
                                }
                                mod withheld {
                                    mod combined_withheld {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::withheld::combined_withheld::run_shard;
                                        tier2_shards!(run_shard, 100; h0 0);
                                    }
                                }
                                mod password {
                                    mod combined_password {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::password::combined_password::run_shard;
                                        tier2_shards!(run_shard, 200; h0 0 h1 1);
                                    }
                                }
                                mod wire {
                                    mod combined_wire {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::wire::combined_wire::run_shard;
                                        tier2_shards!(run_shard, 200; h0 0 h1 1);
                                    }
                                }
                                mod naming {
                                    mod combined_naming {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::naming::combined_naming::run_shard;
                                        tier2_shards!(run_shard, 200; h0 0 h1 1);
                                    }
                                }
                                mod sets {
                                    mod combined_sets {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::breadth::sets::combined_sets::run_shard;
                                        tier2_shards!(run_shard, 200; h0 0 h1 1);
                                    }
                                }
                            }
                            mod headers {
                                mod field {
                                    mod field_campaign {
                                        use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::headers::field::field_campaign::run_shard;
                                        tier2_shards!(run_shard, 100; h0 0);
                                    }
                                }
                                mod header_campaign {
                                    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::headers::header_campaign::run_shard;
                                    tier2_shards!(run_shard, 100; h0 0);
                                }
                            }
                            mod par3_breadth {
                                mod combined_par3_breadth {
                                    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::par3_breadth::combined_par3_breadth::run_shard;
                                    tier2_shards!(run_shard, 500; h0 0 h1 1 h2 2 h3 3 h4 4);
                                }
                            }
                            mod uu {
                                mod combined_uu {
                                    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::uu::combined_uu::run_shard;
                                    tier2_shards!(run_shard, 500; h0 0 h1 1 h2 2 h3 3 h4 4);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
