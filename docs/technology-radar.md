# Technology Radar

Kernel and platform work tracked across weaver and the rarpar crates it consumes (PAR2, PAR3, RAR, yEnc). The rarpar README carries the engine-side radar.

Rules that govern every row: one binary with runtime dispatch; a kernel tier is never dropped because no local host has its instruction set; a tier is kept when it materially moves wall clock or CPU time where it engages without adding significant risk or code, and regresses nothing else by more than 1%; there is no minimum percentage; disk work (fsyncs, opens, read and write calls) is a regression axis on its own.

Status: **Landed** ships; **Building** has an owner now; **Exploring** is a measured spike before a decision; **Watch** waits on hardware or evidence; **Dropped** was measured and abandoned, kept on the radar so the attempt is not repeated.

| Item | Serves | Status | Needs |
|---|---|---|---|
| yEnc decode, 512-bit AVX-512 VBMI2 tier (`vpcompressb` compaction) | yEnc | Landed, validated under emulation | hardware A/B on Zen 4 and Sapphire Rapids |
| CRC32 fold width: 256-bit `vpclmulqdq` versus the current 128-bit fold; `crc-fast` as a candidate | yEnc, PAR2 verify, direct-store integrity | Exploring | decision from the measured gap against rapidyenc |
| GF(2^16) folded tier for AVX2 hosts without GFNI (Zen 2, pre-Ice-Lake) | PAR2 repair | Watch | measured gap on Zen 2 is 2.4x wall against par2cmdline-turbo |
| AVX512BMM GF(2^16) tier (Zen 6 `VBMACXOR16x16x16`) | PAR2, PAR3 | Watch | no Zen 6 instances on EC2 yet |
| SME2 GF(2) outer-product GEMM (`BMOPA`) for Reed-Solomon encode and solve | PAR2, PAR3 Cauchy | Watch | spike measured: slower than NEON at the 12–16 source groups the engines issue and at 8–18 workers (SME unit shared per cluster); wins 2–3x only at 64 or more sources per product |
| Wide-K engine restructure: stage 64 or more source stripes per matrix product instead of 16 | PAR2, PAR3 Cauchy, matrix-unit ISAs | Dropped | breakpoint spike measured on Apple silicon at 8 workers: repair of 1–10 lost blocks never crosses on realistic sets (N 256–51,200 blocks, 64 KiB stripe: SME2 saves 8–14 ms of compute per pass against at least 130 ms of streamed read, and holding K stripes resident forfeits NEON's read-ahead overlap, a net loss cold); PAR2 shows no crossover up to N=32,000 for 1–4 lost blocks or 5–10% recovery; full-recovery create is 1.2–1.5x on the kernel but per-pass plan rebuilds erase it and cached plans cost 32·R·N bytes (1.6 GB at N=32,000); the apparent 1.9x on one lost block was the PAR3 repair nest running row-parallel on one thread, fixed by a column split instead (see PAR3 repair row); revisit only if large-set create with page-cached sources and a 256 KiB or larger stripe becomes a hot path |
| PAR3 repair nest column split when fewer blocks are lost than there are workers | PAR3 | Dropped | measured on Sapphire Rapids and Apple silicon against the merged column-tiled FFT: at 8 workers it was 1.06–1.11x slower on wall and 1.26–1.64x on CPU for 1–4 lost blocks at a 64 KiB stripe, flat at 10 lost, and 1.02–1.16x CPU with +16 MiB at a 1 MiB stripe; repair is bounded by disk reads the existing nest already overlaps, so the idle threads the wide-K spike saw were waiting on I/O, not on the kernel; the patch is kept in the campaign handoff folder |
| SME streaming-width multi-buffer hashing (MD5, BLAKE3) | PAR2 verify, PAR3 planning | Watch | unproven; mode-switch cost |
| AVX10.2 and APX | every x86 kernel | Watch | Diamond Rapids and Nova Lake; EVEX kernels carry over, APX helps register-bound grouped kernels |
| mimalloc v3 allocator with purge tuning | resident memory | Landed | |
