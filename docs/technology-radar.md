# Technology Radar

Kernel and platform work for weaver's own crates: yEnc, CRC and the server. The PAR2, PAR3 and RAR engine radar lives in the rarpar README.

Rules that govern every row: one binary with runtime dispatch; a kernel tier is never dropped because no local host has its instruction set; a tier is kept when it materially moves wall clock or CPU time where it engages without adding significant risk or code, and regresses nothing else by more than 1%; there is no minimum percentage; disk work (fsyncs, opens, read and write calls) is a regression axis on its own.

Status: **Landed** ships; **Building** has an owner now; **Exploring** is a measured spike before a decision; **Watch** waits on hardware or evidence; **Dropped** was measured and abandoned, kept on the radar so the attempt is not repeated.

| Item | Serves | Status | Needs |
|---|---|---|---|
| yEnc decode, 512-bit AVX-512 VBMI2 tier (`vpcompressb` compaction) | yEnc | Landed, validated under emulation | hardware A/B on Zen 4 and Sapphire Rapids |
| CRC32 fold width: 256-bit `vpclmulqdq` versus the current 128-bit fold; `crc-fast` as a candidate | yEnc, PAR2 verify, direct-store integrity | Exploring | decision from the measured gap against rapidyenc |
| AVX10.2 and APX | weaver x86 kernels (yEnc) | Watch | Diamond Rapids and Nova Lake; EVEX kernels carry over |
| mimalloc v3 allocator with purge tuning | resident memory | Landed | |
