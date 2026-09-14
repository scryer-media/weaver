//! Which compute kernel each hot-path library dispatches to on this host.
//!
//! Every library resolves its tiers once, from CPU feature probes and a few
//! escape-hatch environment variables. Only some of those gates are public. A
//! public gate is called directly; a private one is mirrored here with the same
//! probes and the same variables, so the report reads what the dispatcher
//! reads. Nothing in the pipeline consults this — it feeds System info and bug
//! reports.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelComponent {
    YencDecode,
    YencCrc32,
    Par2Repair,
    Par2Md5,
    Par2Crc32,
    RarRecovery,
    RarCrc32,
    RarSha1,
    RarAes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KernelSelection {
    pub component: KernelComponent,
    /// The crate that owns the dispatch.
    pub library: &'static str,
    /// Every kernel this build can select on this architecture, in the order
    /// the dispatcher tries them.
    pub ladder: Vec<&'static str>,
    /// The rung the dispatcher selected; always an entry of `ladder`.
    pub kernel: &'static str,
    /// The environment variable that moved the selection off the rung the CPU
    /// alone would have picked, when one did.
    pub pinned_by: Option<&'static str>,
}

/// One row per dispatch site, in pipeline order.
pub fn selected_kernels() -> Vec<KernelSelection> {
    vec![
        selection(
            KernelComponent::YencDecode,
            "weaver-yenc",
            yenc_decode_ladder(),
            yenc_decode_kernel,
            &[],
        ),
        selection(
            KernelComponent::YencCrc32,
            "weaver-yenc",
            crc_ladder(),
            yenc_crc_kernel,
            &[],
        ),
        selection(
            KernelComponent::Par2Repair,
            "par2-rs",
            gf16_repair_ladder(),
            gf16_repair_kernel,
            &[
                "WEAVER_GF16_ALTMAP_SSE",
                "WEAVER_GF16_FOLDED_AVX512",
                "WEAVER_GF16_SHUFFLE2X_AVX512",
                "WEAVER_GF16_CLMUL_BATCH",
            ],
        ),
        selection(
            KernelComponent::Par2Md5,
            "par2-rs",
            md5_ladder(),
            md5_kernel,
            &[],
        ),
        selection(
            KernelComponent::Par2Crc32,
            "par2-rs",
            crc_ladder(),
            rarpar_crc_kernel,
            &[RARPAR_CRC_ENV],
        ),
        selection(
            KernelComponent::RarRecovery,
            "reedsolomon-rs",
            gf16_region_ladder(),
            gf16_region_kernel,
            &[],
        ),
        selection(
            KernelComponent::RarCrc32,
            "unrar-rs",
            crc_ladder(),
            rarpar_crc_kernel,
            &[RARPAR_CRC_ENV],
        ),
        selection(
            KernelComponent::RarSha1,
            "unrar-rs",
            sha1_ladder(),
            sha1_kernel,
            &[SHA1_HW_ENV, SHA1_X86_ENV],
        ),
        selection(
            KernelComponent::RarAes,
            "aws-lc",
            aes_ladder(),
            aes_kernel,
            &[],
        ),
    ]
}

/// `select(true)` is the live selection; `select(false)` is the same ladder
/// with every environment variable ignored, which is how a pin is recognised.
fn selection(
    component: KernelComponent,
    library: &'static str,
    ladder: Vec<&'static str>,
    select: fn(bool) -> &'static str,
    env: &[&'static str],
) -> KernelSelection {
    let kernel = select(true);
    let pinned_by = if kernel == select(false) {
        None
    } else {
        env.iter()
            .copied()
            .find(|name| std::env::var_os(name).is_some())
    };
    debug_assert!(ladder.contains(&kernel), "{kernel} is not on its ladder");
    KernelSelection {
        component,
        library,
        ladder,
        kernel,
        pinned_by,
    }
}

fn env_is(read_env: bool, name: &str, value: &str) -> bool {
    read_env && std::env::var_os(name).is_some_and(|set| set == value)
}

const SCALAR: &str = "Scalar";

// --- yEnc decode ---------------------------------------------------------

fn yenc_decode_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec![
            "AVX-512 VBMI2",
            "AVX2",
            "AVX",
            "SSE4.1",
            "SSSE3",
            "SSE2",
            SCALAR,
        ]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec!["NEON"]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec![SCALAR]
    }
}

fn yenc_decode_kernel(_read_env: bool) -> &'static str {
    use weaver_yenc::simd::SelectedDecoderTier;
    match weaver_yenc::simd::selected_decoder_tier() {
        SelectedDecoderTier::Avx512Vbmi2 => "AVX-512 VBMI2",
        SelectedDecoderTier::Avx2 => "AVX2",
        SelectedDecoderTier::Avx => "AVX",
        SelectedDecoderTier::Sse41 => "SSE4.1",
        SelectedDecoderTier::Ssse3 => "SSSE3",
        SelectedDecoderTier::Sse2 => "SSE2",
        SelectedDecoderTier::Neon => "NEON",
        SelectedDecoderTier::Scalar => SCALAR,
    }
}

// --- CRC32 ---------------------------------------------------------------

const CRC_VPCLMUL_512: &str = "VPCLMULQDQ AVX-512";
const CRC_VPCLMUL_256: &str = "VPCLMULQDQ AVX2";
const CRC_PCLMUL_512: &str = "PCLMULQDQ AVX-512";
const CRC_PCLMUL_128: &str = "PCLMULQDQ SSE4.1";
const CRC_PMULL_SHA3: &str = "PMULL + SHA3";
const CRC_PMULL: &str = "PMULL";
const CRC_TABLES: &str = "Lookup tables";

/// `RARPAR_CRC32_VPCLMUL`: `0` stands the 256-bit fold down, `1` engages it
/// alongside AVX-512VL too. It never enables the fold on a CPU without it.
const RARPAR_CRC_ENV: &str = "RARPAR_CRC32_VPCLMUL";

fn crc_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec![
            CRC_VPCLMUL_512,
            CRC_VPCLMUL_256,
            CRC_PCLMUL_512,
            CRC_PCLMUL_128,
            CRC_TABLES,
        ]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec![CRC_PMULL_SHA3, CRC_PMULL, CRC_TABLES]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec![CRC_TABLES]
    }
}

/// The tier `crc-fast` runs for everything the 256-bit fold does not take.
fn crc_fast_kernel() -> &'static str {
    match crc_fast::get_calculator_target(crc_fast::CrcAlgorithm::Crc32IsoHdlc).as_str() {
        "x86_64-avx512-vpclmulqdq" => CRC_VPCLMUL_512,
        "x86_64-avx512-pclmulqdq" => CRC_PCLMUL_512,
        "x86_64-sse-pclmulqdq" => CRC_PCLMUL_128,
        "aarch64-neon-pmull-sha3" => CRC_PMULL_SHA3,
        "aarch64-neon-pmull" => CRC_PMULL,
        _ => CRC_TABLES,
    }
}

fn yenc_crc_kernel(_read_env: bool) -> &'static str {
    if weaver_yenc::crc::wide_fold_selected() {
        CRC_VPCLMUL_256
    } else {
        crc_fast_kernel()
    }
}

/// par2-rs and unrar-rs carry the same fold behind the same gate.
fn rarpar_crc_kernel(read_env: bool) -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        let capable = is_x86_feature_detected!("avx2")
            && is_x86_feature_detected!("pclmulqdq")
            && is_x86_feature_detected!("sse4.1")
            && is_x86_feature_detected!("vpclmulqdq");
        let fold = if env_is(read_env, RARPAR_CRC_ENV, "0") || !capable {
            false
        } else {
            env_is(read_env, RARPAR_CRC_ENV, "1") || !is_x86_feature_detected!("avx512vl")
        };
        if fold {
            return CRC_VPCLMUL_256;
        }
    }
    let _ = read_env;
    crc_fast_kernel()
}

// --- GF(2^16) --------------------------------------------------------------

#[cfg(target_arch = "x86_64")]
const GF_XOR_JIT: &str = "XOR-JIT AVX2";
#[cfg(target_arch = "x86_64")]
const GF_FOLDED_GFNI_512: &str = "Folded GFNI AVX-512";
#[cfg(target_arch = "x86_64")]
const GF_FOLDED_GFNI_256: &str = "Folded GFNI AVX2";
#[cfg(target_arch = "x86_64")]
const GF_FOLDED_SHUFFLE_512: &str = "Folded shuffle AVX-512";
#[cfg(target_arch = "x86_64")]
const GF_FOLDED_SHUFFLE_256: &str = "Folded shuffle AVX2";
#[cfg(target_arch = "x86_64")]
const GF_FOLDED_SHUFFLE_128: &str = "Folded shuffle SSSE3";
#[cfg(target_arch = "x86_64")]
const GF_GFNI_512: &str = "GFNI AVX-512";
#[cfg(target_arch = "x86_64")]
const GF_GFNI_256: &str = "GFNI AVX2";
#[cfg(target_arch = "x86_64")]
const GF_SHUFFLE_512: &str = "Shuffle AVX-512";
#[cfg(target_arch = "x86_64")]
const GF_SHUFFLE_256: &str = "Shuffle AVX2";
#[cfg(target_arch = "x86_64")]
const GF_SHUFFLE_128: &str = "Shuffle SSSE3";
#[cfg(target_arch = "aarch64")]
const GF_PMULL_SHA3: &str = "PMULL + SHA3";
#[cfg(target_arch = "aarch64")]
const GF_PMULL: &str = "PMULL";
#[cfg(target_arch = "aarch64")]
const GF_NEON_SHUFFLE: &str = "NEON shuffle";

fn gf16_repair_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec![
            GF_XOR_JIT,
            GF_FOLDED_GFNI_512,
            GF_FOLDED_GFNI_256,
            GF_FOLDED_SHUFFLE_512,
            GF_FOLDED_SHUFFLE_256,
            GF_FOLDED_SHUFFLE_128,
            SCALAR,
        ]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec![GF_PMULL_SHA3, GF_PMULL, GF_NEON_SHUFFLE]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec![SCALAR]
    }
}

/// PAR2 repair: the XOR-JIT on the CPU families it is tuned for, otherwise the
/// folded controller over the split byte-plane layout, otherwise plain batches.
#[cfg(target_arch = "x86_64")]
fn gf16_repair_kernel(read_env: bool) -> &'static str {
    if reedsolomon_rs::xor_jit::JitWidth::detect().is_some() {
        return GF_XOR_JIT;
    }
    if !par2_rs::gf_simd::altmap_supported() {
        return SCALAR;
    }
    // `altmap_uses_avx2`, with its `WEAVER_GF16_ALTMAP_SSE=1` pin.
    let wide = is_x86_feature_detected!("avx2")
        && !(env_is(read_env, "WEAVER_GF16_ALTMAP_SSE", "1") && is_x86_feature_detected!("ssse3"));
    if !wide {
        return GF_FOLDED_SHUFFLE_128;
    }
    let avx512 = is_x86_feature_detected!("avx512bw") && is_x86_feature_detected!("avx512vl");
    if is_x86_feature_detected!("gfni") {
        // `folded_avx512_enabled`.
        return if avx512 && !env_is(read_env, "WEAVER_GF16_FOLDED_AVX512", "0") {
            GF_FOLDED_GFNI_512
        } else {
            GF_FOLDED_GFNI_256
        };
    }
    // `shuffle2x_avx512_enabled`.
    if avx512
        && is_x86_feature_detected!("avx512f")
        && !env_is(read_env, "WEAVER_GF16_SHUFFLE2X_AVX512", "0")
    {
        GF_FOLDED_SHUFFLE_512
    } else {
        GF_FOLDED_SHUFFLE_256
    }
}

/// aarch64 repair batches multiply through PMULL once a group has more than
/// three inputs, unless `WEAVER_GF16_CLMUL_BATCH=0` keeps the NEON shuffle.
#[cfg(target_arch = "aarch64")]
fn gf16_repair_kernel(read_env: bool) -> &'static str {
    if env_is(read_env, "WEAVER_GF16_CLMUL_BATCH", "0") {
        GF_NEON_SHUFFLE
    } else if std::arch::is_aarch64_feature_detected!("sha3") {
        GF_PMULL_SHA3
    } else {
        GF_PMULL
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn gf16_repair_kernel(_read_env: bool) -> &'static str {
    SCALAR
}

fn gf16_region_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec![
            GF_GFNI_512,
            GF_GFNI_256,
            GF_SHUFFLE_512,
            GF_SHUFFLE_256,
            GF_SHUFFLE_128,
            SCALAR,
        ]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec![GF_NEON_SHUFFLE]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec![SCALAR]
    }
}

/// RAR5 recovery volumes accumulate one region at a time (`mul_acc_region`).
fn gf16_region_kernel(_read_env: bool) -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        let avx512 = is_x86_feature_detected!("avx512bw") && is_x86_feature_detected!("avx512vl");
        let gfni = is_x86_feature_detected!("gfni");
        if gfni && avx512 {
            GF_GFNI_512
        } else if gfni && is_x86_feature_detected!("avx2") {
            GF_GFNI_256
        } else if avx512 {
            GF_SHUFFLE_512
        } else if is_x86_feature_detected!("avx2") {
            GF_SHUFFLE_256
        } else if is_x86_feature_detected!("ssse3") {
            GF_SHUFFLE_128
        } else {
            SCALAR
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        GF_NEON_SHUFFLE
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        SCALAR
    }
}

// --- MD5 -----------------------------------------------------------------

fn md5_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec!["AVX2 × 8 lanes", "SSE2 × 4 lanes"]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec!["NEON × 4 lanes"]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec![SCALAR]
    }
}

fn md5_kernel(_read_env: bool) -> &'static str {
    match par2_rs::md5_simd::max_lanes() {
        8 => "AVX2 × 8 lanes",
        #[cfg(target_arch = "x86_64")]
        4 => "SSE2 × 4 lanes",
        #[cfg(target_arch = "aarch64")]
        4 => "NEON × 4 lanes",
        _ => SCALAR,
    }
}

// --- SHA-1 (RAR3 key derivation) -------------------------------------------

const SHA1_HW_ENV: &str = "UNRAR_RS_SHA1_HW";
const SHA1_X86_ENV: &str = "UNRAR_RS_SHA1_X86";

fn sha1_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec!["SHA-NI", "SSSE3", "AVX2", SCALAR]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec!["ARMv8 SHA", SCALAR]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec![SCALAR]
    }
}

/// SHA-NI first; without it the measured-faster SSSE3 kernel ahead of AVX2.
/// `UNRAR_RS_SHA1_X86` names a vector tier (or `0`) and stands SHA-NI down.
#[cfg(target_arch = "x86_64")]
fn sha1_kernel(read_env: bool) -> &'static str {
    let hw_off = env_is(read_env, SHA1_HW_ENV, "0");
    let forced = if read_env {
        std::env::var(SHA1_X86_ENV).ok()
    } else {
        None
    };
    let ssse3 = is_x86_feature_detected!("ssse3");
    let avx2 = ssse3
        && is_x86_feature_detected!("avx2")
        && is_x86_feature_detected!("bmi1")
        && is_x86_feature_detected!("bmi2");
    let sha_ni = is_x86_feature_detected!("sha") && is_x86_feature_detected!("sse4.1") && ssse3;
    let vector_forced = matches!(forced.as_deref(), Some("ssse3" | "avx2"));
    if !hw_off && !vector_forced && sha_ni {
        return "SHA-NI";
    }
    if hw_off || forced.as_deref() == Some("0") {
        return SCALAR;
    }
    match forced.as_deref() {
        Some("ssse3") => return if ssse3 { "SSSE3" } else { SCALAR },
        Some("avx2") => return if avx2 { "AVX2" } else { SCALAR },
        _ => {}
    }
    if ssse3 {
        "SSSE3"
    } else if avx2 {
        "AVX2"
    } else {
        SCALAR
    }
}

#[cfg(target_arch = "aarch64")]
fn sha1_kernel(read_env: bool) -> &'static str {
    if !env_is(read_env, SHA1_HW_ENV, "0") && std::arch::is_aarch64_feature_detected!("sha2") {
        "ARMv8 SHA"
    } else {
        SCALAR
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn sha1_kernel(_read_env: bool) -> &'static str {
    SCALAR
}

// --- AES (RAR decryption) ------------------------------------------------

fn aes_ladder() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        vec!["AES-NI", "VPAES SSSE3", "Constant-time software"]
    }
    #[cfg(target_arch = "aarch64")]
    {
        vec!["ARMv8 AES", "VPAES NEON"]
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        vec!["Constant-time software"]
    }
}

/// AWS-LC's own CPU capability dispatch: the hardware AES instructions, then
/// the vector-permutation AES, then its constant-time software AES.
fn aes_kernel(_read_env: bool) -> &'static str {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("aes") {
            "AES-NI"
        } else if is_x86_feature_detected!("ssse3") {
            "VPAES SSSE3"
        } else {
            "Constant-time software"
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("aes") {
            "ARMv8 AES"
        } else {
            "VPAES NEON"
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        "Constant-time software"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_selection_sits_on_its_ladder() {
        let kernels = selected_kernels();
        assert_eq!(kernels.len(), 9);
        for kernel in &kernels {
            assert!(
                kernel.ladder.contains(&kernel.kernel),
                "{:?} selected {} off its ladder {:?}",
                kernel.component,
                kernel.kernel,
                kernel.ladder
            );
        }
    }

    #[test]
    fn yenc_decode_row_matches_the_dispatcher() {
        let row = selection(
            KernelComponent::YencDecode,
            "weaver-yenc",
            yenc_decode_ladder(),
            yenc_decode_kernel,
            &[],
        );
        assert_eq!(
            row.kernel.to_ascii_lowercase().replace([' ', '-'], ""),
            weaver_yenc::simd::selected_decoder_tier()
                .as_str()
                .replace('-', "")
        );
    }
}
