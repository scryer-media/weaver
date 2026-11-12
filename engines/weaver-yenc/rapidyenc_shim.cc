// Minimal extern "C" shim over RapidYenc::decode for same-process A/B timing.
// Compiled only when WEAVER_RAPIDYENC_SRC points at a rapidyenc checkout (see
// build.rs); never part of normal builds.
#include <cstddef>   // size_t — implicit under MSVC, required under g++/clang
#include <cstdint>
#include "src/decoder.h"
#include "src/crc.h"

extern "C" void weaver_rapidyenc_decode_init(void) {
    RapidYenc::decoder_init();
}

extern "C" unsigned long long weaver_rapidyenc_decode(const void* src, void* dest, unsigned long long len) {
    return (unsigned long long)RapidYenc::decode(1, src, dest, (size_t)len, nullptr);
}

// searchEnd (end-detecting) counterpart: rapidyenc's `_do_decode_end_raw`, i.e.
// the `isRaw=true, searchEnd=true` instantiation. Reports consumed/written the
// same way weaver's `decode_rapidyenc_incremental` does.
extern "C" int weaver_rapidyenc_decode_end(
    const void* src,
    void* dest,
    unsigned long long len,
    unsigned long long* consumed,
    unsigned long long* written
) {
    RapidYenc::YencDecoderState state = RapidYenc::YDEC_STATE_CRLF;
    const void* s = src;
    void* d = dest;
    RapidYenc::YencDecoderEnd end = RapidYenc::decode_end(&s, &d, (size_t)len, &state);
    *consumed = (unsigned long long)((const unsigned char*)s - (const unsigned char*)src);
    *written = (unsigned long long)((unsigned char*)d - (unsigned char*)dest);
    return (int)end;
}

// The same decoder carrying its state across calls, the way an incremental
// caller (sabctools) feeds a body in chunks. `state` is a YencDecoderState.
extern "C" int weaver_rapidyenc_decode_end_state(
    const void* src,
    void* dest,
    unsigned long long len,
    unsigned long long* consumed,
    unsigned long long* written,
    int* state
) {
    RapidYenc::YencDecoderState st = (RapidYenc::YencDecoderState)*state;
    const void* s = src;
    void* d = dest;
    RapidYenc::YencDecoderEnd end = RapidYenc::decode_end(&s, &d, (size_t)len, &st);
    *state = (int)st;
    *consumed = (unsigned long long)((const unsigned char*)s - (const unsigned char*)src);
    *written = (unsigned long long)((unsigned char*)d - (unsigned char*)dest);
    return (int)end;
}

// CRC32 counterpart, for the rapidyenc lanes of the article_profile example
// and the rapidyenc_parity bench. `crc32_init()` is a
// separate initializer from `decoder_init()` (it builds the slice table and then
// installs the PCLMUL/VPCLMUL/ARM function pointers), so it must be called
// before weaver_rapidyenc_crc32.
extern "C" void weaver_rapidyenc_crc32_init(void) {
    RapidYenc::crc32_init();
}

// `init` is the previous CRC in the finalized (post-xor) domain, matching
// weaver's Crc32 and crc_turbo; pass 0 for a fresh checksum.
extern "C" uint32_t weaver_rapidyenc_crc32(const void* data, unsigned long long len, uint32_t init) {
    return RapidYenc::crc32(data, (size_t)len, init);
}

// Which CRC kernel rapidyenc's own dispatch installed (YEncDecIsaLevel, see
// rapidyenc src/common.h), for rapidyenc-side attribution.
extern "C" int weaver_rapidyenc_crc32_isa(void) {
    return RapidYenc::crc32_isa_level();
}

// ---- decode kernel forcing, for decode-bench ------------------------------
// rapidyenc keeps its decode kernels in process-global function pointers;
// forcing one affects every later call. Single-threaded callers only.
#include "src/common.h"
#include "src/decoder_common.h"

// The active decode kernel, as a YEncDecIsaLevel (ISA_GENERIC is scalar).
extern "C" int weaver_rapidyenc_decode_kernel(void) {
    return RapidYenc::decode_isa_level();
}

static bool weaver_decode_kernel_supported(int isa) {
    if(isa == ISA_GENERIC) return true;
#ifdef PLATFORM_X86
    int have = RapidYenc::cpu_supports_isa();
    switch(isa) {
        case ISA_LEVEL_SSE2: return have >= ISA_LEVEL_SSE2;
        case ISA_LEVEL_SSSE3: return have >= ISA_LEVEL_SSSE3;
        case ISA_LEVEL_AVX: return have >= ISA_LEVEL_AVX;
        case ISA_LEVEL_AVX2: return have >= ISA_LEVEL_AVX2;
        case ISA_LEVEL_VBMI2: return have >= ISA_LEVEL_VBMI2;
        default: return false;
    }
#elif defined(PLATFORM_ARM)
    return isa == ISA_LEVEL_NEON && RapidYenc::cpu_supports_neon();
#else
    return false;
#endif
}

// rapidyenc's scalar kernels, read before decoder_init() replaces them: the
// pointers are constant-initialised, so they hold the scalar functions when
// this TU's dynamic initialisers run.
typedef RapidYenc::YencDecoderEnd (*weaver_decode_fn)(const unsigned char**, unsigned char**, size_t, RapidYenc::YencDecoderState*);
static const weaver_decode_fn weaver_generic_decode[3] = {
    RapidYenc::_do_decode, RapidYenc::_do_decode_raw, RapidYenc::_do_decode_end_raw
};

static void weaver_decode_init_kernel(int isa) {
    switch(isa) {
        case ISA_GENERIC:
            RapidYenc::_do_decode = weaver_generic_decode[0];
            RapidYenc::_do_decode_raw = weaver_generic_decode[1];
            RapidYenc::_do_decode_end_raw = weaver_generic_decode[2];
            RapidYenc::_decode_isa = ISA_GENERIC;
            break;
#ifdef PLATFORM_X86
        case ISA_LEVEL_SSE2: RapidYenc::decoder_set_sse2_funcs(); break;
        case ISA_LEVEL_SSSE3: RapidYenc::decoder_set_ssse3_funcs(); break;
        case ISA_LEVEL_AVX: RapidYenc::decoder_set_avx_funcs(); break;
        case ISA_LEVEL_AVX2: RapidYenc::decoder_set_avx2_funcs(); break;
        case ISA_LEVEL_VBMI2: RapidYenc::decoder_set_vbmi2_funcs(); break;
#endif
#ifdef PLATFORM_ARM
        case ISA_LEVEL_NEON: RapidYenc::decoder_set_neon_funcs(); break;
#endif
        default: break;
    }
}

// Each kernel's set-up allocates its lookup tables and never frees them, so
// each is set up once and its function pointers are put back afterwards.
static void weaver_decode_install(int isa) {
    static struct { int isa; weaver_decode_fn d, r, e; int level; } installed[8];
    static int installed_len = 0;
    for(int i = 0; i < installed_len; i++) {
        if(installed[i].isa == isa) {
            RapidYenc::_do_decode = installed[i].d;
            RapidYenc::_do_decode_raw = installed[i].r;
            RapidYenc::_do_decode_end_raw = installed[i].e;
            RapidYenc::_decode_isa = installed[i].level;
            return;
        }
    }
    weaver_decode_init_kernel(isa);
    if(installed_len < 8) {
        installed[installed_len].isa = isa;
        installed[installed_len].d = RapidYenc::_do_decode;
        installed[installed_len].r = RapidYenc::_do_decode_raw;
        installed[installed_len].e = RapidYenc::_do_decode_end_raw;
        installed[installed_len].level = RapidYenc::_decode_isa;
        installed_len++;
    }
}

// Force one decode kernel. Returns 1 when that exact kernel is active, 0 when
// refused (unsupported CPU, or a kernel file built without its ISA, whose
// set-up falls back to a lower kernel); on refusal the previous one returns.
extern "C" int weaver_rapidyenc_decode_force(int isa) {
    int previous = RapidYenc::_decode_isa;
    if(!weaver_decode_kernel_supported(isa)) return 0;
    weaver_decode_install(isa);
    if(RapidYenc::_decode_isa != isa) {
        weaver_decode_install(previous);
        return 0;
    }
    return 1;
}

// Raw decode (no end search) from the CRLF state, the state kept: what
// weaver's decode_rapidyenc runs. Returns bytes written.
extern "C" unsigned long long weaver_rapidyenc_decode_crlf(const void* src, void* dest, unsigned long long len) {
    RapidYenc::YencDecoderState state = RapidYenc::YDEC_STATE_CRLF;
    return (unsigned long long)RapidYenc::decode(1, src, dest, (size_t)len, &state);
}
