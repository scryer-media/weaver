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

// CRC32 counterpart, for the crc_probe attribution harness. `crc32_init()` is a
// separate initializer from `decoder_init()` (it builds the slice table and then
// installs the PCLMUL/VPCLMUL/ARM function pointers), so it must be called
// before weaver_rapidyenc_crc32.
extern "C" void weaver_rapidyenc_crc32_init(void) {
    RapidYenc::crc32_init();
}

// `init` is the previous CRC in the finalized (post-xor) domain, matching
// weaver's Crc32 and crc_fast; pass 0 for a fresh checksum.
extern "C" uint32_t weaver_rapidyenc_crc32(const void* data, unsigned long long len, uint32_t init) {
    return RapidYenc::crc32(data, (size_t)len, init);
}

// Which CRC kernel rapidyenc's own dispatch installed (YEncDecIsaLevel, see
// rapidyenc src/common.h). Reported by the probe as rapidyenc-side attribution.
extern "C" int weaver_rapidyenc_crc32_isa(void) {
    return RapidYenc::crc32_isa_level();
}
