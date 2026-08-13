// Minimal extern "C" shim over RapidYenc::decode for same-process A/B timing.
// Compiled only when WEAVER_RAPIDYENC_SRC points at a rapidyenc checkout (see
// build.rs); never part of normal builds.
#include <cstddef>   // size_t — implicit under MSVC, required under g++/clang
#include <cstdint>
#include "src/decoder.h"

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
