// Minimal extern "C" shim over RapidYenc::decode for same-process A/B timing.
// Compiled only when WEAVER_RAPIDYENC_SRC points at a rapidyenc checkout (see
// build.rs); never part of normal builds.
#include "src/decoder.h"

extern "C" void weaver_rapidyenc_decode_init(void) {
    RapidYenc::decoder_init();
}

extern "C" unsigned long long weaver_rapidyenc_decode(const void* src, void* dest, unsigned long long len) {
    return (unsigned long long)RapidYenc::decode(1, src, dest, (size_t)len, nullptr);
}
