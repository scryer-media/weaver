// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// extern "C" shim over rapidyenc's ENCODER, for the encode oracle tests and
// the encode bench. Compiled only when WEAVER_RAPIDYENC_SRC points at a
// rapidyenc checkout (see rapidyenc_encode_build.rs); never part of normal
// builds. The decode shim (rapidyenc_shim.cc) is separate and unchanged.
//
// rapidyenc keeps its encode kernel in a process-global function pointer, so
// forcing a kernel affects every later call in the process. Callers that
// force from several threads must serialise force + encode themselves.
#include <cstddef>
#include <cstdint>
#include "rapidyenc.h"
#include "src/common.h"
#include "src/encoder_common.h"
#include "src/encoder.h"

extern "C" void weaver_rapidyenc_encode_init(void) {
	rapidyenc_encode_init();
}

// The public entry point, exactly: rapidyenc_encode_ex.
extern "C" unsigned long long weaver_rapidyenc_encode_ex(
	int line_size,
	int* column,
	const void* src,
	void* dest,
	unsigned long long len,
	int is_end
) {
	return (unsigned long long)rapidyenc_encode_ex(line_size, column, src, dest, (size_t)len, is_end);
}

extern "C" unsigned long long weaver_rapidyenc_encode_max_length(unsigned long long len, int line_size) {
	return (unsigned long long)rapidyenc_encode_max_length((size_t)len, line_size);
}

// The active encode kernel, as a RYKERN_* value.
extern "C" int weaver_rapidyenc_encode_kernel(void) {
	return rapidyenc_encode_kernel();
}

// Whether this CPU can run the given kernel. On x86 this is rapidyenc's own
// cpu_supports_isa(), which also demotes a few slow microarchitectures
// (Bonnell/Silvermont, Bobcat, Goldmont); a demoted kernel is refused even if
// the instructions exist. Refusing is safe; running an unsupported kernel is
// not.
static bool weaver_kernel_supported(int kernel) {
	if(kernel == RYKERN_GENERIC) return true;
#ifdef PLATFORM_X86
	int isa = RapidYenc::cpu_supports_isa();
	switch(kernel) {
		case RYKERN_SSE2: return isa >= ISA_LEVEL_SSE2;
		case RYKERN_SSSE3: return isa >= ISA_LEVEL_SSSE3;
		case RYKERN_AVX: return isa >= ISA_LEVEL_AVX;
		case RYKERN_AVX2: return isa >= ISA_LEVEL_AVX2;
		case RYKERN_VBMI2: return isa >= ISA_LEVEL_VBMI2;
		default: return false;
	}
#elif defined(PLATFORM_ARM)
	if(kernel == RYKERN_NEON) return RapidYenc::cpu_supports_neon();
	return false;
#else
	return false;
#endif
}

static void weaver_init_kernel(int kernel);

// A SIMD kernel's init allocates its lookup table and never frees it (about
// 2 MB for AVX2), so each kernel is initialised once and the function it
// installed is put back on every later request. Callers serialise access.
static void weaver_install_kernel(int kernel) {
	static struct {
		int kernel;
		decltype(RapidYenc::_do_encode) encode;
		decltype(RapidYenc::_encode_isa) isa;
	} installed[8];
	static int installed_len = 0;
	for(int i = 0; i < installed_len; i++) {
		if(installed[i].kernel == kernel) {
			RapidYenc::_do_encode = installed[i].encode;
			RapidYenc::_encode_isa = installed[i].isa;
			return;
		}
	}
	weaver_init_kernel(kernel);
	if(installed_len < 8) {
		installed[installed_len].kernel = kernel;
		installed[installed_len].encode = RapidYenc::_do_encode;
		installed[installed_len].isa = RapidYenc::_encode_isa;
		installed_len++;
	}
}

static void weaver_init_kernel(int kernel) {
	switch(kernel) {
		case RYKERN_GENERIC:
			RapidYenc::_do_encode = &RapidYenc::do_encode_generic;
			RapidYenc::_encode_isa = ISA_GENERIC;
			break;
#ifdef PLATFORM_X86
		case RYKERN_SSE2: RapidYenc::encoder_sse2_init(); break;
		case RYKERN_SSSE3: RapidYenc::encoder_ssse3_init(); break;
		case RYKERN_AVX: RapidYenc::encoder_avx_init(); break;
		case RYKERN_AVX2: RapidYenc::encoder_avx2_init(); break;
		case RYKERN_VBMI2: RapidYenc::encoder_vbmi2_init(); break;
#endif
#ifdef PLATFORM_ARM
		case RYKERN_NEON: RapidYenc::encoder_neon_init(); break;
#endif
		default: break;
	}
}

// Force one encode kernel (a RYKERN_* value). Returns 1 when that exact
// kernel is now active, 0 when it was refused (unsupported CPU, unknown id,
// or a kernel file compiled without its ISA, whose init silently falls back
// to a lower kernel); on refusal the previous kernel is put back.
extern "C" int weaver_rapidyenc_encode_force(int kernel) {
	rapidyenc_encode_init();
	int previous = RapidYenc::_encode_isa;
	if(!weaver_kernel_supported(kernel)) return 0;
	weaver_install_kernel(kernel);
	if(RapidYenc::_encode_isa != kernel) {
		weaver_install_kernel(previous);
		return 0;
	}
	return 1;
}
