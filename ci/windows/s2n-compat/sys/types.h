#pragma once

#ifdef _WIN32
#include <stdint.h>
#include_next <sys/types.h>

#ifndef _SSIZE_T_DEFINED
#define _SSIZE_T_DEFINED
typedef intptr_t ssize_t;
#endif

#ifndef SSIZE_MAX
#define SSIZE_MAX INTPTR_MAX
#endif

#ifndef _MODE_T_DEFINED
#define _MODE_T_DEFINED
typedef int mode_t;
#endif
#endif
