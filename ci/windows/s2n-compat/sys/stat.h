#pragma once

#ifdef _WIN32
#include_next <sys/stat.h>

#ifndef _MODE_T_DEFINED
#define _MODE_T_DEFINED
typedef int mode_t;
#endif

#ifndef S_IRWXU
#define S_IRWXU 0700
#endif

#ifndef S_IRWXG
#define S_IRWXG 0070
#endif

#ifndef S_IRWXO
#define S_IRWXO 0007
#endif
#endif
