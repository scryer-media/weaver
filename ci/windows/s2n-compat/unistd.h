#pragma once

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <sys/stat.h>
#include <time.h>

__declspec(dllimport) void __stdcall Sleep(unsigned long dwMilliseconds);

#ifndef fstat
#define fstat _fstat
#endif

#ifndef open
#define open _open
#endif

#ifndef close
#define close _close
#endif

#ifndef read
#define read _read
#endif

#ifndef write
#define write _write
#endif

#ifndef STDIN_FILENO
#define STDIN_FILENO 0
#endif

#ifndef STDOUT_FILENO
#define STDOUT_FILENO 1
#endif

#ifndef STDERR_FILENO
#define STDERR_FILENO 2
#endif

static inline int nanosleep(const struct timespec *requested, struct timespec *remaining)
{
    if (remaining) {
        remaining->tv_sec = 0;
        remaining->tv_nsec = 0;
    }

    if (!requested) {
        return 0;
    }

    unsigned long milliseconds = (unsigned long) (requested->tv_sec * 1000);
    milliseconds += (unsigned long) ((requested->tv_nsec + 999999) / 1000000);
    Sleep(milliseconds);
    return 0;
}
#endif
