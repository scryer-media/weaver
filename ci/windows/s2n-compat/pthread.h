#pragma once

#ifdef _WIN32
#include <stdint.h>

typedef uintptr_t pthread_t;
typedef int pthread_mutexattr_t;

typedef struct pthread_mutex_t {
    void *lock;
} pthread_mutex_t;

__declspec(dllimport) unsigned long __stdcall GetCurrentThreadId(void);
__declspec(dllimport) void __stdcall InitializeSRWLock(void *lock);
__declspec(dllimport) void __stdcall AcquireSRWLockExclusive(void *lock);
__declspec(dllimport) void __stdcall ReleaseSRWLockExclusive(void *lock);

static inline pthread_t pthread_self(void)
{
    return (pthread_t) GetCurrentThreadId();
}

static inline int pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr)
{
    (void) attr;
    InitializeSRWLock(&mutex->lock);
    return 0;
}

static inline int pthread_mutex_destroy(pthread_mutex_t *mutex)
{
    (void) mutex;
    return 0;
}

static inline int pthread_mutex_lock(pthread_mutex_t *mutex)
{
    AcquireSRWLockExclusive(&mutex->lock);
    return 0;
}

static inline int pthread_mutex_unlock(pthread_mutex_t *mutex)
{
    ReleaseSRWLockExclusive(&mutex->lock);
    return 0;
}
#endif
