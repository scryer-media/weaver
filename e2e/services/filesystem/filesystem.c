// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A real FUSE mount with selectable capability refusals, not an implementation
// of Unraid shfs. Libfuse's ordinary hidden-file handling deliberately remains
// enabled: unlinking a live handle must expose the lifetime bug to delivery.
#define _GNU_SOURCE
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/statvfs.h>
#include <unistd.h>

static int backing;
static int portable;
static int ready_fd;

static const char *relative(const char *path) { return path[1] ? path + 1 : "."; }
static int outcome(int result) { return result == -1 ? -errno : result; }
static int refuse(const char *operation, int error) {
    fprintf(stderr, "filesystem-fixture refused %s: %d\n", operation, error);
    return -error;
}

static void *initialize(struct fuse_conn_info *connection, struct fuse_config *config) {
    (void)connection;
    config->use_ino = 1;
    config->entry_timeout = 0;
    config->attr_timeout = 0;
    config->negative_timeout = 0;
    if (write(ready_fd, "1", 1) != 1) abort();
    close(ready_fd);
    return NULL;
}

static int attributes(const char *path, struct stat *st, struct fuse_file_info *file) {
    return outcome(file ? fstat((int)file->fh, st) :
                   fstatat(backing, relative(path), st, AT_SYMLINK_NOFOLLOW));
}
static int directory(const char *path, mode_t mode) {
    if (mkdirat(backing, relative(path), mode) < 0) return -errno;
    struct fuse_context *context = fuse_get_context();
    return outcome(fchownat(backing, relative(path), context->uid, context->gid, AT_SYMLINK_NOFOLLOW));
}
static int remove_file(const char *path) { return outcome(unlinkat(backing, relative(path), 0)); }
static int remove_directory(const char *path) { return outcome(unlinkat(backing, relative(path), AT_REMOVEDIR)); }
static int move(const char *from, const char *to, unsigned int flags) {
    if (flags) return refuse("exclusive-rename", EINVAL);
    return outcome(renameat(backing, relative(from), backing, relative(to)));
}
static int hard_link(const char *from, const char *to) {
    if (portable) return refuse("hard-link", EOPNOTSUPP);
    return outcome(linkat(backing, relative(from), backing, relative(to), 0));
}
static int symbolic_link(const char *target, const char *path) {
    if (symlinkat(target, backing, relative(path)) < 0) return -errno;
    struct fuse_context *context = fuse_get_context();
    return outcome(fchownat(backing, relative(path), context->uid, context->gid, AT_SYMLINK_NOFOLLOW));
}
static int read_link(const char *path, char *buffer, size_t size) {
    ssize_t length = readlinkat(backing, relative(path), buffer, size - 1);
    if (length < 0) return -errno;
    buffer[length] = 0;
    return 0;
}
static int mode(const char *path, mode_t permissions, struct fuse_file_info *file) {
    if (portable) return refuse("chmod", EPERM);
    return outcome(file ? fchmod((int)file->fh, permissions) :
                   fchmodat(backing, relative(path), permissions, 0));
}
static int owner(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *file) {
    if (portable) return refuse("chown", EPERM);
    return outcome(file ? fchown((int)file->fh, uid, gid) :
                   fchownat(backing, relative(path), uid, gid, AT_SYMLINK_NOFOLLOW));
}

// The portable profile really allocates gaps. Merely rejecting fallocate
// would still leave sparse writes supported by the backing filesystem.
static int fill_gap(int fd, off_t end) {
    if (!portable) return 0;
    struct stat st;
    if (fstat(fd, &st) < 0) return -errno;
    char zeros[65536] = {0};
    for (off_t at = st.st_size; at < end;) {
        size_t count = (end - at) > (off_t)sizeof(zeros) ? sizeof(zeros) : (size_t)(end - at);
        ssize_t written = pwrite(fd, zeros, count, at);
        if (written < 0) return -errno;
        if (!written) return -EIO;
        at += written;
    }
    return 0;
}
static int resize(const char *path, off_t length, struct fuse_file_info *file) {
    int fd = file ? (int)file->fh : openat(backing, relative(path), O_WRONLY | O_NOFOLLOW);
    if (fd < 0) return -errno;
    int result = fill_gap(fd, length);
    if (!result) result = outcome(ftruncate(fd, length));
    if (!file) close(fd);
    return result;
}
static int open_file(const char *path, struct fuse_file_info *file) {
    int fd = openat(backing, relative(path), file->flags | O_NOFOLLOW);
    if (fd < 0) return -errno;
    file->fh = (uint64_t)fd;
    return 0;
}
static int create_file(const char *path, mode_t permissions, struct fuse_file_info *file) {
    int fd = openat(backing, relative(path), file->flags | O_CREAT | O_NOFOLLOW, permissions);
    if (fd < 0) return -errno;
    struct fuse_context *context = fuse_get_context();
    if (fchown(fd, context->uid, context->gid) < 0) {
        int error = errno;
        close(fd);
        return -error;
    }
    file->fh = (uint64_t)fd;
    return 0;
}
static int read_file(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *file) {
    (void)path;
    return outcome((int)pread((int)file->fh, buffer, size, offset));
}
static int write_file(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *file) {
    (void)path;
    int result = fill_gap((int)file->fh, offset);
    if (result) return result;
    return outcome((int)pwrite((int)file->fh, buffer, size, offset));
}
static int release(const char *path, struct fuse_file_info *file) {
    (void)path;
    return outcome(close((int)file->fh));
}
static int sync_file(const char *path, int data, struct fuse_file_info *file) {
    (void)path;
    return outcome(data ? fdatasync((int)file->fh) : fsync((int)file->fh));
}
static int space(const char *path, struct statvfs *info) {
    (void)path;
    return outcome(fstatvfs(backing, info));
}
static int entries(const char *path, void *buffer, fuse_fill_dir_t fill, off_t offset,
                   struct fuse_file_info *file, enum fuse_readdir_flags flags) {
    (void)file;
    (void)flags;
    int fd = openat(backing, relative(path), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return -errno;
    DIR *dir = fdopendir(fd);
    if (!dir) { int error = errno; close(fd); return -error; }
    seekdir(dir, offset);
    int result = 0;
    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(dir);
        if (!entry) { result = -errno; break; }
        struct stat st = {.st_ino = entry->d_ino, .st_mode = (mode_t)entry->d_type << 12};
        if (fill(buffer, entry->d_name, &st, telldir(dir), 0)) break;
    }
    closedir(dir);
    return result;
}
static int allocate(const char *path, int flags, off_t offset, off_t size, struct fuse_file_info *file) {
    (void)path; (void)flags; (void)offset; (void)size; (void)file;
    return refuse("fallocate", EOPNOTSUPP);
}

int main(int argc, char **argv) {
    if (argc < 4) { fprintf(stderr, "usage: filesystem BACKING MOUNT FUSE_OPTIONS...\n"); return 2; }
    const char *profile = getenv("WEAVER_FS_PROFILE");
    if (!profile || (strcmp(profile, "unraid") && strcmp(profile, "portable"))) {
        fprintf(stderr, "WEAVER_FS_PROFILE must be unraid or portable\n"); return 2;
    }
    portable = !strcmp(profile, "portable");
    const char *ready = getenv("WEAVER_FS_READY_FD");
    if (!ready) return 2;
    ready_fd = atoi(ready);
    backing = open(argv[1], O_RDONLY | O_DIRECTORY);
    if (backing < 0) { perror("backing directory"); return 1; }
    struct fuse_operations operations = {
        .init = initialize, .getattr = attributes, .mkdir = directory,
        .unlink = remove_file, .rmdir = remove_directory, .rename = move,
        .link = hard_link, .symlink = symbolic_link, .readlink = read_link,
        .chmod = mode, .chown = owner, .truncate = resize, .open = open_file,
        .create = create_file, .read = read_file, .write = write_file,
        .release = release, .fsync = sync_file, .statfs = space,
        .readdir = entries, .fallocate = allocate,
    };
    argv[1] = argv[0];
    int result = fuse_main(argc - 1, argv + 1, &operations, NULL);
    close(backing);
    return result;
}
