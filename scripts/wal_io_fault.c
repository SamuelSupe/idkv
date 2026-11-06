/* Test-only Linux syscall fault shim. Only WAL fds under IDKV_IO_ROOT can fail. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

static int fail_wal(int fd, const char *operation) {
    const char *root = getenv("IDKV_IO_ROOT");
    const char *control = getenv("IDKV_IO_CONTROL");
    const char *log = getenv("IDKV_IO_LOG");
    if (!root || !control || !log) return 0;
    char name[64], path[PATH_MAX], mode[16] = {0};
    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(name, path, sizeof(path) - 1);
    if (length < 0) return 0;
    path[length] = 0;
    size_t prefix = strlen(root);
    if (strncmp(path, root, prefix) || path[prefix] != '/' || !strstr(path, "/wal/")) return 0;
    int setting = syscall(SYS_openat, AT_FDCWD, control, O_RDONLY | O_CLOEXEC, 0);
    if (setting < 0) return 0;
    ssize_t count = syscall(SYS_read, setting, mode, sizeof(mode) - 1);
    syscall(SYS_close, setting);
    if (count <= 0 || strncmp(mode, operation, strlen(operation))) return 0;
    int output = syscall(SYS_openat, AT_FDCWD, log, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
    if (output >= 0) {
        char line[PATH_MAX + 64];
        int size = snprintf(line, sizeof(line), "%s EIO %s\n", operation, path);
        syscall(SYS_write, output, line, size);
        syscall(SYS_close, output);
    }
    errno = EIO;
    return 1;
}

ssize_t write(int fd, const void *buffer, size_t size) {
    return fail_wal(fd, "write") ? -1 : syscall(SYS_write, fd, buffer, size);
}
ssize_t writev(int fd, const struct iovec *vectors, int count) {
    return fail_wal(fd, "write") ? -1 : syscall(SYS_writev, fd, vectors, count);
}
ssize_t pwrite64(int fd, const void *buffer, size_t size, off64_t offset) {
    return fail_wal(fd, "write") ? -1 : syscall(SYS_pwrite64, fd, buffer, size, offset);
}
int fsync(int fd) {
    return fail_wal(fd, "sync") ? -1 : syscall(SYS_fsync, fd);
}
int fdatasync(int fd) {
    return fail_wal(fd, "sync") ? -1 : syscall(SYS_fdatasync, fd);
}
