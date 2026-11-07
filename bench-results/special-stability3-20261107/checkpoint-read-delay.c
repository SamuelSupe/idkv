/* Test-only: pause SST reads solely within this experiment's private backup. */
#define _GNU_SOURCE
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static void delay_checkpoint(int fd) {
    const char *root = getenv("IDKV_DELAY_ROOT");
    const char *control = getenv("IDKV_DELAY_CONTROL");
    const char *marker = getenv("IDKV_DELAY_MARKER");
    if (!root || !control || !marker || access(control, F_OK)) return;
    char name[64], path[PATH_MAX];
    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(name, path, sizeof(path) - 1);
    if (length < 0) return;
    path[length] = 0;
    size_t prefix = strlen(root);
    if (strncmp(path, root, prefix) || path[prefix] != '/' || !strstr(path, "/sst/")) return;
    int output = syscall(SYS_openat, AT_FDCWD, marker, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (output >= 0) {
        syscall(SYS_write, output, path, length);
        syscall(SYS_close, output);
    }
    while (access(control, F_OK) == 0) usleep(10000);
}
ssize_t pread64(int fd, void *buffer, size_t size, off64_t offset) {
    delay_checkpoint(fd);
    return syscall(SYS_pread64, fd, buffer, size, offset);
}
ssize_t pread(int fd, void *buffer, size_t size, off_t offset) {
    delay_checkpoint(fd);
    return syscall(SYS_pread64, fd, buffer, size, offset);
}
