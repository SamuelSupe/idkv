/* Isolated Linux probe: pause after the checkpoint's final directory fsync. */
#define fsync wal_fault_fsync
#include "../../scripts/wal_io_fault.c"
#undef fsync
#include <time.h>

int fsync(int fd) {
    int result = wal_fault_fsync(fd);
    const char *control = getenv("IDKV_CHECKPOINT_PAUSE");
    const char *ready = getenv("IDKV_CHECKPOINT_READY");
    if (result || !control || !ready) return result;
    char target[PATH_MAX], name[64], path[PATH_MAX];
    int input = syscall(SYS_openat, AT_FDCWD, control, O_RDONLY | O_CLOEXEC, 0);
    if (input < 0) return result;
    ssize_t count = syscall(SYS_read, input, target, sizeof(target) - 1);
    syscall(SYS_close, input);
    if (count <= 0) return result;
    target[count] = 0;
    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(name, path, sizeof(path) - 1);
    if (length <= 0) return result;
    path[length] = 0;
    if (strcmp(path, target)) return result;
    int output = syscall(SYS_openat, AT_FDCWD, ready, O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
    if (output >= 0) syscall(SYS_close, output);
    const struct timespec delay = {.tv_nsec = 10000000};
    while (access(control, F_OK) == 0) nanosleep(&delay, NULL);
    return result;
}
