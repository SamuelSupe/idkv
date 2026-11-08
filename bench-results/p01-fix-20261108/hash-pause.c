/* Test-only gate for reads of this experiment's private export SST copies. */
#include <time.h>

ssize_t read(int fd, void *buffer, size_t size) {
    const char *root = getenv("IDKV_IO_ROOT");
    const char *control = getenv("IDKV_HASH_CONTROL");
    const char *log = getenv("IDKV_HASH_LOG");
    char name[64], path[PATH_MAX];
    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(name, path, sizeof(path) - 1);
    if (root && control && log && length > 0) {
        path[length] = 0;
        size_t prefix = strlen(root);
        if (!strncmp(path, root, prefix) && path[prefix] == '/' &&
            strstr(path, "/replication-work/export/") && strstr(path, ".sst") &&
            access(control, F_OK) == 0) {
            int output = syscall(SYS_openat, AT_FDCWD, log, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
            if (output >= 0) {
                syscall(SYS_write, output, "export hash paused\n", 19);
                syscall(SYS_close, output);
            }
            const struct timespec delay = {.tv_nsec = 10000000};
            while (access(control, F_OK) == 0) nanosleep(&delay, NULL);
        }
    }
    return syscall(SYS_read, fd, buffer, size);
}
