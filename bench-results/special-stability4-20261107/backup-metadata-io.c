/* Test-only backup control-directory I/O faults in one owned experiment root. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static int fault(int fd) {
    const char *root = getenv("IDKV_META_ROOT"), *control = getenv("IDKV_META_CONTROL");
    const char *marker = getenv("IDKV_META_MARKER");
    if (!root || !control || !marker) return 0;
    char name[64], path[PATH_MAX], mode[32] = {0};
    snprintf(name, sizeof(name), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(name, path, sizeof(path)-1);
    if (length < 0) return 0;
    path[length] = 0;
    if (strncmp(path,root,strlen(root))) return 0;
    int ctl = syscall(SYS_openat,AT_FDCWD,control,O_RDONLY|O_CLOEXEC,0);
    if (ctl < 0) return 0;
    syscall(SYS_read,ctl,mode,sizeof(mode)-1); syscall(SYS_close,ctl);
    int fail = !strcmp(mode,"directory-eio") && !strcmp(path,root);
    int delay = 0;
    if ((!strcmp(mode,"checkpointing-delay") || !strcmp(mode,"terminal-eio")) && path[strlen(root)] == '/') {
        char bytes[8192] = {0};
        int input = syscall(SYS_openat,AT_FDCWD,path,O_RDONLY|O_CLOEXEC,0);
        if (input >= 0) {
            syscall(SYS_read,input,bytes,sizeof(bytes)-1); syscall(SYS_close,input);
            delay = strstr(bytes+8,"\"state\":\"checkpointing\"") != NULL;
            if (!strcmp(mode,"terminal-eio")) {
                delay = 0;
                fail = strstr(bytes+8,"\"state\":\"completed\"") != NULL;
            }
        }
    }
    if (!fail && !delay) return 0;
    int output = syscall(SYS_openat,AT_FDCWD,marker,O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC,0600);
    if (output >= 0) { syscall(SYS_write,output,path,length); syscall(SYS_close,output); }
    if (fail) {
        syscall(SYS_unlinkat,AT_FDCWD,control,0);
        errno = EIO; return 1;
    }
    while (access(control,F_OK)==0) usleep(10000);
    return 0;
}
int fsync(int fd) { return fault(fd) ? -1 : syscall(SYS_fsync,fd); }
int fdatasync(int fd) { return fault(fd) ? -1 : syscall(SYS_fdatasync,fd); }
