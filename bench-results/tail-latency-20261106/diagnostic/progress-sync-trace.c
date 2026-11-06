#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
static long long now(void){struct timespec t;clock_gettime(CLOCK_MONOTONIC,&t);return (long long)t.tv_sec*1000000000+t.tv_nsec;}
static void record(int fd,const char *op,long long begin,long long end){
 int saved=errno; const char *log=getenv("IDKV_TRACE_LOG");const char *root=getenv("IDKV_TRACE_ROOT");
 if(!log||!root){errno=saved;return;}
 char link[64],path[PATH_MAX];snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
 ssize_t n=readlink(link,path,sizeof(path)-1);if(n<0){errno=saved;return;}path[n]=0;
 if(strncmp(path,root,strlen(root)) || (end-begin<5000000 && !strstr(path,"/control/backup-jobs"))){errno=saved;return;}
 int out=syscall(SYS_openat,AT_FDCWD,log,O_WRONLY|O_CREAT|O_APPEND|O_CLOEXEC,0600);
 if(out>=0){char line[PATH_MAX+160];int size=snprintf(line,sizeof(line),"%lld %lld %ld %s %s\n",begin,end,syscall(SYS_gettid),op,path);syscall(SYS_write,out,line,size);syscall(SYS_close,out);}errno=saved;
}
int fsync(int fd){long long b=now();int r=syscall(SYS_fsync,fd);record(fd,"fsync",b,now());return r;}
int fdatasync(int fd){long long b=now();int r=syscall(SYS_fdatasync,fd);record(fd,"fdatasync",b,now());return r;}
ssize_t pwrite64(int fd,const void *p,size_t n,off64_t o){long long b=now();ssize_t r=syscall(SYS_pwrite64,fd,p,n,o);record(fd,"pwrite64",b,now());return r;}
ssize_t write(int fd,const void *p,size_t n){long long b=now();ssize_t r=syscall(SYS_write,fd,p,n);record(fd,"write",b,now());return r;}
