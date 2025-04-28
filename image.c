// for unshare
#define _GNU_SOURCE

#include "def.h"
#include "err.h"

#if M3_LINUX

#include <limits.h>
#include <sched.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <stdio.h>

static const char M3_IMGDIR_TEMPLATE[] = "m3.XXXXXX";

static int image_enter_mount(m3_Err *err, const char *image, const char *mountpoint)
{
	if (unshare(CLONE_NEWNS | CLONE_NEWUSER))
		return m3_err_sys(err, M3_ERR_UNSHARE);
	if (mount(NULL, "/", NULL, MS_PRIVATE | MS_REC, NULL))
		return m3_err_sys(err, M3_ERR_MPRIV);
	char buf[PATH_MAX];
	memcpy(buf, "lowerdir=", 9);
	if (!realpath(".", buf+9))
		return m3_err_sys(err, M3_ERR_REALPATH);
	size_t n = strlen(buf);
	size_t nimg = strlen(image);
	if (n+nimg+2 > PATH_MAX)
		return m3_err_set(err, M3_ERR_PATHLEN);
	buf[n] = ':';
	memcpy(buf+n+1, image, nimg+1);
	if (mount("overlay", mountpoint, "overlay", 0, buf))
		return m3_err_sys(err, M3_ERR_MOVERLAY);
	if (chdir(mountpoint))
		return m3_err_sys(err, M3_ERR_CHDIR);
	return -2;
}

static pid_t image_child_pid;

static void image_sigint_handler(int signum)
{
	(void)signum;
	kill(image_child_pid, SIGINT); // kill our child
	signal(SIGINT, SIG_DFL); // if we are sent another sigint, kill ourselves, too
}

static int image_wait_child(pid_t pid, const char *mountpoint)
{
	image_child_pid = pid;
	struct sigaction act = {0};
	act.sa_handler = image_sigint_handler;
	struct sigaction old;
	sigaction(SIGINT, &act, &old);
	int status;
	waitpid(pid, &status, 0);
	sigaction(SIGINT, &old, NULL);
	rmdir(mountpoint);
	return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}

// return value:
//   -2  this is the child process, do some work
//   -1  something failed, check err
//   >=0 this is the main process, returning the child exit status
CFUNC int m3_image_enter(m3_Err *err, const char *image)
{
	// create mount point
	char temp[sizeof(M3_IMGDIR_TEMPLATE)];
	memcpy(temp, M3_IMGDIR_TEMPLATE, sizeof(M3_IMGDIR_TEMPLATE));
	if (!mkdtemp(temp))
		return m3_err_sys(err, M3_ERR_MKDTEMP);
	// fork:
	//   * parent handles cleanup
	//   * child performs useful work
	pid_t pid = fork();
	if (pid < 0)
		return m3_err_sys(err, M3_ERR_FORK);
	if (pid) {
		return image_wait_child(pid, temp);
		return 0;
	} else {
		prctl(PR_SET_PDEATHSIG, SIGTERM);
		return image_enter_mount(err, image, temp);
	}
}

#endif
