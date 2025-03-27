// for sched_getaffinity:
#define _GNU_SOURCE

#include "def.h"

#if M3_WINDOWS

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

CFUNC int m3_sys_num_cpus(void)
{
	// windows also has a sched_getaffinity equivalent: GetProcessAffinityMask,
	// but that only supports up to 64 CPUs, so we don't use it.
	return GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
}

#else

#include <signal.h>
#include <stdlib.h>
#include <sched.h>
#include <sys/prctl.h>
#include <sys/wait.h>

CFUNC int m3_sys_num_cpus(void)
{
	cpu_set_t set;
	CPU_ZERO(&set);
	if (sched_getaffinity(0, sizeof(set), &set))
		return 0;
	return CPU_COUNT(&set);
}

CFUNC int m3_sys_fork(void)
{
	pid_t pid = fork();
	if (!pid)
		prctl(PR_SET_PDEATHSIG, SIGTERM);
	return pid;
}

CFUNC int m3_sys_waitpid(int pid)
{
	return waitpid(pid, NULL, WNOHANG);
}

#endif
