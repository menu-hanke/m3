ERRDEF(LSTATE,   "failed to create lua state")
ERRDEF(LINIT,    "failed to initialize environment")
ERRDEF(MMAP,     "failed to map virtual memory")
ERRDEF(OOM,      "out of memory")
#if M3_LINUX
ERRDEF(FORK,     "fork failed")
ERRDEF(UNSHARE,  "unshare failed")
ERRDEF(REALPATH, "realpath failed")
ERRDEF(CHDIR,    "chdir failed")
ERRDEF(MPRIV,    "failed to change mount propagation")
ERRDEF(MOVERLAY, "failed to mount overlay")
ERRDEF(MKDTEMP,  "failed to create temporary directory")
ERRDEF(PATHLEN,  "too long path")
#endif
