// for asprintf
#define _GNU_SOURCE

#include "def.h"
#include "err.h"

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static const char *err_allmsg =
#define ERRDEF(name, msg)	msg "\0"
#include "errmsg.h"
#undef ERRDEF
;

#define err2msg(msg) (err_allmsg+(size_t)msg)

CFUNC void m3_err_clear(m3_Err *err)
{
	if (err->is_malloc) {
		free(err->ep);
		err->ep = NULL;
		err->is_malloc = 0;
	}
}

#if M3_WINDOWS

COLD int m3_err_set(m3_Err *err, ErrMsg msg)
{
	if (err) {
		m3_err_clear(err);
		err->ep = (char *) err2msg(msg);
	}
	return 1;
}

#endif

COLD int m3_err_sys(m3_Err *err, ErrMsg msg)
{
	if (err) {
		m3_err_clear(err);
		if (asprintf(&err->ep, "%s: %s", err2msg(msg), strerror(errno)) >= 0) {
			err->is_malloc = 1;
		} else {
			err->ep = NULL;
		}
	}
	return 1;
}
