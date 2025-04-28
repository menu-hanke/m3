#pragma once

#include "def.h"

typedef enum {
#define ERRDEF(name, _) M3_ERR_##name,
#include "errmsg.h"
#undef ERRDEF
} ErrMsg;

CDEF typedef struct m3_Err {
	char *ep;
	uint8_t is_malloc;
} m3_Err;

#if M3_WINDOWS
M3_FUNC int m3_err_set(m3_Err *err, ErrMsg msg);
#endif
M3_FUNC int m3_err_sys(m3_Err *err, ErrMsg msg);
