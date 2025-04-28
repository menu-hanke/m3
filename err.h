#pragma once

#include "def.h"

typedef enum {
#define ERRDEF(name, msg) \
	M3_ERR_##name, M3_ERR_##name##_ = M3_ERR_##name + sizeof(msg)-1,
#include "errmsg.h"
#undef ERRDEF
} ErrMsg;

CDEF typedef struct m3_Err {
	char *ep;
	uint8_t is_malloc;
} m3_Err;

M3_FUNC int m3_err_set(m3_Err *err, ErrMsg msg);
M3_FUNC int m3_err_sys(m3_Err *err, ErrMsg msg);
