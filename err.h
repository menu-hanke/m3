#pragma once

typedef enum {
	M3_OK,
#define ERRDEF(name, _) M3_ERR_##name,
#include "errmsg.h"
#undef ERRDEF
} Status;
