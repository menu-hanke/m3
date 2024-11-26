#pragma once

#include "def.h"

#include <lua.h>

#if M3_LINUX
NOAPI int m3_mp_fork(lua_State *L);
#endif
