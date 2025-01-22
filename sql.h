// minimal version of sqlite3.h for ffi.cdef.
// this only contains the apis used by m3.

#pragma once

#if M3_LUADEF

#include "def.h"

CDEF typedef struct sqlite3 sqlite3;
CDEF typedef struct sqlite3_stmt sqlite3_stmt;

CDEF int sqlite3_open(const char *, sqlite3 **);
CDEF int sqlite3_close_v2(sqlite3 *);

CDEF int sqlite3_exec(sqlite3 *, const char *, int (*)(void *, int, char **, char **), void *, char **);

CDEF int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
CDEF int sqlite3_step(sqlite3_stmt *);
CDEF int sqlite3_reset(sqlite3_stmt *);
CDEF int sqlite3_finalize(sqlite3_stmt *);

CDEF int sqlite3_bind_double(sqlite3_stmt *, int, double);
CDEF int sqlite3_bind_null(sqlite3_stmt *, int);
CDEF int sqlite3_bind_text(sqlite3_stmt *, int, const char *, int, void(*)(void*));

CDEF double sqlite3_column_double(sqlite3_stmt *, int);
CDEF int sqlite3_column_int(sqlite3_stmt *, int);
CDEF const char *sqlite3_column_text(sqlite3_stmt *, int);
CDEF int sqlite3_column_type(sqlite3_stmt *, int);
CDEF int sqlite3_column_count(sqlite3_stmt *);
CDEF const char *sqlite3_column_name(sqlite3_stmt *, int);

CDEF const char *sqlite3_sql(sqlite3_stmt *);
CDEF int sqlite3_bind_parameter_count(sqlite3_stmt *);
CDEF int sqlite3_bind_parameter_index(sqlite3_stmt *, const char *);

CDEF sqlite3 *sqlite3_db_handle(sqlite3_stmt *);

CDEF const char *sqlite3_errstr(int);
CDEF const char *sqlite3_errmsg(sqlite3 *);

#endif
