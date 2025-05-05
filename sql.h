#if M3_AMALG

#if M3_LUADEF
#define SQLITE_FUNC CFUNC
#else
#define SQLITE_FUNC
#endif

CDEF typedef struct sqlite3 sqlite3;
CDEF typedef struct sqlite3_stmt sqlite3_stmt;
SQLITE_FUNC int sqlite3_initialize(void);
SQLITE_FUNC int sqlite3_open(const char *, sqlite3 **);
SQLITE_FUNC int sqlite3_close_v2(sqlite3 *);
SQLITE_FUNC int sqlite3_exec(sqlite3 *, const char *, int (*)(void *, int, char **, char **), void *, char **);
SQLITE_FUNC int sqlite3_prepare_v2(sqlite3 *, const char *, int, sqlite3_stmt **, const char **);
SQLITE_FUNC int sqlite3_step(sqlite3_stmt *);
SQLITE_FUNC int sqlite3_reset(sqlite3_stmt *);
SQLITE_FUNC int sqlite3_finalize(sqlite3_stmt *);
SQLITE_FUNC int sqlite3_bind_double(sqlite3_stmt *, int, double);
SQLITE_FUNC int sqlite3_bind_int64(sqlite3_stmt *, int, int64_t);
SQLITE_FUNC int sqlite3_bind_null(sqlite3_stmt *, int);
SQLITE_FUNC int sqlite3_bind_text(sqlite3_stmt *, int, const char *, int, void(*)(void*));
SQLITE_FUNC double sqlite3_column_double(sqlite3_stmt *, int);
SQLITE_FUNC int sqlite3_column_int(sqlite3_stmt *, int);
SQLITE_FUNC const char *sqlite3_column_text(sqlite3_stmt *, int);
SQLITE_FUNC int sqlite3_column_type(sqlite3_stmt *, int);
SQLITE_FUNC int sqlite3_column_count(sqlite3_stmt *);
SQLITE_FUNC const char *sqlite3_column_name(sqlite3_stmt *, int);
SQLITE_FUNC const char *sqlite3_sql(sqlite3_stmt *);
SQLITE_FUNC int sqlite3_bind_parameter_count(sqlite3_stmt *);
SQLITE_FUNC int sqlite3_bind_parameter_index(sqlite3_stmt *, const char *);
SQLITE_FUNC sqlite3 *sqlite3_db_handle(sqlite3_stmt *);
SQLITE_FUNC const char *sqlite3_errstr(int);
SQLITE_FUNC const char *sqlite3_errmsg(sqlite3 *);
SQLITE_FUNC const char *sqlite3_libversion(void);

#else

// sqlite requires _GNU_SOURCE
#define _GNU_SOURCE

#include "def.h"
#include "config.h"

#define SQLITE_API                     M3_NOAPI

// recommended flags, see: https://www.sqlite.org/compile.html#recommended_compile_time_options
#define SQLITE_THREADSAFE              M3_USE_THREADS
#define SQLITE_DEFAULT_MEMSTATUS       0
#define SQLITE_DEFAULT_WAL_SYNCHRONOUS 1
#define SQLITE_LIKE_DOESNT_MATCH_GLOBS
#define SQLITE_MAX_EXPR_DEPTH          0
#define SQLITE_OMIT_DECLTYPE
#define SQLITE_OMIT_DEPRECATED
#define SQLITE_OMIT_PROGRESS_CALLBACK
#define SQLITE_OMIT_SHARED_CACHE
#define SQLITE_USE_ALLOCA
#define SQLITE_OMIT_AUTOINIT

// we don't need these
#define SQLITE_OMIT_AUTHORIZATION
#define SQLITE_OMIT_COMPILEOPTION_DIAGS
#define SQLITE_OMIT_COMPLETE
#define SQLITE_OMIT_DESERIALIZE
#define SQLITE_OMIT_EXPLAIN
#define SQLITE_OMIT_GET_TABLE
#define SQLITE_OMIT_INCRBLOB
#define SQLITE_OMIT_INTEGRITY_CHECK
#define SQLITE_OMIT_INTROSPECTION_PRAGMAS
#define SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS
#define SQLITE_OMIT_TCL_VARIABLE
#define SQLITE_OMIT_TRACE
#define SQLITE_OMIT_UTF16

#define SQLITE_UNTESTABLE

#endif
