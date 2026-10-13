/*
 * SynodSQL C API (libsynod).
 *
 * Every function that returns int returns SYNOD_OK (0) or SYNOD_ERROR (1);
 * after an error, synod_last_error() gives the message on that thread.
 * Strings the library returns are freed with synod_free_string, and bytes
 * with synod_free_bytes. A synod_db is used by one thread at a time.
 *
 * Values for writes are text, as the synod command line takes them: NULL,
 * numbers, text as is, blobs as x'0a1b', sets as a,b,c. A NULL pointer
 * value is SQL NULL too. A row ID is text like 0000000a-0000000000000003.
 */
#ifndef SYNOD_H
#define SYNOD_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SYNOD_OK 0
#define SYNOD_ERROR 1

#define SYNOD_NULL 0
#define SYNOD_INTEGER 1
#define SYNOD_REAL 2
#define SYNOD_TEXT 3
#define SYNOD_BLOB 4

typedef struct SynodDb synod_db;
typedef struct SynodRows synod_rows;

/* A query parameter. For SYNOD_TEXT, data is NUL-terminated UTF-8; for
 * SYNOD_BLOB, data holds len bytes. */
typedef struct synod_value {
    int kind;
    int64_t integer;
    double real;
    const uint8_t *data;
    size_t len;
} synod_value;

const char *synod_version(void);
const char *synod_last_error(void);

/* Stores. epoch_ms 0 means 60000; lateness 0 means 16. */
int synod_create(const char *path, uint32_t node, uint64_t epoch_ms, uint64_t lateness, synod_db **out);
int synod_open(const char *path, synod_db **out);
int synod_clone(synod_db *db, const char *path, uint32_t node, synod_db **out);
int synod_close(synod_db *db);

/* Schema and writes. */
int synod_create_table(synod_db *db, const char *ddl);
int synod_insert(synod_db *db, const char *table, size_t n, const char *const *columns,
                 const char *const *values, char **row_id);
int synod_update(synod_db *db, const char *table, const char *row_id, size_t n,
                 const char *const *columns, const char *const *values);
int synod_delete(synod_db *db, const char *table, const char *row_id);

/* Queries: read-only SQL on the tables' views, with ?1, ?2, ... */
int synod_query(synod_db *db, const char *sql, size_t n, const synod_value *params, synod_rows **out);
size_t synod_rows_count(const synod_rows *r);
size_t synod_rows_columns(const synod_rows *r);
const char *synod_rows_name(const synod_rows *r, size_t col);
int synod_rows_type(const synod_rows *r, size_t row, size_t col);
int64_t synod_rows_int(const synod_rows *r, size_t row, size_t col);
double synod_rows_real(const synod_rows *r, size_t row, size_t col);
const char *synod_rows_text(const synod_rows *r, size_t row, size_t col);
const uint8_t *synod_rows_blob(const synod_rows *r, size_t row, size_t col, size_t *len);
char *synod_rows_display(const synod_rows *r, size_t row, size_t col);
void synod_rows_free(synod_rows *r);

/* Store commands of the synod command line, without the store path:
 * status, conflicts [--all], resolve <id>, revert --node <id> ... */
int synod_exec(synod_db *db, size_t argc, const char *const *argv, char **out);

/* File sync: seal ended epochs (first to first + count - 1), export a
 * sealed log, merge others' logs. */
int synod_seal(synod_db *db, uint64_t *first, size_t *count);
int synod_export(synod_db *db, uint64_t epoch, uint8_t **data, size_t *len);
int synod_merge(synod_db *db, size_t n, const uint8_t *const *logs, const size_t *lens, size_t *merged);

/* Cluster: run the node in the background (peers as "id=host:port,...").
 * Other calls go on working while it serves; synod_close stops it. */
int synod_serve(synod_db *db, const char *listen, const char *peers, const char *certs);
/* hosts: "1=10.0.0.1,2=db2.example.com" adds names to node certificates (NULL for none). */
int synod_make_certs(const char *dir, const char *nodes, const char *clients, const char *hosts);
int synod_remote(const char *addr, uint32_t node, const char *certs, const char *name, size_t argc,
                 const char *const *argv, char **out);

void synod_free_string(char *s);
void synod_free_bytes(uint8_t *data, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* SYNOD_H */
