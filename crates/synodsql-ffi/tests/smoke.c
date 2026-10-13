/* C smoke test of libsynod: smoke <dir>. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "synod.h"

#define CHECK(x)                                                                  \
    do {                                                                          \
        if ((x) != SYNOD_OK) {                                                    \
            fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #x, synod_last_error()); \
            exit(1);                                                              \
        }                                                                         \
    } while (0)

#define EXPECT(c)                                                        \
    do {                                                                 \
        if (!(c)) {                                                      \
            fprintf(stderr, "%s:%d: expected %s\n", __FILE__, __LINE__, #c); \
            exit(1);                                                     \
        }                                                                \
    } while (0)

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    char path[4096];
    snprintf(path, sizeof path, "%s/c.db", argv[1]);

    synod_db *db = NULL;
    CHECK(synod_create(path, 1, 0, 0, &db));
    EXPECT(synod_create(path, 1, 0, 0, &db) == SYNOD_ERROR && strstr(synod_last_error(), "exists"));
    CHECK(synod_create_table(db, "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, qty INTEGER MERGE delta, tags SET<TEXT> MERGE union)"));

    const char *cols[] = {"sku", "qty", "tags"};
    const char *vals[] = {"A-100", "800", "red,blue"};
    char *id = NULL;
    CHECK(synod_insert(db, "stock", 3, cols, vals, &id));
    const char *vals2[] = {"B-200", NULL, ""};
    CHECK(synod_insert(db, "stock", 3, cols, vals2, NULL));
    const char *qty[] = {"qty"}, *v650[] = {"650"};
    CHECK(synod_update(db, "stock", id, 1, qty, v650));
    EXPECT(synod_update(db, "stock", id, 1, qty, (const char *[]){"many"}) == SYNOD_ERROR);

    synod_value p = {SYNOD_INTEGER, 100, 0.0, NULL, 0};
    synod_rows *r = NULL;
    CHECK(synod_query(db, "SELECT row_id, sku, qty, tags FROM stock WHERE qty > ?1", 1, &p, &r));
    EXPECT(synod_rows_count(r) == 1 && synod_rows_columns(r) == 4);
    EXPECT(strcmp(synod_rows_name(r, 2), "qty") == 0);
    EXPECT(synod_rows_type(r, 0, 1) == SYNOD_TEXT && strcmp(synod_rows_text(r, 0, 1), "A-100") == 0);
    EXPECT(synod_rows_type(r, 0, 2) == SYNOD_INTEGER && synod_rows_int(r, 0, 2) == 650);
    size_t len = 0;
    EXPECT(synod_rows_blob(r, 0, 0, &len) != NULL && len == 16);
    char *shown = synod_rows_display(r, 0, 0);
    EXPECT(strcmp(shown, id) == 0);
    synod_free_string(shown);
    shown = synod_rows_display(r, 0, 3);
    EXPECT(shown[0] == '{' && strstr(shown, "red") && strstr(shown, "blue"));
    synod_free_string(shown);
    synod_rows_free(r);

    CHECK(synod_delete(db, "stock", id));
    synod_free_string(id);
    CHECK(synod_query(db, "SELECT count(*) FROM stock", 0, NULL, &r));
    EXPECT(synod_rows_int(r, 0, 0) == 1);
    synod_rows_free(r);

    const char *status[] = {"status"};
    char *out = NULL;
    CHECK(synod_exec(db, 1, status, &out));
    EXPECT(strstr(out, "node            00000001") != NULL);
    synod_free_string(out);
    CHECK(synod_close(db));

    CHECK(synod_open(path, &db));
    CHECK(synod_query(db, "SELECT sku FROM stock", 0, NULL, &r));
    EXPECT(synod_rows_count(r) == 1 && strcmp(synod_rows_text(r, 0, 0), "B-200") == 0);
    synod_rows_free(r);
    CHECK(synod_close(db));
    printf("C: ok (libsynod %s)\n", synod_version());
    return 0;
}
