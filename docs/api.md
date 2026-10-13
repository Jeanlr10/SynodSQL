# Client API

Every SynodSQL node serves a JSON API on its port, for programs in any language: anything that speaks HTTPS with a client certificate can use it. The full rules are in the [specification, Section 10](spec/v0.1.md#10-client-api); this page is a short reference.

- HTTPS with mutual TLS, HTTP/1.1 or HTTP/2.
- One request is one call. A write request is one transaction: all of its operations, or none.
- No sessions, no drivers. Bodies (at most 16 MiB) and answers are JSON objects.

## Setup

Make certificates that name the address clients connect to, and serve:

```sh
synod certs certs --nodes 1 --hosts 1=127.0.0.1,1=db1.example.com
synod init n1.db --node 1
synod serve n1.db --listen 127.0.0.1:7001 --peers 1=127.0.0.1:7001 --certs certs
```

`certs/admin.pem` and `certs/admin.key` are a client certificate; `--clients app,report` makes more. Certificates made before this release have no Authority Key Identifier, which Python 3.13 and later require; make them again for those clients.

```sh
api() { curl -sS --cacert certs/ca.pem --cert certs/admin.pem --key certs/admin.key \
             -H 'content-type: application/json' "https://127.0.0.1:7001$@"; }
```

## Endpoints

| | |
| --- | --- |
| `GET /v1/status` | node ID, epochs, roots, tables |
| `GET /v1/tables` | each table: name, DDL, row ID column, columns with type and merge policy |
| `POST /v1/tables` | create a table: `{"ddl": "CREATE TABLE ..."}` |
| `POST /v1/query` | run a read-only SQL statement |
| `POST /v1/write` | inserts, updates, and deletes in one transaction |
| `GET /v1/conflicts` | open conflicts (`?all=true`: also resolved ones) |
| `POST /v1/conflicts/<id>/resolve` | mark a conflict resolved |

### Create a table

```sh
api /v1/tables -d '{"ddl": "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, qty INTEGER MERGE delta, tags SET<TEXT> MERGE union)"}'
# 201 {"name":"stock","ddl":"..."}
```

### Write

```sh
api /v1/write -d '{"ops": [
  {"insert": {"table": "stock", "values": {"sku": "A-100", "qty": 800, "tags": ["red"]}}},
  {"update": {"table": "stock", "where": "sku = ?1", "params": ["B-200"], "values": {"qty": 5}}},
  {"delete": {"table": "stock", "row_id": "00000001-0000000000000007"}}
]}'
# {"results":[{"row_id":"00000001-0000000000000008"},{"row_ids":[...]},{"row_ids":["00000001-0000000000000007"]}],
#  "epoch":5973055767,"hlc":"1791916730257.0"}
```

Updates and deletes take a `row_id`, or a `where` condition with `params`. `ops` must not be empty. If one op fails, nothing is written, and the error gives the op's index in `op`.

### Query

```sh
api /v1/query -d '{"sql": "SELECT row_id, sku, qty, tags FROM stock WHERE qty > ?1", "params": [100]}'
# {"columns":[{"name":"row_id","type":"ROWID"},{"name":"sku","type":"TEXT"},
#             {"name":"qty","type":"INTEGER"},{"name":"tags","type":"SET<TEXT>"}],
#  "rows":[["00000001-0000000000000008","A-100",800,["red"]]]}
```

A query may only read: transaction statements, `ATTACH`, and `PRAGMA` are refused. It stops after 5 seconds, or at 64 MiB of results, with error `limit` (`synod serve --query-ms <ms> --result-mb <MiB>`; 0 for no limit). Requests run one at a time on the node's thread, so keep queries short.

A column's `type` is the type of the table column it comes from, or `null` for an expression. Add `"ints_as_strings": true` to get integers as strings (JavaScript numbers lose digits above 2^53).

### Conflicts

```sh
api /v1/conflicts
# {"conflicts":[{"id":"ffffffff-...","kind":"unique_key","table":"stock","column":null,
#                "row_id":"...","rule":"...","base":null,"applied":[...],
#                "recorded":[{"node":2,"hlc":"...","values":["B-200",7]}],"status":"open"}]}
api /v1/conflicts/ffffffff-.../resolve -X POST
```

## Values

| Type | JSON |
| --- | --- |
| `INTEGER` | number (or a string of digits) |
| `REAL` | number; NaN and infinities come back as `"NaN"`, `"inf"`, `"-inf"` |
| `DECIMAL(p,s)` | string, such as `"9.5"` (a number is also taken) |
| `TEXT` | string |
| `BLOB` | `{"base64": "AAEC"}` |
| `SET<T>` | array |
| row ID | string, such as `"0000000a-0000000000000003"` |
| NULL | `null` |

## Errors

```json
{"error": {"code": "constraint", "message": "constraint: ...", "op": 1}}
```

| Status | Codes |
| --- | --- |
| 400 | `bad_request`, `invalid`, `schema`, `sql`, `limit` |
| 404 | `not_found` |
| 405 | `method_not_allowed` |
| 413 | `too_large` |
| 409 | `constraint` |
| 503 | `clock`, `unavailable` |
| 500 | `internal` |

## From other languages

Python, with the standard library only:

```python
import http.client, json, ssl

ctx = ssl.create_default_context(cafile="certs/ca.pem")
ctx.load_cert_chain("certs/admin.pem", "certs/admin.key")
conn = http.client.HTTPSConnection("127.0.0.1", 7001, context=ctx)
conn.request("POST", "/v1/query", json.dumps({"sql": "SELECT sku, qty FROM stock"}),
             {"content-type": "application/json"})
print(json.loads(conn.getresponse().read()))
```

Node.js:

```js
const https = require("https"), fs = require("fs");
const req = https.request({ host: "127.0.0.1", port: 7001, path: "/v1/query", method: "POST",
  ca: fs.readFileSync("certs/ca.pem"), cert: fs.readFileSync("certs/admin.pem"),
  key: fs.readFileSync("certs/admin.key"), headers: { "content-type": "application/json" } },
  res => res.on("data", d => process.stdout.write(d)));
req.end(JSON.stringify({ sql: "SELECT sku, qty FROM stock", ints_as_strings: true }));
```

Go uses `tls.LoadX509KeyPair` and `x509.NewCertPool` in an `http.Transport`; Java and C# load the same files into a key store. `crates/synod-cli/tests/api.rs` runs the API with `curl` over HTTP/1.1 and HTTP/2, and `api_client.py` with the Python standard library.
