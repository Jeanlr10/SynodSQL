"""A client of the SynodSQL API with the Python standard library only.

    python3 -I api_client.py <port> <certs dir>

Run by tests/api.rs against a node with one unique-key conflict: lists the
conflict, resolves it, and reads and writes rows. Exits non-zero on failure.
"""

import http.client
import json
import os
import ssl
import sys

port, certs = int(sys.argv[1]), sys.argv[2]
ctx = ssl.create_default_context(cafile=os.path.join(certs, "ca.pem"))
ctx.load_cert_chain(os.path.join(certs, "admin.pem"), os.path.join(certs, "admin.key"))
conn = http.client.HTTPSConnection("127.0.0.1", port, context=ctx)


def call(method, path, body=None):
    headers = {"content-type": "application/json"} if body is not None else {}
    conn.request(method, path, json.dumps(body) if body is not None else None, headers)
    r = conn.getresponse()
    return r.status, json.loads(r.read())


code, s = call("GET", "/v1/status")
assert code == 200 and s["node"] == 10, s

code, c = call("GET", "/v1/conflicts")
assert code == 200, c
[conflict] = c["conflicts"]
assert conflict["kind"] == "unique_key" and conflict["table"] == "stock", conflict
assert conflict["status"] == "open", conflict
assert [r["node"] for r in conflict["recorded"]] == [11], conflict
assert conflict["recorded"][0]["values"][:2] == ["B", 7], conflict

code, r = call("POST", "/v1/conflicts/%s/resolve" % conflict["id"])
assert (code, r) == (200, {"resolved": conflict["id"]}), r
assert call("GET", "/v1/conflicts")[1] == {"conflicts": []}
code, c = call("GET", "/v1/conflicts?all=true")
assert [x["status"] for x in c["conflicts"]] == ["resolved"], c

code, w = call("POST", "/v1/write", {"ops": [{"insert": {"table": "stock", "values": {"sku": "C", "qty": 2**60}}}]})
assert code == 200, w
code, q = call("POST", "/v1/query", {"sql": "SELECT sku, qty FROM stock ORDER BY sku", "ints_as_strings": True})
assert code == 200, q
assert q["rows"] == [["A", "800"], ["B", "5"], ["C", str(2**60)]], q
print("ok")
