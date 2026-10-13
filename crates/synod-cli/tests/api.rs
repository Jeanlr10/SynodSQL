//! The client API (`/v1/`), used the way any language would use it: with
//! `curl`, over HTTP/1.1 and HTTP/2, with the cluster's client certificate.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

struct Node {
    dir: PathBuf,
    child: Child,
    port: u16,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn synod(dir: &Path, args: &[&str]) -> String {
    synod_at(dir, None, args)
}

// The synod command, at time `now` (ms) if given.
fn synod_at(dir: &Path, now: Option<u64>, args: &[&str]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_synod"));
    if let Some(now) = now {
        cmd.env("SYNOD_NOW_MS", now.to_string());
    }
    let out = cmd.args(args).current_dir(dir).output().unwrap();
    assert!(out.status.success(), "synod {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8(out.stdout).unwrap()
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("synod-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

impl Node {
    // Serves `db` of node `id` in `dir`, alone, with certificates that name
    // 127.0.0.1.
    fn serve(dir: PathBuf, db: &str, id: u32) -> Node {
        let hosts = format!("{id}=127.0.0.1,{id}=localhost");
        synod(&dir, &["certs", "certs", "--nodes", &id.to_string(), "--hosts", &hosts]);
        let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let listen = format!("127.0.0.1:{port}");
        let peers = format!("{id}={listen}");
        let child = Command::new(env!("CARGO_BIN_EXE_synod"))
            .args(["serve", db, "--listen", &listen, "--peers", &peers, "--certs", "certs"])
            .current_dir(&dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let node = Node { dir, child, port };
        let end = Instant::now() + Duration::from_secs(20);
        while node.try_call("GET", "/v1/status", None, "--http1.1").is_err() {
            assert!(Instant::now() < end, "the node does not answer");
            std::thread::sleep(Duration::from_millis(50));
        }
        node
    }

    // (HTTP status, JSON body) of a request with curl.
    fn try_call(&self, method: &str, path: &str, body: Option<&Value>, http: &str) -> Result<(u16, Value), String> {
        let c = |f: &str| self.dir.join("certs").join(f).to_str().unwrap().to_owned();
        let url = format!("https://127.0.0.1:{}{path}", self.port);
        let mut cmd = Command::new("curl");
        cmd.args(["-sS", http, "--cacert", &c("ca.pem"), "--cert", &c("admin.pem"), "--key", &c("admin.key")]);
        cmd.args(["-X", method, "-w", "\n%{http_code}", &url]);
        if let Some(b) = body {
            cmd.args(["-H", "content-type: application/json", "--data-binary", &b.to_string()]);
        }
        let out = cmd.output().map_err(|e| format!("curl: {e}"))?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).into_owned());
        }
        let text = String::from_utf8(out.stdout).unwrap();
        let (body, code) = text.rsplit_once('\n').unwrap();
        let json = if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(body).map_err(|e| format!("{e}: {body}"))?
        };
        Ok((code.parse().unwrap(), json))
    }

    fn call(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        let a = self.try_call(method, path, body.as_ref(), "--http1.1").unwrap();
        let b = self.try_call(method, path, body.as_ref(), "--http2");
        // Reads give the same answer over both; writes run once.
        if method == "GET" || path == "/v1/query" {
            assert_eq!(b.unwrap(), a, "{method} {path}: HTTP/2 differs");
        }
        a
    }
}

#[test]
fn any_http_client_uses_the_api() {
    if Command::new("curl").arg("--version").output().is_err() {
        panic!("this test needs curl");
    }
    let dir = temp_dir("api");
    synod(&dir, &["init", "n1.db", "--node", "1", "--epoch-ms", "300"]);
    let n = Node::serve(dir, "n1.db", 1);

    let (code, s) = n.call("GET", "/v1/status", None);
    assert_eq!(code, 200, "{s}");
    assert_eq!(s["node"], 1);
    assert_eq!(s["epoch_ms"], 300);

    let ddl = "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, price DECIMAL(10,2), qty INTEGER MERGE delta, \
               tags SET<TEXT> MERGE union, photo BLOB)";
    let (code, t) = n.call("POST", "/v1/tables", Some(json!({ "ddl": ddl })));
    assert_eq!(code, 201, "{t}");
    assert_eq!(t["name"], "stock");
    let (_, tables) = n.call("GET", "/v1/tables", None);
    let stock = tables["tables"].as_array().unwrap().iter().find(|t| t["name"] == "stock").unwrap().clone();
    assert_eq!(stock["row_id"], "row_id");
    assert_eq!(
        stock["columns"][3],
        json!({ "name": "tags", "type": "SET<TEXT>", "merge": "union", "not_null": false })
    );

    // One request, one transaction.
    let (code, w) = n.call(
        "POST",
        "/v1/write",
        Some(json!({ "ops": [
            { "insert": { "table": "stock", "values": {
                "sku": "A", "price": "9.50", "qty": 800, "tags": ["red", "blue"], "photo": { "base64": "AAEC" } } } },
            { "insert": { "table": "stock", "values": { "sku": "B", "price": 2.25, "qty": 5 } } },
        ] })),
    );
    assert_eq!(code, 200, "{w}");
    let a = w["results"][0]["row_id"].as_str().unwrap().to_owned();
    assert!(w["epoch"].as_u64().is_some());

    let (code, q) = n.call(
        "POST",
        "/v1/query",
        Some(json!({ "sql": "SELECT row_id, sku, price, qty, tags, photo, qty * 2 AS twice FROM stock WHERE qty > ?1 \
                             ORDER BY sku", "params": [100] })),
    );
    assert_eq!(code, 200, "{q}");
    assert_eq!(
        q["columns"],
        json!([
            { "name": "row_id", "type": "ROWID" }, { "name": "sku", "type": "TEXT" },
            { "name": "price", "type": "DECIMAL(10,2)" }, { "name": "qty", "type": "INTEGER" },
            { "name": "tags", "type": "SET<TEXT>" }, { "name": "photo", "type": "BLOB" },
            { "name": "twice", "type": null },
        ])
    );
    assert_eq!(q["rows"], json!([[a, "A", "9.5", 800, ["red", "blue"], { "base64": "AAEC" }, 1600]]));
    let (_, q) = n.call("POST", "/v1/query", Some(json!({ "sql": "SELECT qty FROM stock", "ints_as_strings": true })));
    assert_eq!(q["rows"][0][0], "800");

    // Update by condition, delete by ID.
    let (code, u) = n.call(
        "POST",
        "/v1/write",
        Some(json!({ "ops": [
            { "update": { "table": "stock", "where": "qty < ?1", "params": [10], "values": { "qty": 6 } } },
            { "delete": { "table": "stock", "row_id": a } },
        ] })),
    );
    assert_eq!(code, 200, "{u}");
    assert_eq!(u["results"][0]["row_ids"].as_array().unwrap().len(), 1);
    let (_, q) = n.call("POST", "/v1/query", Some(json!({ "sql": "SELECT sku, qty FROM stock" })));
    assert_eq!(q["rows"], json!([["B", 6]]));

    // A failing op rolls back the whole request, and says which op failed.
    let (code, e) = n.call(
        "POST",
        "/v1/write",
        Some(json!({ "ops": [
            { "insert": { "table": "stock", "values": { "sku": "C" } } },
            { "insert": { "table": "stock", "values": { "sku": "B" } } },
        ] })),
    );
    assert_eq!(
        (code, e["error"]["code"].as_str(), e["error"]["op"].as_u64()),
        (409, Some("constraint"), Some(1)),
        "{e}"
    );
    let (_, q) = n.call("POST", "/v1/query", Some(json!({ "sql": "SELECT count(*) FROM stock" })));
    assert_eq!(q["rows"], json!([[1]]));

    // Errors.
    let (code, e) = n.call(
        "POST",
        "/v1/write",
        Some(json!({ "ops": [{ "insert": { "table": "stock", "values": { "qty": "x" } } }] })),
    );
    assert_eq!((code, e["error"]["code"].as_str()), (400, Some("invalid")), "{e}");
    let (code, e) = n.call("POST", "/v1/query", Some(json!({ "sql": "DELETE FROM stock" })));
    assert_eq!((code, e["error"]["code"].as_str()), (400, Some("sql")), "{e}");
    let (code, e) = n.call("POST", "/v1/query", Some(json!({ "sql": "SELEC 1" })));
    assert_eq!((code, e["error"]["code"].as_str()), (400, Some("sql")), "{e}");
    let (code, e) = n.call("GET", "/v1/nothing", None);
    assert_eq!((code, e["error"]["code"].as_str()), (404, Some("not_found")));
    let (code, _) = n.call("GET", "/v1/query", None);
    assert_eq!(code, 405);
    let (code, e) = n.try_call("POST", "/v1/query", Some(&json!("not an object")), "--http1.1").unwrap();
    assert_eq!((code, e["error"]["code"].as_str()), (400, Some("bad_request")));

    let (code, c) = n.call("GET", "/v1/conflicts?all=true", None);
    assert_eq!((code, c["conflicts"].is_array()), (200, true), "{c}");
    let (code, _) = n.call("POST", "/v1/conflicts/00000001-0000000000000063/resolve", None);
    assert_eq!(code, 404);

    // The node merges by itself; the write shows in a merged epoch.
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        let (_, s) = n.call("GET", "/v1/status", None);
        if s["merged_epoch"].as_u64().is_some_and(|m| m >= w["epoch"].as_u64().unwrap()) {
            break;
        }
        assert!(Instant::now() < end, "{s}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn a_python_client_resolves_a_conflict() {
    // Two nodes insert the same unique key in one epoch, and sync by files;
    // then node 10 serves, and a client with only the Python standard
    // library lists the conflict and resolves it.
    let dir = temp_dir("api-py");
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let t = now / 60_000 * 60_000 - 180_000;
    let d = dir.as_path();
    synod_at(d, Some(t), &["init", "a.db", "--node", "10", "--genesis-ms", &t.to_string()]);
    synod_at(d, Some(t), &["create-table", "a.db", "CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER MERGE delta)"]);
    synod_at(d, Some(t + 1), &["insert", "a.db", "stock", "sku=A", "qty=800"]);
    synod_at(d, Some(t + 60_000), &["seal", "a.db"]);
    synod_at(d, Some(t + 60_000), &["merge", "a.db"]);
    synod_at(d, Some(t + 60_001), &["clone", "a.db", "b.db", "--node", "11"]);
    synod_at(d, Some(t + 60_010), &["insert", "a.db", "stock", "sku=B", "qty=5"]);
    synod_at(d, Some(t + 60_011), &["insert", "b.db", "stock", "sku=B", "qty=7"]);
    synod_at(d, Some(t + 120_000), &["seal", "b.db"]);
    synod_at(d, Some(t + 120_000), &["export", "b.db", "1", "b-1.log"]);
    synod_at(d, Some(t + 120_000), &["seal", "a.db"]);
    let m = synod_at(d, Some(t + 120_000), &["merge", "a.db", "b-1.log"]);
    assert!(m.contains("1 conflicts"), "{m}");

    let n = Node::serve(dir, "a.db", 10);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/api_client.py");
    let out = Command::new("python3")
        .args(["-I", script.to_str().unwrap(), &n.port.to_string(), n.dir.join("certs").to_str().unwrap()])
        .output()
        .expect("this test needs python3");
    assert!(out.status.success(), "{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
}
