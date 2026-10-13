//! The library API: writes, queries, file sync between two nodes, and a
//! node that serves while the program goes on using it.

use std::time::{Duration, Instant};

use synodsql::{Cell, Database, Options, Param, ServeOptions};

fn dir(name: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("synodsql-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn p(d: &std::path::Path, f: &str) -> String {
    d.join(f).to_str().unwrap().to_owned()
}

const STOCK: &str = "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, price DECIMAL(10,2), qty INTEGER MERGE delta, tags SET<TEXT> MERGE union)";

#[test]
fn writes_and_queries() {
    let d = dir("writes");
    let mut db = Database::create(&p(&d, "a.db"), &Options::new(1)).unwrap();
    assert!(Database::create(&p(&d, "a.db"), &Options::new(1)).is_err());
    db.create_table(STOCK).unwrap();
    let id = db
        .insert(
            "stock",
            &[
                ("sku", Param::Text("A".into())),
                ("price", Param::Real(9.5)),
                ("qty", Param::Integer(800)),
                ("tags", Param::List(vec![Param::Text("x".into()), Param::Text("y".into())])),
            ],
        )
        .unwrap();
    let b = db.insert_text("stock", &[("sku", "B"), ("price", "1.25"), ("qty", "5"), ("tags", "z")]).unwrap();
    db.update("stock", &id, &[("qty", Param::Integer(650))]).unwrap();
    db.update_text("stock", &b, &[("qty", "NULL")]).unwrap();
    assert!(db.insert("stock", &[("qty", Param::Text("many".into()))]).unwrap_err().0.contains("qty"));

    let r = db.query("SELECT sku, price, qty, tags FROM stock ORDER BY sku", &[]).unwrap();
    assert_eq!(r.columns, ["sku", "price", "qty", "tags"]);
    assert_eq!(r.rows[0][0], Cell::Text("A".into()));
    assert_eq!(r.rows[0][1].display(), "9.5");
    assert_eq!(r.rows[0][2], Cell::Integer(650));
    assert_eq!(r.rows[0][3].display(), "{x, y}");
    assert_eq!(r.rows[1][2], Cell::Null);

    let r = db.query("SELECT row_id FROM stock WHERE sku = ?1", &[Param::Text("B".into())]).unwrap();
    assert_eq!(r.rows[0][0].display(), b);
    db.delete("stock", &b).unwrap();
    assert_eq!(db.query("SELECT count(*) FROM stock", &[]).unwrap().rows[0][0], Cell::Integer(1));
    assert!(db.exec(&["status"]).unwrap().lines().any(|l| l.starts_with("tables") && l.contains("stock")));
    assert!(db.exec(&["init"]).is_err());
    drop(db);

    let mut db = Database::open(&p(&d, "a.db")).unwrap();
    assert_eq!(db.node(), 1);
    assert_eq!(db.query("SELECT sku FROM stock", &[]).unwrap().rows, [[Cell::Text("A".into())]]);
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn two_nodes_sync_by_files() {
    let d = dir("files");
    let options = Options { epoch_ms: 100, ..Options::new(1) };
    let mut a = Database::create(&p(&d, "a.db"), &options).unwrap();
    a.create_table(STOCK).unwrap();
    let mut b = a.clone_to(&p(&d, "b.db"), 2).unwrap();
    let id = a.insert_text("stock", &[("sku", "A"), ("qty", "10")]).unwrap();
    b.insert_text("stock", &[("sku", "B"), ("qty", "7")]).unwrap();
    std::thread::sleep(Duration::from_millis(250));
    let (ea, eb) = (a.seal().unwrap(), b.seal().unwrap());
    assert!(!ea.is_empty() && !eb.is_empty());
    let la: Vec<_> = ea.iter().map(|e| a.export(*e).unwrap()).collect();
    let lb: Vec<_> = eb.iter().map(|e| b.export(*e).unwrap()).collect();
    assert!(!a.merge(&lb).unwrap().is_empty());
    assert!(!b.merge(&la).unwrap().is_empty());
    let sql = "SELECT sku, qty FROM stock ORDER BY sku";
    assert_eq!(a.query(sql, &[]).unwrap(), b.query(sql, &[]).unwrap());
    assert_eq!(a.query(sql, &[]).unwrap().rows.len(), 2);
    a.update_text("stock", &id, &[("qty", "9")]).unwrap();
    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn a_node_serves_and_takes_calls() {
    let d = dir("serve");
    let certs = p(&d, "certs");
    assert_eq!(synodsql::make_certs(&certs, &[1], &["admin"]).unwrap(), 2);
    let mut db = Database::create(&p(&d, "a.db"), &Options { epoch_ms: 200, ..Options::new(1) }).unwrap();
    db.create_table(STOCK).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{port}");
    db.serve(&ServeOptions::new(&addr, &[], &certs)).unwrap();
    assert!(db.is_serving());
    assert!(db.seal().is_err());
    let id = db.insert_text("stock", &[("sku", "A"), ("qty", "3")]).unwrap();
    assert_eq!(db.query("SELECT qty FROM stock", &[]).unwrap().rows, [[Cell::Integer(3)]]);

    // The node merges its epochs by itself.
    let end = Instant::now() + Duration::from_secs(20);
    loop {
        let status = synodsql::remote(&addr, 1, &certs, "admin", &["net-status"]).unwrap();
        if status.lines().any(|l| l.starts_with("merged_epoch ") && !l.ends_with("none")) {
            break;
        }
        assert!(Instant::now() < end, "{status}");
        std::thread::sleep(Duration::from_millis(50));
    }
    let out = synodsql::remote(&addr, 1, &certs, "admin", &["query", "SELECT sku, qty FROM stock"]).unwrap();
    assert!(out.contains("A    3"), "{out}");
    db.update_text("stock", &id, &[("qty", "4")]).unwrap();
    db.close().unwrap();
    assert!(!db.is_serving());
    assert!(db.query("SELECT 1", &[]).is_err());

    let mut db = Database::open(&p(&d, "a.db")).unwrap();
    assert_eq!(db.query("SELECT qty FROM stock", &[]).unwrap().rows, [[Cell::Integer(4)]]);
    std::fs::remove_dir_all(&d).ok();
}
