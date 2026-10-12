//! The offline file sync of the plan, from the command line: two nodes
//! export logs, swap the files, merge, and agree.

use std::path::{Path, PathBuf};
use std::process::Command;

const T: u64 = 1_791_763_200_000;

struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn synod(dir: &Path, now: u64, args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_synod"))
        .args(args)
        .current_dir(dir)
        .env("SYNOD_NOW_MS", now.to_string())
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(out.status.success(), "synod {args:?} failed: {}{text}", String::from_utf8_lossy(&out.stderr));
    text
}

fn root(merge_output: &str) -> &str {
    merge_output.trim().rsplit("root ").next().unwrap()
}

#[test]
fn two_nodes_sync_by_files() {
    let dir = Dir(std::env::temp_dir().join(format!("synod-cli-{}", std::process::id())));
    std::fs::create_dir_all(&dir.0).unwrap();
    let d = dir.0.as_path();
    let genesis = T.to_string();

    synod(d, T, &["init", "a.db", "--node", "10", "--genesis-ms", &genesis]);
    synod(
        d,
        T,
        &["create-table", "a.db", "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, qty INTEGER MERGE delta)"],
    );
    let r1 = synod(d, T + 1, &["insert", "a.db", "stock", "sku=A-100", "qty=800"]).trim().to_owned();
    synod(d, T + 60_000, &["seal", "a.db"]);
    synod(d, T + 60_000, &["merge", "a.db"]);
    synod(d, T + 60_001, &["clone", "a.db", "b.db", "--node", "11"]);

    // Two sales on two nodes in one epoch.
    synod(d, T + 60_010, &["update", "a.db", "stock", &r1, "qty=650"]);
    synod(d, T + 60_011, &["update", "b.db", "stock", &r1, "qty=700"]);
    let end = T + 120_000;
    for n in ["a", "b"] {
        synod(d, end, &["seal", &format!("{n}.db")]);
        synod(d, end, &["export", &format!("{n}.db"), "1", &format!("{n}-1.log")]);
    }
    let ma = synod(d, end, &["merge", "a.db", "b-1.log"]);
    let mb = synod(d, end, &["merge", "b.db", "a-1.log", "b-1.log"]);
    assert!(ma.contains("merged epoch 1: 2 logs, 0 conflicts"), "{ma}");
    assert_eq!(root(&ma), root(&mb));
    for db in ["a.db", "b.db"] {
        let q = synod(d, end, &["query", db, "SELECT sku, qty FROM stock"]);
        assert!(q.contains("A-100  550"), "{q}");
    }
    let status = synod(d, end, &["status", "b.db"]);
    assert!(status.contains(&format!("base root       {}", root(&ma))), "{status}");
}

#[test]
fn errors_are_reported() {
    let out =
        Command::new(env!("CARGO_BIN_EXE_synod")).args(["query", "/nonexistent/x.db", "SELECT 1"]).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("no store at"));
    let out = Command::new(env!("CARGO_BIN_EXE_synod")).arg("frobnicate").output().unwrap();
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown command"));
}
