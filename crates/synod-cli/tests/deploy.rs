//! Phase 3 gate 3, on one machine: three `synod serve` processes on
//! localhost, over HTTP/2 with mutual TLS, run the epoch cycle with no
//! manual work. A node that stops and starts again catches up.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Cluster {
    dir: PathBuf,
    ports: Vec<u16>,
    children: Vec<Option<Child>>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for c in self.children.iter_mut().flatten() {
            let _ = c.kill();
            let _ = c.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn synod(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new(env!("CARGO_BIN_EXE_synod")).args(args).current_dir(dir).output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if out.status.success() { Ok(text) } else { Err(format!("{text}{}", String::from_utf8_lossy(&out.stderr))) }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

impl Cluster {
    fn peers(&self) -> String {
        self.ports.iter().enumerate().map(|(i, p)| format!("{}=127.0.0.1:{p}", i + 1)).collect::<Vec<_>>().join(",")
    }

    fn start(&mut self, i: usize) {
        let db = format!("n{}.db", i + 1);
        let listen = format!("127.0.0.1:{}", self.ports[i]);
        let log = std::fs::File::create(self.dir.join(format!("n{}.out", i + 1))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_synod"))
            .args(["serve", &db, "--listen", &listen, "--peers", &self.peers(), "--certs", "certs"])
            .current_dir(&self.dir)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .stdin(Stdio::null())
            .spawn()
            .unwrap();
        self.children[i] = Some(child);
    }

    fn stop(&mut self, i: usize) {
        if let Some(mut c) = self.children[i].take() {
            c.kill().unwrap();
            c.wait().unwrap();
        }
    }

    fn remote(&self, i: usize, args: &[&str]) -> Result<String, String> {
        let addr = format!("127.0.0.1:{}", self.ports[i]);
        let to = (i + 1).to_string();
        let mut all = vec!["remote", &addr, "--to", &to, "--certs", "certs"];
        all.extend_from_slice(args);
        synod(&self.dir, &all)
    }

    // (merged epoch, head, root) of a node, if it answers.
    fn position(&self, i: usize) -> Option<(u64, String, String)> {
        let s = self.remote(i, &["net-status"]).ok()?;
        let field = |k: &str| s.lines().find_map(|l| l.strip_prefix(&format!("{k} "))).map(String::from);
        Some((field("merged_epoch")?.parse().ok()?, field("head")?, field("root")?))
    }

    // Waits until all nodes are on one merge at or after `epoch`.
    fn wait_agree(&self, epoch: u64, timeout: Duration) -> (u64, String, String) {
        let end = Instant::now() + timeout;
        loop {
            let pos: Vec<_> = (0..self.ports.len()).map(|i| self.position(i)).collect();
            if let Some(Some(first)) = pos.first()
                && first.0 >= epoch
                && pos.iter().all(|p| p.as_ref() == Some(first))
            {
                return first.clone();
            }
            assert!(Instant::now() < end, "no agreement at epoch {epoch}: {pos:?}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[test]
fn three_processes_run_the_epoch_cycle() {
    let dir = std::env::temp_dir().join(format!("synod-deploy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.as_path();
    synod(d, &["init", "n1.db", "--node", "1", "--epoch-ms", "500", "--lateness", "8"]).unwrap();
    synod(d, &["create-table", "n1.db", "CREATE TABLE stock (sku TEXT UNIQUE MERGE lww, qty INTEGER MERGE delta)"])
        .unwrap();
    synod(d, &["clone", "n1.db", "n2.db", "--node", "2"]).unwrap();
    synod(d, &["clone", "n1.db", "n3.db", "--node", "3"]).unwrap();
    synod(d, &["certs", "certs", "--nodes", "1,2,3"]).unwrap();
    let mut c =
        Cluster { dir: dir.clone(), ports: (0..3).map(|_| free_port()).collect(), children: vec![None, None, None] };
    for i in 0..3 {
        c.start(i);
    }
    let (e0, _, _) = c.wait_agree(0, Duration::from_secs(30));

    // Writes on two nodes; a sale on each node of the same item (delta).
    let a = c.remote(0, &["insert", "stock", "sku=A", "qty=10"]).unwrap();
    c.remote(1, &["insert", "stock", "sku=B", "qty=7"]).unwrap();
    let a = a.trim().to_owned();
    c.wait_agree(e0 + 3, Duration::from_secs(30));
    c.remote(0, &["update", "stock", &a, "qty=9"]).unwrap();
    c.remote(2, &["update", "stock", &a, "qty=8"]).unwrap();
    let (e1, _, _) = c.wait_agree(e0 + 6, Duration::from_secs(30));
    for i in 0..3 {
        let rows = c.remote(i, &["query", "SELECT sku, qty FROM stock ORDER BY sku"]).unwrap();
        assert!(rows.contains("A    7") && rows.contains("B    7"), "node {}: {rows}", i + 1);
    }

    // Node 3 stops for more than L = 8 epochs; the others go on, with
    // writes; node 3 starts again and catches up. Its last write could
    // not be merged in time: it is in `_conflicts` as a stale write.
    c.remote(2, &["insert", "stock", "sku=C", "qty=1"]).unwrap();
    std::thread::sleep(Duration::from_millis(100));
    c.stop(2);
    let end = Instant::now() + Duration::from_secs(30);
    while c.position(0).is_none_or(|p| p.0 < e1 + 12) {
        assert!(Instant::now() < end, "nodes 1 and 2 stopped merging without node 3");
        std::thread::sleep(Duration::from_millis(100));
    }
    c.remote(0, &["insert", "stock", "sku=D", "qty=2"]).unwrap();
    c.start(2);
    let (e2, head, root) = c.wait_agree(e1 + 14, Duration::from_secs(60));
    for i in 0..3 {
        let rows = c.remote(i, &["query", "SELECT sku FROM stock ORDER BY sku"]).unwrap();
        for sku in ["A", "B", "D"] {
            assert!(rows.lines().any(|l| l.trim() == sku), "node {} misses {sku}: {rows}", i + 1);
        }
        let conflicts = c.remote(i, &["conflicts"]).unwrap();
        let stale = conflicts.lines().find(|l| l.contains("stale_write")).unwrap_or_else(|| panic!("{conflicts}"));
        assert!(stale.contains("too_late") && stale.contains("C, 1"), "{stale}");
    }
    let metrics = c.remote(0, &["metrics"]).unwrap();
    assert!(metrics.contains("synod_merges{node=\"1\"}"));
    assert!(metrics.contains("synod_hash_mismatches{node=\"1\"} 0"));
    eprintln!("agreed at epoch {e2}: head {head}, root {root}");
}
