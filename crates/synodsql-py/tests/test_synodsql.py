"""Smoke tests of the synodsql module."""

import decimal
import socket
import time

import pytest

import synodsql

STOCK = (
    "CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, price DECIMAL(10,2),"
    " qty INTEGER MERGE delta, tags SET<TEXT> MERGE union)"
)


def test_writes_and_queries(tmp_path):
    path = str(tmp_path / "a.db")
    with synodsql.Database.create(path, node=1) as db:
        assert db.node == 1 and db.path == path
        db.create_table(STOCK)
        a = db.insert("stock", sku="A", price=decimal.Decimal("9.50"), qty=800, tags={"x", "y"})
        b = db.insert("stock", {"sku": "B", "qty": None, "price": 2})
        db.update("stock", a, qty=650)
        with pytest.raises(synodsql.Error, match="qty"):
            db.insert("stock", sku="C", qty="many")
        with pytest.raises(synodsql.Error):
            synodsql.Database.create(path, node=1)

        rows = db.query("SELECT sku, price, qty FROM stock WHERE qty > ?1 OR qty IS NULL ORDER BY sku", [100])
        assert rows == [("A", "9.5", 650), ("B", "2", None)]
        (row,) = db.query_dicts("SELECT row_id, sku FROM stock WHERE sku = ?1", ["B"])
        assert synodsql.row_id_str(row["row_id"]) == b
        db.delete("stock", row["row_id"])
        assert db.query("SELECT count(*) FROM stock") == [(1,)]
        assert "stock" in db.exec("status")

    with synodsql.Database.open(path) as db:
        assert db.query("SELECT sku FROM stock") == [("A",)]


def test_two_nodes_sync_by_files(tmp_path):
    a = synodsql.Database.create(str(tmp_path / "a.db"), node=1, epoch_ms=100)
    a.create_table(STOCK)
    b = a.clone_to(str(tmp_path / "b.db"), 2)
    a.insert("stock", sku="A", qty=10)
    b.insert("stock", sku="B", qty=7)
    time.sleep(0.25)
    ea, eb = a.seal(), b.seal()
    assert ea and eb
    assert a.merge([b.export(e) for e in eb])
    assert b.merge([a.export(e) for e in ea])
    sql = "SELECT sku, qty FROM stock ORDER BY sku"
    assert a.query(sql) == b.query(sql) == [("A", 10), ("B", 7)]
    a.close()
    b.close()


def test_a_node_serves(tmp_path):
    certs = str(tmp_path / "certs")
    assert synodsql.make_certs(certs, [1]) == 2
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        addr = f"127.0.0.1:{s.getsockname()[1]}"
    db = synodsql.Database.create(str(tmp_path / "a.db"), node=1, epoch_ms=200)
    db.create_table(STOCK)
    db.serve(addr, [], certs)
    assert db.serving
    db.insert("stock", sku="A", qty=3)
    end = time.time() + 20
    while "merged_epoch none" in synodsql.remote(addr, 1, certs, "net-status"):
        assert time.time() < end
        time.sleep(0.05)
    assert "A    3" in synodsql.remote(addr, 1, certs, "query", "SELECT sku, qty FROM stock")
    db.close()
    assert not db.serving
