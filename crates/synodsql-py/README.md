# synodsql

Python bindings of [SynodSQL](https://github.com/Jeanlr10/SynodSQL), a git-style
replicated SQL database for a small group of trusted servers.

```python
import synodsql

with synodsql.Database.create("node.db", node=1) as db:
    db.create_table("CREATE TABLE stock (row_id ROWID, sku TEXT UNIQUE, qty INTEGER MERGE delta)")
    row = db.insert("stock", sku="A-100", qty=800)
    db.update("stock", row, qty=650)
    print(db.query("SELECT sku, qty FROM stock WHERE qty > ?1", [100]))
```

A node can sync by files (`seal`, `export`, `merge`), or run as a cluster node in
the background (`serve`) while the program goes on using it.
