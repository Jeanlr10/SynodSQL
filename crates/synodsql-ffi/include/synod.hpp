// SynodSQL C++ API: a header-only wrapper of synod.h (C++17).
//
//   auto db = synod::Database::create("node.db", 1);
//   db.create_table("CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER MERGE delta)");
//   auto id = db.insert("stock", {{"sku", "A-100"}, {"qty", "800"}});
//   auto rows = db.query("SELECT sku, qty FROM stock WHERE qty > ?1", {synod::Value(100)});
//
// Errors throw synod::Error.
#ifndef SYNOD_HPP
#define SYNOD_HPP

#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "synod.h"

namespace synod {

class Error : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

namespace detail {
inline void check(int rc) {
    if (rc != SYNOD_OK) throw Error(synod_last_error());
}

inline std::string take(char *s) {
    std::string out = s ? s : "";
    synod_free_string(s);
    return out;
}

inline std::vector<const char *> c_strs(const std::vector<std::string> &v) {
    std::vector<const char *> out;
    out.reserve(v.size());
    for (auto &s : v) out.push_back(s.c_str());
    return out;
}
}  // namespace detail

enum class Type { Null = SYNOD_NULL, Integer = SYNOD_INTEGER, Real = SYNOD_REAL, Text = SYNOD_TEXT, Blob = SYNOD_BLOB };

// A query parameter.
class Value {
  public:
    Value() = default;
    Value(std::nullptr_t) {}
    Value(int i) : Value(static_cast<int64_t>(i)) {}
    Value(int64_t i) { v_.kind = SYNOD_INTEGER; v_.integer = i; }
    Value(double r) { v_.kind = SYNOD_REAL; v_.real = r; }
    Value(std::string text) : text_(std::move(text)) { v_.kind = SYNOD_TEXT; }
    Value(const char *text) : Value(std::string(text)) {}
    static Value blob(std::vector<uint8_t> bytes) {
        Value v;
        v.v_.kind = SYNOD_BLOB;
        v.blob_ = std::move(bytes);
        return v;
    }

    synod_value c() const {
        synod_value out = v_;
        if (v_.kind == SYNOD_TEXT) out.data = reinterpret_cast<const uint8_t *>(text_.c_str());
        if (v_.kind == SYNOD_BLOB) {
            out.data = blob_.data();
            out.len = blob_.size();
        }
        return out;
    }

  private:
    synod_value v_{SYNOD_NULL, 0, 0.0, nullptr, 0};
    std::string text_;
    std::vector<uint8_t> blob_;
};

// A query result.
class Rows {
  public:
    explicit Rows(synod_rows *r) : r_(r, synod_rows_free) {}
    size_t size() const { return synod_rows_count(r_.get()); }
    size_t columns() const { return synod_rows_columns(r_.get()); }
    std::string name(size_t col) const {
        const char *n = synod_rows_name(r_.get(), col);
        return n ? n : "";
    }
    Type type(size_t row, size_t col) const { return static_cast<Type>(synod_rows_type(r_.get(), row, col)); }
    bool is_null(size_t row, size_t col) const { return type(row, col) == Type::Null; }
    int64_t integer(size_t row, size_t col) const { return synod_rows_int(r_.get(), row, col); }
    double real(size_t row, size_t col) const { return synod_rows_real(r_.get(), row, col); }
    std::string text(size_t row, size_t col) const {
        const char *t = synod_rows_text(r_.get(), row, col);
        return t ? t : "";
    }
    std::vector<uint8_t> blob(size_t row, size_t col) const {
        size_t len = 0;
        const uint8_t *b = synod_rows_blob(r_.get(), row, col, &len);
        return b ? std::vector<uint8_t>(b, b + len) : std::vector<uint8_t>();
    }
    // As the command line shows it: row IDs and sets decoded.
    std::string display(size_t row, size_t col) const { return detail::take(synod_rows_display(r_.get(), row, col)); }

  private:
    std::unique_ptr<synod_rows, void (*)(synod_rows *)> r_;
};

// Column values as text, as the command line takes them; nullopt is NULL.
using Columns = std::vector<std::pair<std::string, std::optional<std::string>>>;

class Database {
  public:
    // epoch_ms 0 means 60000; lateness 0 means 16.
    static Database create(const std::string &path, uint32_t node, uint64_t epoch_ms = 0, uint64_t lateness = 0) {
        synod_db *db = nullptr;
        detail::check(synod_create(path.c_str(), node, epoch_ms, lateness, &db));
        return Database(db);
    }
    static Database open(const std::string &path) {
        synod_db *db = nullptr;
        detail::check(synod_open(path.c_str(), &db));
        return Database(db);
    }

    Database(Database &&o) noexcept : db_(std::exchange(o.db_, nullptr)) {}
    Database &operator=(Database &&o) noexcept {
        if (this != &o) {
            synod_close(db_);
            db_ = std::exchange(o.db_, nullptr);
        }
        return *this;
    }
    Database(const Database &) = delete;
    Database &operator=(const Database &) = delete;
    ~Database() { synod_close(db_); }

    // Stops the node if it serves, and closes the store.
    void close() {
        int rc = synod_close(std::exchange(db_, nullptr));
        detail::check(rc);
    }

    Database clone_to(const std::string &path, uint32_t node) {
        synod_db *db = nullptr;
        detail::check(synod_clone(db_, path.c_str(), node, &db));
        return Database(db);
    }

    void create_table(const std::string &ddl) { detail::check(synod_create_table(db_, ddl.c_str())); }

    std::string insert(const std::string &table, const Columns &columns) {
        auto [names, values] = split(columns);
        char *id = nullptr;
        detail::check(synod_insert(db_, table.c_str(), names.size(), names.data(), values.data(), &id));
        return detail::take(id);
    }

    void update(const std::string &table, const std::string &row_id, const Columns &columns) {
        auto [names, values] = split(columns);
        detail::check(synod_update(db_, table.c_str(), row_id.c_str(), names.size(), names.data(), values.data()));
    }

    void remove(const std::string &table, const std::string &row_id) {
        detail::check(synod_delete(db_, table.c_str(), row_id.c_str()));
    }

    Rows query(const std::string &sql, const std::vector<Value> &params = {}) {
        std::vector<synod_value> ps;
        for (auto &p : params) ps.push_back(p.c());
        synod_rows *r = nullptr;
        detail::check(synod_query(db_, sql.c_str(), ps.size(), ps.data(), &r));
        return Rows(r);
    }

    // A store command of the synod command line, without the store path.
    std::string exec(const std::vector<std::string> &args) {
        auto argv = detail::c_strs(args);
        char *out = nullptr;
        detail::check(synod_exec(db_, argv.size(), argv.data(), &out));
        return detail::take(out);
    }

    // Seals every epoch that has ended; returns the sealed epochs.
    std::vector<uint64_t> seal() {
        uint64_t first = 0;
        size_t n = 0;
        detail::check(synod_seal(db_, &first, &n));
        std::vector<uint64_t> out;
        for (size_t i = 0; i < n; i++) out.push_back(first + i);
        return out;
    }

    std::vector<uint8_t> export_log(uint64_t epoch) {
        uint8_t *data = nullptr;
        size_t len = 0;
        detail::check(synod_export(db_, epoch, &data, &len));
        std::vector<uint8_t> out(data, data + len);
        synod_free_bytes(data, len);
        return out;
    }

    size_t merge(const std::vector<std::vector<uint8_t>> &logs) {
        std::vector<const uint8_t *> ptrs;
        std::vector<size_t> lens;
        for (auto &l : logs) {
            ptrs.push_back(l.data());
            lens.push_back(l.size());
        }
        size_t n = 0;
        detail::check(synod_merge(db_, logs.size(), ptrs.data(), lens.data(), &n));
        return n;
    }

    // Runs the node in the background; peers as "id=host:port,...".
    void serve(const std::string &listen, const std::string &peers, const std::string &certs) {
        detail::check(synod_serve(db_, listen.c_str(), peers.c_str(), certs.c_str()));
    }

    synod_db *handle() const { return db_; }

  private:
    explicit Database(synod_db *db) : db_(db) {}

    static std::pair<std::vector<const char *>, std::vector<const char *>> split(const Columns &columns) {
        std::vector<const char *> names, values;
        for (auto &[n, v] : columns) {
            names.push_back(n.c_str());
            values.push_back(v ? v->c_str() : nullptr);
        }
        return {names, values};
    }

    synod_db *db_;
};

// hosts: "1=10.0.0.1,2=db2.example.com" adds names to node certificates.
inline void make_certs(const std::string &dir, const std::string &nodes, const std::string &clients = "admin",
                       const std::string &hosts = "") {
    detail::check(synod_make_certs(dir.c_str(), nodes.c_str(), clients.c_str(), hosts.c_str()));
}

// A store command on a running node, as a client of the cluster's CA.
inline std::string remote(const std::string &addr, uint32_t node, const std::string &certs,
                          const std::vector<std::string> &args, const std::string &name = "admin") {
    auto argv = detail::c_strs(args);
    char *out = nullptr;
    detail::check(synod_remote(addr.c_str(), node, certs.c_str(), name.c_str(), argv.size(), argv.data(), &out));
    return detail::take(out);
}

inline std::string version() { return synod_version(); }

}  // namespace synod

#endif  // SYNOD_HPP
