// C++ smoke test of synod.hpp: smoke <dir>. Two nodes sync by files.
#include <chrono>
#include <iostream>
#include <thread>

#include "synod.hpp"

#define EXPECT(c)                                                                 \
    do {                                                                          \
        if (!(c)) {                                                               \
            std::cerr << __FILE__ << ":" << __LINE__ << ": expected " #c "\n";    \
            return 1;                                                             \
        }                                                                         \
    } while (0)

int main(int argc, char **argv) {
    if (argc != 2) return 2;
    std::string dir = argv[1];
    try {
        auto a = synod::Database::create(dir + "/a.db", 1, 100);
        a.create_table("CREATE TABLE stock (sku TEXT UNIQUE, qty INTEGER MERGE delta)");
        auto b = a.clone_to(dir + "/b.db", 2);
        auto id = a.insert("stock", {{"sku", "A"}, {"qty", "10"}});
        b.insert("stock", {{"sku", "B"}, {"qty", std::nullopt}});
        try {
            a.insert("stock", {{"nope", "1"}});
            EXPECT(false);
        } catch (const synod::Error &e) {
            EXPECT(std::string(e.what()).find("nope") != std::string::npos);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        auto ea = a.seal(), eb = b.seal();
        EXPECT(!ea.empty() && !eb.empty());
        std::vector<std::vector<uint8_t>> la, lb;
        for (auto e : ea) la.push_back(a.export_log(e));
        for (auto e : eb) lb.push_back(b.export_log(e));
        EXPECT(a.merge(lb) > 0 && b.merge(la) > 0);

        auto ra = a.query("SELECT sku, qty FROM stock ORDER BY sku");
        auto rb = b.query("SELECT sku, qty FROM stock ORDER BY sku");
        EXPECT(ra.size() == 2 && rb.size() == 2);
        EXPECT(ra.text(1, 0) == "B" && rb.text(0, 0) == "A" && rb.integer(0, 1) == 10 && ra.is_null(1, 1));
        a.update("stock", id, {{"qty", "9"}});
        EXPECT(a.query("SELECT qty FROM stock WHERE sku = ?1", {synod::Value("A")}).integer(0, 0) == 9);
        a.close();
    } catch (const synod::Error &e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
    std::cout << "C++: ok (libsynod " << synod::version() << ")\n";
    return 0;
}
