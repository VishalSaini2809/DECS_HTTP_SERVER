#include "httplib.h"
#include <iostream>
#include <unordered_map>
#include <list>
#include <mutex>
#include <atomic>
#include <pqxx/pqxx> // For libpqxx (C++ wrapper for libpq) - easier to use
// If you prefer raw libpq: #include <libpq-fe.h>

using namespace httplib;

std::atomic<uint64_t> cache_hits{0};
std::atomic<uint64_t> cache_misses{0};

// ------------------- LRU Cache --------------------
class LRUCache
{
public:
    LRUCache(size_t capacity) : cap(capacity) {}

    bool get(const std::string &key, std::string &value)
    {
        std::lock_guard<std::mutex> lock(mtx);

        auto it = map.find(key);
        if (it == map.end())
            return false;

        // Move to front (most recently used)
        cache.splice(cache.begin(), cache, it->second);
        value = it->second->second;
        return true;
    }

    void put(const std::string &key, const std::string &value)
    {
        std::lock_guard<std::mutex> lock(mtx);

        auto it = map.find(key);
        if (it != map.end())
        {
            // Update existing
            it->second->second = value;
            cache.splice(cache.begin(), cache, it->second);
            return;
        }

        // New insert
        cache.emplace_front(key, value);
        map[key] = cache.begin();

        if (cache.size() > cap)
        {
            auto last = cache.back().first;
            cache.pop_back();
            map.erase(last);
        }
    }

    void remove(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(mtx);

        auto it = map.find(key);
        if (it != map.end())
        {
            cache.erase(it->second);
            map.erase(it);
        }
    }

private:
    size_t cap;
    std::list<std::pair<std::string, std::string>> cache;
    std::unordered_map<std::string, decltype(cache.begin())> map;
    std::mutex mtx;
};

// ------------------- PostgreSQL DB Wrapper --------------------

class Database
{
public:
    std::string connStr;

    Database(const std::string &str) : connStr(str)
    {
        // Only use a temporary connection for setup
        pqxx::connection conn(connStr);
        pqxx::work w(conn);
        w.exec("CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT)");
        w.commit();
    }

    void put(const std::string &key, const std::string &value)
    {
        // pqxx::connection conn(connStr); // âœ… New connection per op
        thread_local pqxx::connection conn(connStr);
        pqxx::work w(conn);
        w.exec_params(
            "INSERT INTO kv(key,value) VALUES($1,$2) "
            "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            key, value);
        w.commit();
    }

    bool get(const std::string &key, std::string &value)
    {
        // pqxx::connection conn(connStr); // âœ… New connection per op
        thread_local pqxx::connection conn(connStr);
        pqxx::work w(conn);
        pqxx::result r = w.exec_params("SELECT value FROM kv WHERE key=$1", key);

        if (r.empty())
            return false;
        value = r[0]["value"].as<std::string>();
        return true;
    }

    void remove(const std::string &key)
    {
        // pqxx::connection conn(connStr); // âœ… New connection per op
        thread_local pqxx::connection conn(connStr);
        pqxx::work w(conn);
        w.exec_params("DELETE FROM kv WHERE key=$1", key);
        w.commit();
    }
};

// ------------------- MAIN SERVER --------------------

int main()
{
    Server svr;

    // Initialize DB + Cache
    Database db("dbname=kvdb user=kvuser password=kvpass host=127.0.0.1");
    LRUCache cache(1000);

    // PUT /kv/key
    svr.Put(R"(^/kv/([^/]+)$)", [&](const Request &req, Response &res)
            {
                std::string key = req.matches[1];
                std::string value = req.body; // raw value

                db.put(key, value);
                cache.put(key, value);

                res.set_content("PUT OK", "text/plain");
                // std::cout << "PUT /kv/" << key << " = " << value << std::endl;
            });

    // GET /kv/key
    svr.Get(R"(^/kv/(.+)$)", [&](const Request &req, Response &res)
            {
        std::string key = req.matches[1];
        std::string value;

        // Check cache
        if (cache.get(key, value)) {
            cache_hits++; 
            res.set_content("CACHE HIT: " + value, "text/plain");
            return;
        }

        cache_misses++; 
        // Fallback DB
        if (db.get(key, value)) {
            cache.put(key, value);
            res.set_content("DB HIT: " + value, "text/plain");
            return;
        }

        res.status = 404;
        res.set_content("Not found", "text/plain");
        std::cout << "GET /kv/" << key << std::endl; });

    // DELETE /kv/key
    svr.Delete(R"(^/kv/([^/]+)$)", [&](const Request &req, Response &res)
               {
                   std::string key = req.matches[1];

                   db.remove(key);
                   cache.remove(key);

                   res.set_content("DELETE OK", "text/plain");
                   std::cout << "DELETE /kv/" << key << std::endl; });

    // GET /stats  -> show cache stats
    svr.Get("/stats", [&](const Request &req, Response &res)
            {
    uint64_t h = cache_hits.load();
    uint64_t m = cache_misses.load();
    uint64_t total = h + m;
    double hit_rate = (total > 0) ? (double(h) * 100.0 / total) : 0.0;

    std::string body =
        "cache_hits=" + std::to_string(h) + "\n" +
        "cache_misses=" + std::to_string(m) + "\n" +
        "hit_rate=" + std::to_string(hit_rate) + "%\n";

    res.set_content(body, "text/plain"); });

    std::cout << "Server running on http://127.0.0.1:8080\n";
    svr.listen("0.0.0.0", 8080);
}