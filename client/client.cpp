#include "httplib.h"
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <mutex>

using namespace std;
using namespace std::chrono;

std::atomic<uint64_t> global_key_counter{0};

// 1. Put-All

// Only create/delete â†’ database-heavy â†’ disk bottleneck.

// 2. Get-All

// Only reads with new keys â†’ cache miss every time â†’ database bottleneck.

// 3. Get-Popular

// Few keys accessed repeatedly â†’ cache hits â†’ CPU/memory bottleneck.

// 4. Random Mix (Get+Put)

// Mixed workload â†’ identifies combined behavior.

enum WorkloadType
{
    PUT_ALL,
    GET_ALL,
    GET_POPULAR,
    MIXED,
    DELETE_ALL
};

struct Config
{
    string server_url = "127.0.0.1";
    int port = 8080;
    int clients = 10;
    int duration_sec = 10;
    WorkloadType workload = GET_POPULAR;
    int keyspace = 1000;
    int popular = 10;
};

// ------------------- Load Generator ---------------------

int main(int argc, char *argv[])
{
    Config cfg;

    // ---- Simple argument parsing ----
    for (int i = 1; i < argc; i++)
    {
        // --- Warmup for GET_POPULAR: create popular_0..popular_{popular-1} ---

        string a = argv[i];
        if (a == "--url")
            cfg.server_url = argv[++i];
        else if (a == "--port")
            cfg.port = stoi(argv[++i]);
        else if (a == "--clients")
            cfg.clients = stoi(argv[++i]);
        else if (a == "--dur")
            cfg.duration_sec = stoi(argv[++i]);
        else if (a == "--keyspace")
            cfg.keyspace = stoi(argv[++i]);
        else if (a == "--popular")
            cfg.popular = stoi(argv[++i]);
        else if (a == "--workload")
        {
            string w = argv[++i];
            if (w == "put-all")
                cfg.workload = PUT_ALL;
            else if (w == "get-all")
                cfg.workload = GET_ALL;
            else if (w == "get-popular")
                cfg.workload = GET_POPULAR;
            else if (w == "delete-all")
                cfg.workload = DELETE_ALL;
            else
                cfg.workload = MIXED;
        }
    }

    if (cfg.workload == GET_POPULAR)
    {
        cout << "Warmup: inserting popular keys into server..." << endl;
        httplib::Client warm_cli(cfg.server_url, cfg.port);
        warm_cli.set_connection_timeout(5, 0);
        warm_cli.set_read_timeout(5, 0);

        for (int i = 0; i < cfg.popular; i++)
        {
            string key = "popular_" + to_string(i);
            string value = "popular_value_" + to_string(i);
            auto res = warm_cli.Put(("/kv/" + key).c_str(), value, "text/plain");
            if (!res || res->status < 200 || res->status >= 300)
            {
                cerr << "Warmup PUT failed for key " << key << endl;
            }
        }
        cout << "Warmup done.\n";
    }

    atomic<uint64_t> total_requests{0};
    atomic<uint64_t> success{0};
    atomic<uint64_t> failures{0};
    atomic<uint64_t> total_latency_ns{0};

    vector<thread> threads;
    threads.reserve(cfg.clients);

    auto end_time = steady_clock::now() + seconds(cfg.duration_sec);

    cout << "Starting load generator with "
         << cfg.clients << " clients for "
         << cfg.duration_sec << " seconds..." << endl;

    for (int c = 0; c < cfg.clients; c++)
    {
        threads.emplace_back([&, c]()
                             {
            httplib::Client cli(cfg.server_url, cfg.port);
            cli.set_connection_timeout(5, 0);
            cli.set_read_timeout(5, 0);

            std::mt19937_64 rng(std::random_device{}());
            std::uniform_int_distribution<int> dist(0, cfg.keyspace - 1);
            std::uniform_int_distribution<int> pop_dist(0, cfg.popular - 1);
            std::uniform_real_distribution<double> chance(0.0, 1.0);

            while (steady_clock::now() < end_time) {
                auto t0 = steady_clock::now();

                string key, value;
                httplib::Result res;
                double p = chance(rng);

                // ------------- Workload Logic -------------
                if (cfg.workload == PUT_ALL) {
                    // key = "k" + to_string(dist(rng));
                    // value = "v" + to_string(rng());
                    // res = cli.Put(("/kv/" + key).c_str(), value, "text/plain");
                     // sequential index, thread-safe
    uint64_t idx = global_key_counter.fetch_add(1);

    // If you want to wrap within keyspace:
    idx = idx % cfg.keyspace;

    key = "k" + to_string(idx);
    value = "v" + to_string(rng());  // value can still be random
    res = cli.Put(("/kv/" + key).c_str(), value, "text/plain");
                } else if (cfg.workload == GET_ALL) {
                    key = "k" + to_string(dist(rng));
                    res = cli.Get(("/kv/" + key).c_str());

                } else if (cfg.workload == GET_POPULAR) {
                    key = "popular_" + to_string(pop_dist(rng));
                    res = cli.Get(("/kv/" + key).c_str());

                }else if (cfg.workload == DELETE_ALL) {
    key = "k" + to_string(dist(rng));
    res = cli.Delete(("/kv/" + key).c_str());
}
 
                else {  // MIXED
                    if (p < 0.5) {   // GET
                        key = "k" + to_string(dist(rng));
                        res = cli.Get(("/kv/" + key).c_str());
                    }
                    else if (p < 0.8) { // PUT
                        key = "k" + to_string(dist(rng));
                        value = "v" + to_string(rng());
                        res = cli.Put(("/kv/" + key).c_str(), value, "text/plain");
                    }
                    else { // DELETE
                        key = "k" + to_string(dist(rng));
                        res = cli.Delete(("/kv/" + key).c_str());
                    }
                }

                auto elapsed = duration_cast<nanoseconds>(steady_clock::now() - t0).count();
                total_latency_ns += elapsed;
                total_requests++;

                if (res && res->status >= 200 && res->status < 300)
                    success++;
                else
                    failures++;
            } });
    }

    for (auto &t : threads)
        t.join();

    double duration = cfg.duration_sec;
    uint64_t succ = success.load();
    uint64_t req = total_requests.load();
    double throughput = succ / duration;
    // double avg_latency_ms = (double(total_latency_ns) / succ) / 1e6;
    double avg_latency_ms = succ > 0
                                ? (double(total_latency_ns) / succ) / 1e6
                                : 0.0;

    // -------------------- Results ------------------------
    cout << "\n===== RESULTS =====" << endl;
    cout << "Total Requests:      " << req << endl;
    cout << "Successful Requests: " << succ << endl;
    cout << "Failed Requests:     " << failures << endl;
    cout << "Throughput (req/s):  " << throughput << endl;
    cout << "Avg Latency (ms):    " << avg_latency_ms << endl;
    cout << "====================\n";

    return 0;
}