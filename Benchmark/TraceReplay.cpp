// TraceReplay: miss ratio (and throughput) of the Furrballs eviction policies
// on real or synthetic traces, against exact LRU/FIFO and Belady OPT.
//
//   TraceReplay --synthetic zipf:0.99:1000000:10000000 --sizes 0.001,0.01,0.1
//   TraceReplay --trace twitter.oracleGeneral --format oracle --sizes 0.01,0.1
//   TraceReplay --trace keys.csv --format csv --key-column 1 --skip-header
//   TraceReplay --synthetic zipf:0.9:100000:5000000 --policies lru,arc,s3fifo --threads 8

#include "TraceSim.h"

#include <cstdlib>
#include <iostream>

namespace {

    void Usage() {
        std::cerr <<
            "Usage: TraceReplay (--trace PATH [--format oracle|csv|txt] | --synthetic SPEC) [options]\n"
            "\n"
            "Input:\n"
            "  --trace PATH          trace file\n"
            "  --format F            oracle: libCacheSim oracleGeneral binary (uncompressed)\n"
            "                        csv:    comma-separated, key in --key-column (default 1)\n"
            "                        txt:    one key per line (default for non-.oracleGeneral files)\n"
            "  --key-column N        0-based key column for csv (default 1)\n"
            "  --skip-header         skip the first line of a csv/txt trace\n"
            "  --synthetic SPEC      zipf:THETA:KEYS:REQUESTS\n"
            "                        scan:THETA:KEYS:REQUESTS:SCAN_FRACTION:SCAN_LENGTH\n"
            "  --max-requests N      truncate the trace\n"
            "  --seed N              seed for synthetic traces (default 1)\n"
            "\n"
            "Replay:\n"
            "  --sizes LIST          cache sizes; values < 1 are fractions of the unique keys\n"
            "                        (default 0.001,0.01,0.1)\n"
            "  --policies LIST       lru,arc,s3fifo,ref-lru,ref-fifo,opt (default: all)\n"
            "  --threads N           threads for lru/arc/s3fifo (default 1; others always 1)\n"
            "  --csv-out PATH        also write results as CSV\n";
    }

    std::vector<std::string> Split(const std::string& s, char sep) {
        std::vector<std::string> out;
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, sep))
            if (!item.empty()) out.push_back(item);
        return out;
    }

    TraceSim::Trace Synthetic(const std::string& spec, uint64_t seed) {
        auto f = Split(spec, ':');
        if (f.size() == 4 && f[0] == "zipf")
            return TraceSim::Zipf(std::stoull(f[2]), std::stoull(f[3]), std::stod(f[1]), seed);
        if (f.size() == 6 && f[0] == "scan")
            return TraceSim::ZipfWithScans(std::stoull(f[2]), std::stoull(f[3]), std::stod(f[1]),
                                           std::stod(f[4]), std::stoull(f[5]), seed);
        throw std::invalid_argument("bad --synthetic spec: " + spec);
    }

} // namespace

int main(int argc, char** argv) {
    std::string tracePath, format, synthetic, csvOut;
    std::string sizesArg = "0.001,0.01,0.1";
    std::vector<std::string> policies = TraceSim::PolicyNames();
    int keyColumn = 1;
    bool skipHeader = false;
    size_t maxRequests = SIZE_MAX;
    uint64_t seed = 1;
    unsigned threads = 1;

    try {
        for (int i = 1; i < argc; i++) {
            std::string a = argv[i];
            auto next = [&]() -> std::string {
                if (i + 1 >= argc) throw std::invalid_argument(a + " needs a value");
                return argv[++i];
            };
            if (a == "--trace") tracePath = next();
            else if (a == "--format") format = next();
            else if (a == "--key-column") keyColumn = std::stoi(next());
            else if (a == "--skip-header") skipHeader = true;
            else if (a == "--synthetic") synthetic = next();
            else if (a == "--max-requests") maxRequests = std::stoull(next());
            else if (a == "--seed") seed = std::stoull(next());
            else if (a == "--sizes") sizesArg = next();
            else if (a == "--policies") policies = Split(next(), ',');
            else if (a == "--threads") threads = static_cast<unsigned>(std::stoul(next()));
            else if (a == "--csv-out") csvOut = next();
            else if (a == "-h" || a == "--help") { Usage(); return 0; }
            else throw std::invalid_argument("unknown option " + a);
        }
        if (tracePath.empty() == synthetic.empty())
            throw std::invalid_argument("give exactly one of --trace or --synthetic");
        for (const auto& p : policies) {
            auto& names = TraceSim::PolicyNames();
            if (std::find(names.begin(), names.end(), p) == names.end())
                throw std::invalid_argument("unknown policy: " + p);
        }

        TraceSim::Trace trace;
        std::string source = synthetic;
        if (!tracePath.empty()) {
            source = tracePath;
            if (format.empty())
                format = tracePath.find(".oracleGeneral") != std::string::npos ? "oracle" : "txt";
            if (format == "oracle") trace = TraceSim::LoadOracleGeneral(tracePath, maxRequests);
            else if (format == "csv") trace = TraceSim::LoadText(tracePath, keyColumn, skipHeader, maxRequests);
            else if (format == "txt") trace = TraceSim::LoadText(tracePath, -1, skipHeader, maxRequests);
            else throw std::invalid_argument("unknown --format " + format);
        } else {
            trace = Synthetic(synthetic, seed);
            if (trace.size() > maxRequests) trace.resize(maxRequests);
        }
        if (trace.empty()) throw std::runtime_error("trace is empty");

        size_t unique = TraceSim::CountUnique(trace);
        std::printf("trace: %s\nrequests: %zu  unique keys: %zu  threads: %u\n\n", source.c_str(), trace.size(),
                    unique, threads);

        std::vector<size_t> sizes;
        for (const auto& s : Split(sizesArg, ',')) {
            double v = std::stod(s);
            sizes.push_back(v < 1.0 ? std::max<size_t>(1, static_cast<size_t>(v * double(unique)))
                                    : static_cast<size_t>(v));
        }

        FILE* csv = nullptr;
        if (!csvOut.empty()) {
            csv = std::fopen(csvOut.c_str(), "w");
            if (!csv) throw std::runtime_error("cannot write " + csvOut);
            std::fprintf(csv, "trace,policy,cache_size,threads,requests,misses,miss_ratio,failed_sets,mops\n");
        }

        std::printf("%-10s %12s %8s %12s %10s %10s\n", "policy", "cache_size", "threads", "miss_ratio", "Mops/s",
                    "failedSet");
        for (size_t cap : sizes) {
            for (const auto& p : policies) {
                unsigned t = TraceSim::IsConcurrentPolicy(p) ? threads : 1;
                TraceSim::ReplayResult r = TraceSim::Replay(p, trace, cap, t);
                std::printf("%-10s %12zu %8u %12.4f %10.2f %10zu\n", p.c_str(), cap, t, r.MissRatio(),
                            r.MopsPerSec(), r.failedSets);
                if (csv)
                    std::fprintf(csv, "%s,%s,%zu,%u,%zu,%zu,%.6f,%zu,%.3f\n", source.c_str(), p.c_str(), cap, t,
                                 r.requests, r.misses, r.MissRatio(), r.failedSets, r.MopsPerSec());
            }
            std::printf("\n");
        }
        if (csv) std::fclose(csv);
    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n\n";
        Usage();
        return 1;
    }
    return 0;
}
