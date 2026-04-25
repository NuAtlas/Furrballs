#include "Remarc.h"
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <deque>
#include <vector>
#include <random>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <cmath>

using Clock = std::chrono::high_resolution_clock;

static size_t zipfianSample(size_t n, double theta, std::mt19937_64& rng) {
    if (n <= 1) return 0;
    double alpha = 1.0 / (1.0 - theta);
    double zeta = 0.0;
    for (size_t i = 0; i < n; i++) zeta += 1.0 / std::pow((double)(i + 1), theta);
    double eta = (1.0 - std::pow(2.0 / (double)n, 1.0 - theta)) /
                 (1.0 - zeta + std::pow(2.0 / (double)n, 1.0 - theta) / (1.0 - 1.0 / (double)n));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    double u = dist(rng);
    double uz = u * zeta;
    if (uz < 1.0) return 0;
    if (uz < 1.0 + std::pow(0.5, theta)) return 1;
    return (size_t)((double)n * std::pow(eta * u - eta + 1.0, 1.0 / (1.0 - theta))) % n;
}

// ========== BareLRU ==========

class BareLRU {
    std::list<uint64_t> list_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> map_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;

public:
    explicit BareLRU(size_t cap) : cap_(cap) {}

    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            list_.splice(list_.begin(), list_, it->second);
            hits_++;
            return;
        }
        misses_++;
        if (list_.size() >= cap_) {
            map_.erase(list_.back());
            list_.pop_back();
            evictions_++;
        }
        list_.push_front(key);
        map_[key] = list_.begin();
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== BareARC ==========

class ArcList {
    std::list<uint64_t> list_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> idx_;

public:
    bool contains(uint64_t k) const { return idx_.count(k); }
    void push_front(uint64_t k) { list_.push_front(k); idx_[k] = list_.begin(); }
    void pop_back() { if (!list_.empty()) { idx_.erase(list_.back()); list_.pop_back(); } }
    void erase(uint64_t k) {
        auto i = idx_.find(k);
        if (i != idx_.end()) { list_.erase(i->second); idx_.erase(i); }
    }
    void splice_front(uint64_t k) {
        auto i = idx_.find(k);
        if (i != idx_.end()) list_.splice(list_.begin(), list_, i->second);
    }
    uint64_t back() const { return list_.back(); }
    size_t size() const { return list_.size(); }
    bool empty() const { return list_.empty(); }
};

class BareARC {
    std::unordered_set<uint64_t> cached_;
    ArcList t1_, t2_, b1_, b2_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;

    void replace(uint64_t key) {
        if (!t1_.empty() && (t1_.size() > p_ ||
            (b2_.contains(key) && t1_.size() == p_))) {
            uint64_t old = t1_.back();
            cached_.erase(old);
            t1_.pop_back();
            b1_.push_front(old);
        } else if (!t2_.empty()) {
            uint64_t old = t2_.back();
            cached_.erase(old);
            t2_.pop_back();
            b2_.push_front(old);
        }
        evictions_++;
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) {
                b1_.pop_back();
            } else if (!t1_.empty()) {
                cached_.erase(t1_.back());
                t1_.pop_back();
                evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) {
                b2_.pop_back();
            } else if (!t2_.empty()) {
                cached_.erase(t2_.back());
                t2_.pop_back();
                evictions_++;
            }
        }
    }

public:
    explicit BareARC(size_t cap) : cap_(cap), p_(0) {}

    void access(uint64_t key) {
        if (cached_.count(key)) {
            if (t1_.contains(key)) { t1_.erase(key); t2_.push_front(key); }
            else t2_.splice_front(key);
            hits_++;
            return;
        }
        misses_++;
        bool ghost = false;
        if (b1_.contains(key)) {
            size_t d = b1_.size() > 0
                ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            doEvict(); replace(key); b1_.erase(key);
            ghost = true;
        } else if (b2_.contains(key)) {
            size_t d = b2_.size() > 0
                ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            doEvict(); replace(key); b2_.erase(key);
            ghost = true;
        } else {
            doEvict();
        }
        cached_.insert(key);
        if (ghost) t2_.push_front(key);
        else t1_.push_front(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== BareREMARC ==========

class BareREMARC {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    NuAtlas::RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;
    size_t decayInterval_;

    void decayAll() {
        for (auto& pg : pages_) {
            for (auto& tc : pg.tempCtrl) {
                uint8_t sl = NuAtlas::UnpackSLocal(tc);
                uint8_t sr = NuAtlas::UnpackSRemote(tc);
                sl = NuAtlas::RemarcTimeDecay(sl, cfg_.TimeDecayNum, cfg_.TimeDecayDen);
                sr = NuAtlas::RemarcTimeDecay(sr, cfg_.TimeDecayNum, cfg_.TimeDecayDen);
                tc = NuAtlas::PackTempCtrl(sl, sr);
            }
        }
    }

    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.size() < kpp_) { next_ = p; return; }
        }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.size() < kpp_) { next_ = p; return; }
        }
    }

    void evictColdest() {
        scans_++;
        float best = -1.0f;
        size_t bi = SIZE_MAX;
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.empty()) continue;
            uint32_t en = 0;
            for (size_t o = 0; o < pages_[p].tempCtrl.size(); o += 32) {
                auto s = NuAtlas::RemarcScanBatch(
                    pages_[p].tempCtrl.data(), o,
                    pages_[p].tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float ep = NuAtlas::RemarcComputeEPage(en, pages_[p].tempCtrl.size());
            if (ep > best) { best = ep; bi = p; }
        }
        if (bi != SIZE_MAX) {
            for (uint64_t k : pages_[bi].keys) map_.erase(k);
            evictions_ += pages_[bi].keys.size();
            pages_[bi].keys.clear();
            pages_[bi].tempCtrl.clear();
        }
    }

public:
    BareREMARC(size_t capacity, size_t keysPerPage, const NuAtlas::RemarcConfig& cfg,
                size_t decayInterval = 64)
        : kpp_(keysPerPage), cfg_(cfg), decayInterval_(decayInterval) {
        maxPages_ = (capacity + kpp_ - 1) / kpp_;
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        if (++opCount_ % decayInterval_ == 0) decayAll();
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            pg.tempCtrl[it->second.second] = NuAtlas::RemarcUpdateLocal(
                pg.tempCtrl[it->second.second], cfg_);
            hits_++;
            return;
        }
        misses_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(NuAtlas::PackTempCtrl(NuAtlas::REMARC_MAX, 0));
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// ========== BareREMARC-RF (Recency + Frequency axes) ==========

class BareREMARC_RF {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    NuAtlas::RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInterval_;
    size_t freqDecayInterval_;

    static inline uint8_t RemarcUpdateRF(uint8_t tc, const NuAtlas::RemarcConfig& cfg) noexcept {
        uint8_t sl = NuAtlas::UnpackSLocal(tc);
        uint8_t sr = NuAtlas::UnpackSRemote(tc);
        sl = NuAtlas::RemarcBoost(sl, cfg.AlphaLocal);
        sr = NuAtlas::RemarcBoost(sr, cfg.AlphaRemote);
        return NuAtlas::PackTempCtrl(sl, sr);
    }

    void decayRecency() {
        for (auto& pg : pages_) {
            for (auto& tc : pg.tempCtrl) {
                uint8_t sl = NuAtlas::RemarcTimeDecay(
                    NuAtlas::UnpackSLocal(tc), cfg_.TimeDecayNum, cfg_.TimeDecayDen);
                tc = NuAtlas::PackTempCtrl(sl, NuAtlas::UnpackSRemote(tc));
            }
        }
    }

    void decayFrequency() {
        for (auto& pg : pages_) {
            for (auto& tc : pg.tempCtrl) {
                uint8_t sr = NuAtlas::RemarcTimeDecay(
                    NuAtlas::UnpackSRemote(tc), cfg_.TimeDecayNum, cfg_.TimeDecayDen);
                tc = NuAtlas::PackTempCtrl(NuAtlas::UnpackSLocal(tc), sr);
            }
        }
    }

    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.size() < kpp_) { next_ = p; return; }
        }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.size() < kpp_) { next_ = p; return; }
        }
    }

    void evictColdest() {
        scans_++;
        float best = -1.0f;
        size_t bi = SIZE_MAX;
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.empty()) continue;
            uint32_t en = 0;
            for (size_t o = 0; o < pages_[p].tempCtrl.size(); o += 32) {
                auto s = NuAtlas::RemarcScanBatch(
                    pages_[p].tempCtrl.data(), o,
                    pages_[p].tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float ep = NuAtlas::RemarcComputeEPage(en, pages_[p].tempCtrl.size());
            if (ep > best) { best = ep; bi = p; }
        }
        if (bi != SIZE_MAX) {
            for (uint64_t k : pages_[bi].keys) map_.erase(k);
            evictions_ += pages_[bi].keys.size();
            pages_[bi].keys.clear();
            pages_[bi].tempCtrl.clear();
        }
    }

public:
    BareREMARC_RF(size_t capacity, size_t keysPerPage, const NuAtlas::RemarcConfig& cfg,
                  size_t recDecayInterval = 64, size_t freqDecayInterval = 2048)
        : kpp_(keysPerPage), cfg_(cfg), 
          recDecayInterval_(recDecayInterval), freqDecayInterval_(freqDecayInterval) {
        maxPages_ = (capacity + kpp_ - 1) / kpp_;
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInterval_ == 0) decayRecency();
        if (opCount_ % freqDecayInterval_ == 0) decayFrequency();
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            pg.tempCtrl[it->second.second] = RemarcUpdateRF(
                pg.tempCtrl[it->second.second], cfg_);
            hits_++;
            return;
        }
        misses_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(NuAtlas::PackTempCtrl(0, 0));
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// ========== BareREMARC-RF per-key (pure policy, no page batching) ==========

class BareREMARC_RF_Key {
    struct Entry { uint8_t tc; };

    std::unordered_map<uint64_t, Entry> map_;
    NuAtlas::RemarcConfig cfg_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t sl = NuAtlas::RemarcTimeDecay(
                NuAtlas::UnpackSLocal(e.tc), cfg_.TimeDecayNum, cfg_.TimeDecayDen);
            e.tc = NuAtlas::PackTempCtrl(sl, NuAtlas::UnpackSRemote(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t sr = NuAtlas::RemarcTimeDecay(
                NuAtlas::UnpackSRemote(e.tc), cfg_.TimeDecayNum, cfg_.TimeDecayDen);
            e.tc = NuAtlas::PackTempCtrl(NuAtlas::UnpackSLocal(e.tc), sr);
        }
    }

    void evictColdest() {
        uint64_t worst = 0;
        uint8_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint8_t esc = NuAtlas::EvictLookup[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        if (worstE > 0 || !map_.empty()) {
            map_.erase(worst);
            evictions_++;
        }
    }

public:
    BareREMARC_RF_Key(size_t cap, const NuAtlas::RemarcConfig& cfg,
                      size_t recDecay = 64, size_t freqDecay = 2048)
        : cap_(cap), cfg_(cfg), recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t sl = NuAtlas::RemarcBoost(
                NuAtlas::UnpackSLocal(it->second.tc), cfg_.AlphaLocal);
            uint8_t sr = NuAtlas::RemarcBoost(
                NuAtlas::UnpackSRemote(it->second.tc), cfg_.AlphaRemote);
            it->second.tc = NuAtlas::PackTempCtrl(sl, sr);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) evictColdest();
        map_[key].tc = NuAtlas::PackTempCtrl(0, 0);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

namespace R8 {
    constexpr uint16_t MAX = 255;

    inline uint16_t Pack(uint8_t recency, uint8_t frequency) noexcept {
        return static_cast<uint16_t>((static_cast<uint16_t>(frequency) << 8) | recency);
    }
    inline uint8_t Recency(uint16_t tc) noexcept { return static_cast<uint8_t>(tc & 0xFF); }
    inline uint8_t Frequency(uint16_t tc) noexcept { return static_cast<uint8_t>(tc >> 8); }

    inline uint8_t Boost(uint8_t cur, uint8_t alpha) noexcept {
        uint32_t delta = static_cast<uint32_t>(alpha) * (MAX - cur);
        return static_cast<uint8_t>((delta + MAX / 2) / MAX);
    }

    inline uint8_t Decay(uint8_t cur, uint8_t num, uint8_t den) noexcept {
        uint32_t v = static_cast<uint32_t>(cur) * num;
        return static_cast<uint8_t>((v + den / 2) / den);
    }

    inline uint16_t ComputeEvict(uint8_t r, uint8_t f) noexcept {
        uint32_t e = (static_cast<uint32_t>(MAX - r)) * (2 * static_cast<uint32_t>(MAX) - f);
        return static_cast<uint16_t>(e / (2 * MAX));
    }

    struct Config {
        uint8_t alphaRec = 8;
        uint8_t alphaFreq = 4;
        uint8_t decayNum = 7;
        uint8_t decayDen = 8;
    };

    inline const std::vector<uint16_t>& GetEvictTable() {
        static std::vector<uint16_t> table;
        if (table.empty()) {
            table.resize(65536);
            for (uint32_t i = 0; i < 65536; i++) {
                table[i] = ComputeEvict(static_cast<uint8_t>(i & 0xFF),
                                        static_cast<uint8_t>(i >> 8));
            }
        }
        return table;
    }
}

class BareREMARC8_RF_Key {
    struct Entry { uint16_t tc; };

    std::unordered_map<uint64_t, Entry> map_;
    R8::Config cfg_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), cfg_.decayNum, cfg_.decayDen);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), cfg_.decayNum, cfg_.decayDen);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
    }

    void evictColdest() {
        const auto& lut = R8::GetEvictTable();
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint16_t esc = lut[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        if (worstE > 0 || !map_.empty()) {
            map_.erase(worst);
            evictions_++;
        }
    }

public:
    BareREMARC8_RF_Key(size_t cap, const R8::Config& cfg,
                       size_t recDecay = 64, size_t freqDecay = 2048)
        : cap_(cap), cfg_(cfg), recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, cfg_.alphaRec)), static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + R8::Boost(f, cfg_.alphaFreq)), static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) evictColdest();
        map_[key].tc = R8::Pack(0, 0);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== 8-bit REMARC RF per-key with ghost set ==========

class BareREMARC8_Ghost {
    struct Entry { uint16_t tc; };

    std::unordered_map<uint64_t, Entry> map_;
    std::unordered_map<uint64_t, uint16_t> ghost_;
    R8::Config cfg_;
    size_t cap_;
    size_t ghostCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), cfg_.decayNum, cfg_.decayDen);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), cfg_.decayNum, cfg_.decayDen);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
        for (auto& [k, tc] : ghost_) {
            uint8_t f = R8::Decay(R8::Frequency(tc), cfg_.decayNum, cfg_.decayDen);
            tc = R8::Pack(R8::Recency(tc), f);
        }
    }

    void evictColdest() {
        const auto& lut = R8::GetEvictTable();
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint16_t esc = lut[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        if (worstE > 0 || !map_.empty()) {
            auto it = map_.find(worst);
            if (it != map_.end()) {
                ghost_[worst] = it->second.tc;
                if (ghost_.size() > ghostCap_) {
                    uint64_t oldest = ghost_.begin()->first;
                    ghost_.erase(oldest);
                }
                map_.erase(it);
                evictions_++;
            }
        }
    }

public:
    BareREMARC8_Ghost(size_t cap, const R8::Config& cfg,
                      size_t recDecay = 64, size_t freqDecay = 2048)
        : cap_(cap), ghostCap_(cap), cfg_(cfg),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, cfg_.alphaRec)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + R8::Boost(f, cfg_.alphaFreq)),
                         static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) evictColdest();

        auto git = ghost_.find(key);
        uint16_t initTc;
        if (git != ghost_.end()) {
            uint8_t r = R8::Recency(git->second);
            uint8_t f = R8::Frequency(git->second);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, cfg_.alphaRec)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + R8::Boost(f, cfg_.alphaFreq)),
                         static_cast<uint16_t>(R8::MAX));
            initTc = R8::Pack(r, f);
            ghost_.erase(git);
        } else {
            uint8_t r = R8::Boost(0, cfg_.alphaRec);
            uint8_t f = R8::Boost(0, cfg_.alphaFreq);
            initTc = R8::Pack(r, f);
        }

        map_[key].tc = initTc;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== 8-bit REMARC with ghost-dormant phi re-insertion ==========

class BareREMARC8_Dormant {
    struct Entry { uint16_t tc; };

    std::unordered_map<uint64_t, Entry> map_;
    std::unordered_map<uint64_t, uint8_t> dormant_;
    uint8_t alphaRec_;
    uint8_t freqInc_;
    uint8_t decayNum_, decayDen_;
    size_t cap_;
    size_t dormantCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
        for (auto& [k, f] : dormant_) {
            f = R8::Decay(f, decayNum_, decayDen_);
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
    }

    void evictColdest() {
        const auto& lut = R8::GetEvictTable();
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint16_t esc = lut[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        if (worstE > 0 || !map_.empty()) {
            auto it = map_.find(worst);
            if (it != map_.end()) {
                dormant_[worst] = R8::Frequency(it->second.tc);
                if (dormant_.size() > dormantCap_) {
                    dormant_.erase(dormant_.begin());
                }
                map_.erase(it);
                evictions_++;
            }
        }
    }

public:
    BareREMARC8_Dormant(size_t cap, uint8_t alphaRec = 8, uint8_t freqInc = 2,
                        size_t recDecay = 64, size_t freqDecay = 2048)
        : alphaRec_(alphaRec), freqInc_(freqInc),
          decayNum_(7), decayDen_(8), cap_(cap), dormantCap_(cap),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, alphaRec_)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + freqInc_),
                         static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) evictColdest();

        uint8_t initR = R8::Boost(static_cast<uint8_t>(0), alphaRec_);
        uint8_t initF = 0;

        auto dit = dormant_.find(key);
        if (dit != dormant_.end()) {
            initF = std::min(static_cast<uint16_t>(dit->second + freqInc_),
                             static_cast<uint16_t>(R8::MAX));
            dormant_.erase(dit);
        }

        map_[key].tc = R8::Pack(initR, initF);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Poisson REMARC: λ = rate × e^(-Δt/τ), evict smallest λ ==========

class BareREMARC_Poisson {
    struct Entry { double rate; size_t lastAccess; };

    std::unordered_map<uint64_t, Entry> map_;
    double tau_;
    double alpha_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;

    double lambda(const Entry& e) const {
        double dt = static_cast<double>(opCount_ - e.lastAccess);
        return e.rate * std::exp(-dt / tau_);
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstL = std::numeric_limits<double>::max();
        for (auto& [k, e] : map_) {
            double l = lambda(e);
            if (l < worstL) { worstL = l; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_Poisson(size_t cap, double tau = 100.0, double alpha = 0.1)
        : tau_(tau), alpha_(alpha), cap_(cap) {}

    void access(uint64_t key) {
        ++opCount_;

        auto it = map_.find(key);
        if (it != map_.end()) {
            double dt = static_cast<double>(opCount_ - it->second.lastAccess);
            double instantRate = 1.0 / std::max(dt, 1.0);
            it->second.rate += alpha_ * (instantRate - it->second.rate);
            it->second.lastAccess = opCount_;
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {1.0 / static_cast<double>(tau_), opCount_};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Poisson REMARC with linear rate accumulation ==========

class BareREMARC_PoissonLin {
    struct Entry { double logRate; size_t lastAccess; };

    std::unordered_map<uint64_t, Entry> map_;
    double tau_;
    double rateInc_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;

    double logLambda(const Entry& e) const {
        double dt = static_cast<double>(opCount_ - e.lastAccess);
        return e.logRate - dt / tau_;
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstLL = std::numeric_limits<double>::max();
        for (auto& [k, e] : map_) {
            double ll = logLambda(e);
            if (ll < worstLL) { worstLL = ll; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_PoissonLin(size_t cap, double tau = 100.0, double rateInc = 0.05)
        : tau_(tau), rateInc_(rateInc), cap_(cap) {}

    void access(uint64_t key) {
        ++opCount_;

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.logRate += rateInc_;
            it->second.lastAccess = opCount_;
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {rateInc_, opCount_};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Hawkes REMARC: K kernels, phi=Boost+Decay, proj_s=ΣwₖSₖ ==========

class BareREMARC_Hawkes {
    static constexpr size_t K = 4;
    struct Entry { double s[K]; };

    std::unordered_map<uint64_t, Entry> map_;
    double alpha_[K];
    double decay_[K];
    double weights_[K];
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t decayInt_;

    void decayAll() {
        for (auto& [k, e] : map_) {
            for (size_t j = 0; j < K; j++)
                e.s[j] *= decay_[j];
        }
    }

    double intensity(const Entry& e) const {
        double lambda = 0.0;
        for (size_t j = 0; j < K; j++)
            lambda += weights_[j] * e.s[j];
        return lambda;
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstL = std::numeric_limits<double>::max();
        for (auto& [k, e] : map_) {
            double l = intensity(e);
            if (l < worstL) { worstL = l; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_Hawkes(size_t cap, size_t decayInt = 8)
        : cap_(cap), decayInt_(decayInt) {
        double taus[K] = {8.0, 64.0, 512.0, 4096.0};
        for (size_t j = 0; j < K; j++) {
            alpha_[j] = 1.0 / taus[j];
            decay_[j] = std::exp(-static_cast<double>(decayInt) / taus[j]);
            weights_[j] = taus[j] / 4096.0;
        }
    }

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % decayInt_ == 0) decayAll();

        auto it = map_.find(key);
        if (it != map_.end()) {
            for (size_t j = 0; j < K; j++)
                it->second.s[j] += alpha_[j];
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        Entry e{};
        for (size_t j = 0; j < K; j++)
            e.s[j] = alpha_[j];
        map_[key] = e;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Continuous REMARC (float, EMA bounded R,F ∈ [0,1]) ==========

class BareREMARC_Float {
    struct Entry { double r; double f; };

    std::unordered_map<uint64_t, Entry> map_;
    double alphaR_;
    double alphaF_;
    double decayRate_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) e.r *= decayRate_;
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) e.f *= decayRate_;
    }

    double computeEvict(const Entry& e) const {
        return (1.0 - e.r) * (1.0 - e.f);
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstE = -1.0;
        for (auto& [k, e] : map_) {
            double esc = computeEvict(e);
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_Float(size_t cap, double alphaR = 0.1, double alphaF = 0.02,
                     size_t recDecay = 64, size_t freqDecay = 2048)
        : alphaR_(alphaR), alphaF_(alphaF), decayRate_(7.0 / 8.0),
          cap_(cap), recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.r += alphaR_ * (1.0 - it->second.r);
            it->second.f += alphaF_ * (1.0 - it->second.f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {alphaR_, 0.0};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Continuous REMARC (float, EMA R ∈ [0,1], unbounded linear F) ==========

class BareREMARC_FloatLin {
    struct Entry { double r; double f; };

    std::unordered_map<uint64_t, Entry> map_;
    double alphaR_;
    double freqInc_;
    double decayRate_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) e.r *= decayRate_;
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) e.f *= decayRate_;
    }

    double computeEvict(const Entry& e) const {
        return (1.0 - e.r) / (1.0 + e.f);
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstE = -1.0;
        for (auto& [k, e] : map_) {
            double esc = computeEvict(e);
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_FloatLin(size_t cap, double alphaR = 0.1, double freqInc = 0.01,
                        size_t recDecay = 64, size_t freqDecay = 2048)
        : alphaR_(alphaR), freqInc_(freqInc), decayRate_(7.0 / 8.0),
          cap_(cap), recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.r += alphaR_ * (1.0 - it->second.r);
            it->second.f += freqInc_;
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {alphaR_, 0.0};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== 8-bit REMARC with linear frequency increment ==========

class BareREMARC8_LinFreq {
    struct Entry { uint16_t tc; };

    std::unordered_map<uint64_t, Entry> map_;
    uint8_t alphaRec_;
    uint8_t freqInc_;
    uint8_t decayNum_, decayDen_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
    }

    void evictColdest() {
        const auto& lut = R8::GetEvictTable();
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint16_t esc = lut[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        if (worstE > 0 || !map_.empty()) {
            map_.erase(worst);
            evictions_++;
        }
    }

public:
    BareREMARC8_LinFreq(size_t cap, uint8_t alphaRec = 8, uint8_t freqInc = 2,
                        size_t recDecay = 64, size_t freqDecay = 2048)
        : alphaRec_(alphaRec), freqInc_(freqInc),
          decayNum_(7), decayDen_(8), cap_(cap),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, alphaRec_)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + freqInc_),
                         static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) evictColdest();
        uint8_t r = R8::Boost(0, alphaRec_);
        map_[key].tc = R8::Pack(r, 0);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== 8-bit REMARC ghost (freq-only restore) + linear freq ==========

class BareREMARC8_GhostFreq {
    struct Entry { uint16_t tc; };

    std::unordered_map<uint64_t, Entry> map_;
    std::unordered_map<uint64_t, uint8_t> ghost_;
    uint8_t alphaRec_;
    uint8_t freqInc_;
    uint8_t decayNum_, decayDen_;
    size_t cap_;
    size_t ghostCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
        for (auto& [k, f] : ghost_) {
            f = R8::Decay(f, decayNum_, decayDen_);
        }
    }

    void evictColdest() {
        const auto& lut = R8::GetEvictTable();
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint16_t esc = lut[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        if (worstE > 0 || !map_.empty()) {
            auto it = map_.find(worst);
            if (it != map_.end()) {
                ghost_[worst] = R8::Frequency(it->second.tc);
                if (ghost_.size() > ghostCap_) {
                    uint64_t oldest = ghost_.begin()->first;
                    ghost_.erase(oldest);
                }
                map_.erase(it);
                evictions_++;
            }
        }
    }

public:
    BareREMARC8_GhostFreq(size_t cap, uint8_t alphaRec = 8, uint8_t freqInc = 2,
                          size_t recDecay = 64, size_t freqDecay = 2048)
        : alphaRec_(alphaRec), freqInc_(freqInc),
          decayNum_(7), decayDen_(8), cap_(cap), ghostCap_(cap),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, alphaRec_)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + freqInc_),
                         static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) evictColdest();

        uint8_t r = R8::Boost(0, alphaRec_);
        uint8_t f = 0;
        auto git = ghost_.find(key);
        if (git != ghost_.end()) {
            f = std::min(static_cast<uint16_t>(git->second + freqInc_),
                         static_cast<uint16_t>(R8::MAX));
            ghost_.erase(git);
        }
        map_[key].tc = R8::Pack(r, f);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== 8-bit REMARC additive formula + adaptive revival R ==========

class BareREMARC8_SymAdaptive {
    struct Entry { uint16_t tc; };
    struct GhostEntry { size_t evictOp; };

    std::unordered_map<uint64_t, Entry> map_;
    std::unordered_map<uint64_t, GhostEntry> ghost_;
    uint8_t alphaRec_;
    uint8_t freqInc_;
    uint8_t decayNum_, decayDen_;
    size_t cap_;
    size_t ghostCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;
    int p_;
    size_t returnWindow_;

    uint16_t computeEvict(uint8_t r, uint8_t f) const {
        uint32_t dR = static_cast<uint32_t>(R8::MAX - r);
        uint32_t dF = static_cast<uint32_t>(R8::MAX - f);
        uint32_t pU = static_cast<uint32_t>(std::clamp(p_, 0, static_cast<int>(cap_ * 2)));
        uint32_t wR = (2u * static_cast<uint32_t>(cap_) - pU) * dR;
        uint32_t wF = pU * dF;
        return static_cast<uint16_t>((wR + wF) / static_cast<uint32_t>(cap_));
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, ent] : map_) {
            uint16_t esc = computeEvict(R8::Recency(ent.tc), R8::Frequency(ent.tc));
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        return worst;
    }

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
    }

public:
    BareREMARC8_SymAdaptive(size_t cap, uint8_t alphaRec = 8, uint8_t freqInc = 2,
                            size_t recDecay = 64, size_t freqDecay = 2048)
        : alphaRec_(alphaRec), freqInc_(freqInc),
          decayNum_(7), decayDen_(8), cap_(cap), ghostCap_(cap),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay),
          p_(static_cast<int>(cap)), returnWindow_(cap) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, alphaRec_)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + freqInc_),
                         static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;

        uint8_t initR = R8::Boost(0, alphaRec_);
        auto git = ghost_.find(key);
        if (git != ghost_.end()) {
            size_t age = opCount_ - git->second.evictOp;
            if (age < returnWindow_) {
                double warmth = 1.0 - static_cast<double>(age) / static_cast<double>(returnWindow_);
                initR = static_cast<uint8_t>(static_cast<double>(R8::MAX) * warmth);
                initR = std::max(initR, R8::Boost(0, alphaRec_));
                p_ = std::min(p_ + 1, static_cast<int>(cap_ * 2));
            } else {
                p_ = std::max(p_ - 1, 0);
            }
            ghost_.erase(git);
        }

        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            auto vit = map_.find(victim);
            if (vit != map_.end()) {
                ghost_[victim] = {opCount_};
                if (ghost_.size() > ghostCap_) ghost_.erase(ghost_.begin());
                map_.erase(vit);
                evictions_++;
            }
        }

        map_[key].tc = R8::Pack(initR, 0);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    int pVal() const { return p_; }
};

// ========== REMARC + TinyLFU admission gate ==========

class BareREMARC8_TinyLFU {
    static constexpr size_t kSketchRows = 4;
    struct Entry { uint16_t tc; };

    std::vector<uint16_t> sketch_;
    size_t sketchMask_;
    std::unordered_map<uint64_t, Entry> map_;
    uint8_t alphaRec_;
    uint8_t freqInc_;
    uint8_t decayNum_, decayDen_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t admitted_ = 0, rejected_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_;
    size_t freqDecayInt_;
    size_t sketchDecayInt_;
    size_t totalAccesses_ = 0;
    size_t sketchResetThreshold_;

    void sketchAdd(uint64_t key) {
        for (size_t r = 0; r < kSketchRows; r++) {
            size_t h = (key * (r + 0x9e3779b97f4a7c15ULL + r)) & sketchMask_;
            size_t idx = r * (sketchMask_ + 1) + h;
            if (sketch_[idx] < 65535) sketch_[idx]++;
        }
        totalAccesses_++;
        if (totalAccesses_ >= sketchResetThreshold_) {
            for (auto& v : sketch_) v >>= 1;
            totalAccesses_ = 0;
        }
    }

    uint16_t sketchGet(uint64_t key) const {
        uint16_t mn = 65535;
        for (size_t r = 0; r < kSketchRows; r++) {
            size_t h = (key * (r + 0x9e3779b97f4a7c15ULL + r)) & sketchMask_;
            size_t idx = r * (sketchMask_ + 1) + h;
            mn = std::min(mn, sketch_[idx]);
        }
        return mn;
    }

    void decayRecency() {
        for (auto& [k, e] : map_) {
            uint8_t r = R8::Decay(R8::Recency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(r, R8::Frequency(e.tc));
        }
    }

    void decayFrequency() {
        for (auto& [k, e] : map_) {
            uint8_t f = R8::Decay(R8::Frequency(e.tc), decayNum_, decayDen_);
            e.tc = R8::Pack(R8::Recency(e.tc), f);
        }
    }

    uint64_t findVictim() {
        const auto& lut = R8::GetEvictTable();
        uint64_t worst = 0;
        uint16_t worstE = 0;
        for (auto& [k, e] : map_) {
            uint16_t esc = lut[e.tc];
            if (esc > worstE) { worstE = esc; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC8_TinyLFU(size_t cap, size_t sketchBits = 14,
                        uint8_t alphaRec = 8, uint8_t freqInc = 2,
                        size_t recDecay = 64, size_t freqDecay = 2048)
        : sketchMask_((1ULL << sketchBits) - 1),
          alphaRec_(alphaRec), freqInc_(freqInc),
          decayNum_(7), decayDen_(8), cap_(cap),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay),
          sketchDecayInt_(cap * 10),
          sketchResetThreshold_(cap * 10) {
        sketch_.resize(kSketchRows * (sketchMask_ + 1), 0);
    }

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) decayRecency();
        if (opCount_ % freqDecayInt_ == 0) decayFrequency();

        sketchAdd(key);

        auto it = map_.find(key);
        if (it != map_.end()) {
            uint8_t r = R8::Recency(it->second.tc);
            uint8_t f = R8::Frequency(it->second.tc);
            r = std::min(static_cast<uint16_t>(r + R8::Boost(r, alphaRec_)),
                         static_cast<uint16_t>(R8::MAX));
            f = std::min(static_cast<uint16_t>(f + freqInc_),
                         static_cast<uint16_t>(R8::MAX));
            it->second.tc = R8::Pack(r, f);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            uint16_t newFreq = sketchGet(key);
            uint16_t victimFreq = sketchGet(victim);
            if (newFreq >= victimFreq) {
                map_.erase(victim);
                evictions_++;
                uint8_t r = R8::Boost(0, alphaRec_);
                map_[key].tc = R8::Pack(r, 0);
                admitted_++;
            } else {
                rejected_++;
            }
        } else {
            uint8_t r = R8::Boost(0, alphaRec_);
            map_[key].tc = R8::Pack(r, 0);
            admitted_++;
        }
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Random Replacement baseline ==========

class BareRR {
    std::vector<uint64_t> keys_;
    std::unordered_map<uint64_t, size_t> idx_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    std::mt19937 rng_;

public:
    BareRR(size_t cap, uint64_t seed = 42) : cap_(cap), rng_(seed) {}

    void access(uint64_t key) {
        auto it = idx_.find(key);
        if (it != idx_.end()) { hits_++; return; }

        misses_++;
        if (keys_.size() >= cap_) {
            std::uniform_int_distribution<size_t> dist(0, keys_.size() - 1);
            size_t pos = dist(rng_);
            idx_.erase(keys_[pos]);
            keys_[pos] = key;
            idx_[key] = pos;
            evictions_++;
        } else {
            idx_[key] = keys_.size();
            keys_.push_back(key);
        }
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== LogFreq REMARC: R=EMA, F=log2(1+count), E=(1-R)/(1+logF) ==========

class BareREMARC_LogFreq {
    struct Entry { double R; double logF; };

    std::unordered_map<uint64_t, Entry> map_;
    double alphaR_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t decayInt_;
    double decayR_;

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstE = -1.0;
        for (auto& [k, e] : map_) {
            double E = (1.0 - e.R) / (1.0 + e.logF);
            if (E > worstE) { worstE = E; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_LogFreq(size_t cap, double alphaR = 0.5, size_t decayInt = 8)
        : alphaR_(alphaR), cap_(cap), decayInt_(decayInt),
          decayR_(std::pow(7.0/8.0, 1.0 / decayInt)) {}

    void access(uint64_t key) {
        ++opCount_;

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.R = it->second.R + alphaR_ * (1.0 - it->second.R);
            it->second.logF = std::log2(1.0 + std::pow(2.0, it->second.logF));
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {alphaR_, 0.0};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Adaptive-Weight REMARC: population-normalized additive ==========

class BareREMARC_Adaptive {
    struct Entry { double R, F; };

    std::unordered_map<uint64_t, Entry> map_;
    double alphaR_, alphaF_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t decayInt_;
    double decayR_, decayF_;

    uint64_t findVictim() {
        double sumR = 0, sumF = 0, n = 0;
        for (auto& [k, e] : map_) { sumR += e.R; sumF += e.F; n++; }
        if (n == 0) return 0;
        double meanR = sumR / n, meanF = sumF / n;
        double meanStaleness = (meanR < 0.999) ? (1.0 - meanR) : 0.001;

        uint64_t worst = 0;
        double worstE = -1e30;
        for (auto& [k, e] : map_) {
            double staleness = 1.0 - e.R;
            double E = staleness / meanStaleness + e.F / (meanF + 0.001);
            if (E > worstE) { worstE = E; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_Adaptive(size_t cap, double alphaR = 0.5, double alphaF = 0.0625,
                        size_t decayInt = 8)
        : alphaR_(alphaR), alphaF_(alphaF), cap_(cap), decayInt_(decayInt),
          decayR_(std::pow(7.0/8.0, 1.0 / decayInt)),
          decayF_(std::pow(7.0/8.0, 1.0 / decayInt)) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % decayInt_ == 0) {
            for (auto& [k, e] : map_) {
                e.R *= decayR_;
                e.F *= decayF_;
            }
        }

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.R += alphaR_ * (1.0 - it->second.R);
            it->second.F += alphaF_ * (1.0 - it->second.F);
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {alphaR_, alphaF_};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== CoopLog: log-freq + adaptive population weights ==========

class BareREMARC_CoopLog {
    struct Entry { double R; double logF; };

    std::unordered_map<uint64_t, Entry> map_;
    double alphaR_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t decayInt_;
    double decayR_;

    uint64_t findVictim() {
        double sumStale = 0, sumLogF = 0, n = 0;
        for (auto& [k, e] : map_) {
            sumStale += (1.0 - e.R);
            sumLogF += e.logF;
            n++;
        }
        if (n == 0) return 0;
        double meanStale = sumStale / n;
        double meanLogF = sumLogF / n;
        if (meanStale < 0.001) meanStale = 0.001;
        if (meanLogF < 0.001) meanLogF = 0.001;

        uint64_t worst = 0;
        double worstE = -1e30;
        for (auto& [k, e] : map_) {
            double stale = 1.0 - e.R;
            double E = stale / meanStale + e.logF / meanLogF;
            if (E > worstE) { worstE = E; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_CoopLog(size_t cap, double alphaR = 0.5, size_t decayInt = 8)
        : alphaR_(alphaR), cap_(cap), decayInt_(decayInt),
          decayR_(std::pow(7.0/8.0, 1.0 / decayInt)) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % decayInt_ == 0) {
            for (auto& [k, e] : map_) e.R *= decayR_;
        }

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.R += alphaR_ * (1.0 - it->second.R);
            it->second.logF = std::log2(1.0 + std::pow(2.0, it->second.logF));
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            map_.erase(victim);
            evictions_++;
        }

        map_[key] = {alphaR_, 0.0};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== LogFreq-Dormant: logF ghost restore + slow decay ==========

class BareREMARC_LogFreqDorm {
    struct Entry { double R; double logF; };

    std::unordered_map<uint64_t, Entry> map_;
    std::unordered_map<uint64_t, double> ghost_;
    double alphaR_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t decayInt_;
    double decayR_;
    double ghostDecay_;

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstE = -1.0;
        for (auto& [k, e] : map_) {
            double E = (1.0 - e.R) / (1.0 + e.logF);
            if (E > worstE) { worstE = E; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_LogFreqDorm(size_t cap, double alphaR = 0.5, size_t decayInt = 8,
                           double ghostDecayPer64 = 0.875)
        : alphaR_(alphaR), cap_(cap), decayInt_(decayInt),
          decayR_(std::pow(7.0/8.0, 1.0 / decayInt)),
          ghostDecay_(std::pow(ghostDecayPer64, 1.0 / decayInt)) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % decayInt_ == 0) {
            for (auto& [k, e] : map_) e.R *= decayR_;
            for (auto& [k, f] : ghost_) {
                f *= ghostDecay_;
                if (f < 0.1) f = 0.0;
            }
        }

        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.R += alphaR_ * (1.0 - it->second.R);
            it->second.logF = std::log2(1.0 + std::pow(2.0, it->second.logF));
            hits_++;
            return;
        }

        misses_++;
        if (map_.size() >= cap_) {
            uint64_t victim = findVictim();
            auto vit = map_.find(victim);
            ghost_[victim] = vit->second.logF;
            map_.erase(victim);
            evictions_++;
        }

        double initLogF = 0.0;
        auto git = ghost_.find(key);
        if (git != ghost_.end()) {
            initLogF = git->second;
            ghost_.erase(git);
        }

        map_[key] = {alphaR_, initLogF};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Factored REMARC: S_i + G (cache mean) + H (ghost mean) + Q (return rate) ==========

class BareREMARC_Factored {
    struct Entry { double R, F; };

    std::unordered_map<uint64_t, Entry> cache_;
    std::unordered_map<uint64_t, Entry> ghost_;
    std::list<uint64_t> ghostOrder_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> ghostIter_;

    double G_R_ = 0, G_F_ = 0;
    double H_R_ = 0, H_F_ = 0;
    double Q_ = 0;

    double alphaR_, alphaF_, alphaQ_;
    double decayR_, decayF_;
    size_t cap_, ghostCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t decayInt_;
    int mode_;

    double computeE(const Entry& e) const {
        double stale = 1.0 - e.R;
        double rare = 1.0 - e.F;
        double gd = std::abs(e.R - H_R_) + std::abs(e.F - H_F_);
        double prot = Q_ / (1.0 + 5.0 * gd);

        if (mode_ == 0) {
            double sn = stale / std::max(1.0 - G_R_, 0.02);
            double rn = rare / std::max(1.0 - G_F_, 0.02);
            return sn + rn - prot;
        } else if (mode_ == 1) {
            return (stale + rare) - prot;
        } else if (mode_ == 2) {
            double wR = 1.0 + 2.0 * Q_;
            double wF = 1.0 + 2.0 * (1.0 - Q_);
            return wR * stale + wF * rare;
        } else {
            return stale * rare - prot * rare;
        }
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstE = -1e30;
        for (auto& [k, e] : cache_) {
            double E = computeE(e);
            if (E > worstE) { worstE = E; worst = k; }
        }
        return worst;
    }

    void ghostInsert(uint64_t key, const Entry& e) {
        if (ghost_.size() >= ghostCap_) {
            uint64_t old = ghostOrder_.front();
            ghostOrder_.pop_front();
            auto gi = ghost_.find(old);
            if (gi != ghost_.end()) {
                double n = ghost_.size();
                if (n > 1) {
                    H_R_ += (H_R_ - gi->second.R) / (n - 1);
                    H_F_ += (H_F_ - gi->second.F) / (n - 1);
                } else { H_R_ = 0; H_F_ = 0; }
                ghost_.erase(gi);
            }
            ghostIter_.erase(old);
        }
        double n = ghost_.size();
        H_R_ = (n == 0) ? e.R : H_R_ + (e.R - H_R_) / (n + 1);
        H_F_ = (n == 0) ? e.F : H_F_ + (e.F - H_F_) / (n + 1);
        ghost_[key] = e;
        ghostOrder_.push_back(key);
        ghostIter_[key] = std::prev(ghostOrder_.end());
    }

    void ghostRemove(uint64_t key) {
        auto gi = ghost_.find(key);
        if (gi == ghost_.end()) return;
        double n = ghost_.size();
        if (n > 1) {
            H_R_ += (H_R_ - gi->second.R) / (n - 1);
            H_F_ += (H_F_ - gi->second.F) / (n - 1);
        } else { H_R_ = 0; H_F_ = 0; }
        ghost_.erase(gi);
        auto li = ghostIter_.find(key);
        if (li != ghostIter_.end()) {
            ghostOrder_.erase(li->second);
            ghostIter_.erase(li);
        }
    }

    void cacheInsert(uint64_t key, const Entry& e) {
        double n = cache_.size();
        G_R_ = (n == 0) ? e.R : G_R_ + (e.R - G_R_) / (n + 1);
        G_F_ = (n == 0) ? e.F : G_F_ + (e.F - G_F_) / (n + 1);
        cache_[key] = e;
    }

    void cacheRemove(uint64_t key) {
        auto ci = cache_.find(key);
        if (ci == cache_.end()) return;
        double n = cache_.size();
        if (n > 1) {
            G_R_ += (G_R_ - ci->second.R) / (n - 1);
            G_F_ += (G_F_ - ci->second.F) / (n - 1);
        } else { G_R_ = 0; G_F_ = 0; }
        cache_.erase(ci);
    }

public:
    BareREMARC_Factored(size_t cap, double alphaR = 0.5, double alphaF = 0.0625,
                        size_t decayInt = 8, double alphaQ = 0.05, size_t ghostCap = 0,
                        int mode = 0)
        : alphaR_(alphaR), alphaF_(alphaF), alphaQ_(alphaQ),
          decayR_(std::pow(7.0/8.0, 1.0/decayInt)),
          decayF_(std::pow(7.0/8.0, 1.0/decayInt)),
          cap_(cap), ghostCap_(ghostCap > 0 ? ghostCap : cap), decayInt_(decayInt),
          mode_(mode) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % decayInt_ == 0) {
            for (auto& [k, e] : cache_) { e.R *= decayR_; e.F *= decayF_; }
            G_R_ *= decayR_; G_F_ *= decayF_;
            for (auto& [k, e] : ghost_) { e.R *= decayR_; e.F *= decayF_; }
            H_R_ *= decayR_; H_F_ *= decayF_;
        }

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            double oR = it->second.R, oF = it->second.F;
            it->second.R += alphaR_ * (1.0 - it->second.R);
            it->second.F += alphaF_ * (1.0 - it->second.F);
            double n = cache_.size();
            if (n > 0) { G_R_ += (it->second.R - oR) / n; G_F_ += (it->second.F - oF) / n; }
            ++hits_;
            return;
        }

        ++misses_;

        if (cache_.size() >= cap_) {
            uint64_t victim = findVictim();
            auto vit = cache_.find(victim);
            Entry ve = vit->second;
            cacheRemove(victim);
            ghostInsert(victim, ve);
            ++evictions_;
        }

        auto gi = ghost_.find(key);
        if (gi != ghost_.end()) {
            Entry rest = gi->second;
            rest.R += alphaR_ * (1.0 - rest.R);
            rest.F += alphaF_ * (1.0 - rest.F);
            ghostRemove(key);
            Q_ += alphaQ_ * (1.0 - Q_);
            cacheInsert(key, rest);
        } else {
            Q_ *= (1.0 - alphaQ_);
            cacheInsert(key, {alphaR_, alphaF_});
        }
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Factored REMARC (Integer 8-bit) + Ghost + G/H/Q ==========

class BareREMARC_FactoredInt {
    struct Entry { uint16_t tc; };

    std::unordered_map<uint64_t, Entry> cache_;
    std::unordered_map<uint64_t, Entry> ghost_;
    std::list<uint64_t> ghostOrder_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> ghostIter_;

    double G_R_ = 0, G_F_ = 0;
    double H_R_ = 0, H_F_ = 0;
    double Q_ = 0;

    R8::Config cfg_;
    double alphaQ_;
    size_t cap_, ghostCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t recDecayInt_, freqDecayInt_;
    int mode_;

    double computeE(uint16_t tc) const {
        uint8_t r = R8::Recency(tc);
        uint8_t f = R8::Frequency(tc);
        double stale = static_cast<double>(R8::MAX - r) / R8::MAX;
        double rare = static_cast<double>(R8::MAX - f) / R8::MAX;
        double gd = std::abs(r / 255.0 - H_R_) + std::abs(f / 255.0 - H_F_);
        double prot = Q_ / (1.0 + 5.0 * gd);

        if (mode_ == 0) {
            return stale * rare - prot * rare;
        } else if (mode_ == 1) {
            double sn = stale / std::max(1.0 - G_R_, 0.02);
            double rn = rare / std::max(1.0 - G_F_, 0.02);
            return sn + rn - prot;
        } else {
            double wR = 1.0 + 2.0 * Q_;
            double wF = 1.0 + 2.0 * (1.0 - Q_);
            return wR * stale + wF * rare;
        }
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        double worstE = -1e30;
        for (auto& [k, e] : cache_) {
            double E = computeE(e.tc);
            if (E > worstE) { worstE = E; worst = k; }
        }
        return worst;
    }

    void updateG() {
        double sR = 0, sF = 0;
        for (auto& [k, e] : cache_) {
            sR += R8::Recency(e.tc);
            sF += R8::Frequency(e.tc);
        }
        G_R_ = sR / (255.0 * cache_.size());
        G_F_ = sF / (255.0 * cache_.size());
    }

    void updateH() {
        if (ghost_.empty()) { H_R_ = 0; H_F_ = 0; return; }
        double sR = 0, sF = 0;
        for (auto& [k, e] : ghost_) {
            sR += R8::Recency(e.tc);
            sF += R8::Frequency(e.tc);
        }
        H_R_ = sR / (255.0 * ghost_.size());
        H_F_ = sF / (255.0 * ghost_.size());
    }

public:
    BareREMARC_FactoredInt(size_t cap, size_t recDecay = 64, size_t freqDecay = 2048,
                           double alphaQ = 0.05, size_t ghostCap = 0, int mode = 0)
        : cfg_(), alphaQ_(alphaQ),
          cap_(cap), ghostCap_(ghostCap > 0 ? ghostCap : cap),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay), mode_(mode) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) {
            for (auto& [k, e] : cache_) {
                uint8_t r = R8::Decay(R8::Recency(e.tc), cfg_.decayNum, cfg_.decayDen);
                e.tc = R8::Pack(r, R8::Frequency(e.tc));
            }
        }
        if (opCount_ % freqDecayInt_ == 0) {
            for (auto& [k, e] : cache_) {
                uint8_t f = R8::Decay(R8::Frequency(e.tc), cfg_.decayNum, cfg_.decayDen);
                e.tc = R8::Pack(R8::Recency(e.tc), f);
            }
        }
        for (auto& [k, e] : ghost_) {
            if (opCount_ % recDecayInt_ == 0) {
                uint8_t r = R8::Decay(R8::Recency(e.tc), cfg_.decayNum, cfg_.decayDen);
                e.tc = R8::Pack(r, R8::Frequency(e.tc));
            }
            if (opCount_ % freqDecayInt_ == 0) {
                uint8_t f = R8::Decay(R8::Frequency(e.tc), cfg_.decayNum, cfg_.decayDen);
                e.tc = R8::Pack(R8::Recency(e.tc), f);
            }
        }

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            uint8_t r = R8::Boost(R8::Recency(it->second.tc), cfg_.alphaRec);
            uint8_t f = R8::Boost(R8::Frequency(it->second.tc), cfg_.alphaFreq);
            it->second.tc = R8::Pack(r, f);
            ++hits_;
            return;
        }

        ++misses_;

        if (cache_.size() >= cap_) {
            updateG();
            updateH();
            uint64_t victim = findVictim();
            auto vit = cache_.find(victim);

            if (ghost_.size() >= ghostCap_) {
                uint64_t old = ghostOrder_.front();
                ghostOrder_.pop_front();
                ghostIter_.erase(old);
                ghost_.erase(old);
            }
            ghost_[victim] = vit->second;
            ghostOrder_.push_back(victim);
            ghostIter_[victim] = std::prev(ghostOrder_.end());

            cache_.erase(vit);
            ++evictions_;
        }

        auto gi = ghost_.find(key);
        if (gi != ghost_.end()) {
            Entry restored = gi->second;
            uint8_t r = R8::Boost(R8::Recency(restored.tc), cfg_.alphaRec);
            uint8_t f = R8::Boost(R8::Frequency(restored.tc), cfg_.alphaFreq);
            restored.tc = R8::Pack(r, f);

            auto li = ghostIter_.find(key);
            if (li != ghostIter_.end()) ghostOrder_.erase(li->second);
            ghostIter_.erase(key);
            ghost_.erase(gi);

            Q_ += alphaQ_ * (1.0 - Q_);
            cache_[key] = restored;
        } else {
            Q_ *= (1.0 - alphaQ_);
            cache_[key] = {R8::Pack(0, 0)};
        }
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Log8 REMARC: logarithmic quantization, s ∈ {0..255}, v = 1 - 2^(-s/32) ==========

class BareREMARC_Log8 {
    struct Entry { uint8_t sR, sF; };

    std::unordered_map<uint64_t, Entry> cache_;
    std::unordered_map<uint64_t, Entry> ghost_;
    std::list<uint64_t> ghostOrder_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> ghostIter_;

    uint8_t boostR_, boostF_, decayR_, decayF_;
    size_t recDecayInt_, freqDecayInt_;
    size_t cap_, ghostCap_;
    bool useGhost_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;

    uint64_t findVictim() {
        uint64_t worst = 0;
        uint16_t worstSum = 65535;
        for (auto& [k, e] : cache_) {
            uint16_t sum = static_cast<uint16_t>(e.sR) + e.sF;
            if (sum < worstSum) { worstSum = sum; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_Log8(size_t cap, uint8_t boostR = 16, uint8_t boostF = 2,
                    uint8_t decayR = 1, uint8_t decayF = 1,
                    size_t recDecay = 8, size_t freqDecay = 256,
                    bool ghost = false, size_t ghostCap = 0)
        : boostR_(boostR), boostF_(boostF), decayR_(decayR), decayF_(decayF),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay),
          cap_(cap), ghostCap_(ghostCap > 0 ? ghostCap : cap), useGhost_(ghost) {}

    void access(uint64_t key) {
        ++opCount_;
        if (opCount_ % recDecayInt_ == 0) {
            for (auto& [k, e] : cache_)
                e.sR = (e.sR > decayR_) ? (e.sR - decayR_) : 0;
            for (auto& [k, e] : ghost_)
                e.sR = (e.sR > decayR_) ? (e.sR - decayR_) : 0;
        }
        if (opCount_ % freqDecayInt_ == 0) {
            for (auto& [k, e] : cache_)
                e.sF = (e.sF > decayF_) ? (e.sF - decayF_) : 0;
            for (auto& [k, e] : ghost_)
                e.sF = (e.sF > decayF_) ? (e.sF - decayF_) : 0;
        }

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second.sR = std::min<uint16_t>(255, it->second.sR + boostR_);
            it->second.sF = std::min<uint16_t>(255, it->second.sF + boostF_);
            ++hits_;
            return;
        }

        ++misses_;
        if (cache_.size() >= cap_) {
            uint64_t victim = findVictim();
            auto vit = cache_.find(victim);
            if (useGhost_) {
                if (ghost_.size() >= ghostCap_) {
                    uint64_t old = ghostOrder_.front();
                    ghostOrder_.pop_front();
                    ghostIter_.erase(old);
                    ghost_.erase(old);
                }
                ghost_[victim] = vit->second;
                ghostOrder_.push_back(victim);
                ghostIter_[victim] = std::prev(ghostOrder_.end());
            }
            cache_.erase(vit);
            ++evictions_;
        }

        if (useGhost_) {
            auto gi = ghost_.find(key);
            if (gi != ghost_.end()) {
                Entry rest = gi->second;
                rest.sR = std::min<uint16_t>(255, rest.sR + boostR_);
                rest.sF = std::min<uint16_t>(255, rest.sF + boostF_);
                auto li = ghostIter_.find(key);
                if (li != ghostIter_.end()) ghostOrder_.erase(li->second);
                ghostIter_.erase(key);
                ghost_.erase(gi);
                cache_[key] = rest;
                return;
            }
        }
        cache_[key] = {boostR_, boostF_};
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// ========== Tiered REMARC: capacity allocation as D_f ==========
//
// D is not fixed to {evict, retain}. Here D includes tier membership and
// capacity allocation controlled by global parameter p.
//
// Structure (mirrors ARC but with REMARC per-key scoring within tiers):
//   T_R (recency tier): fresh misses enter here. Target size = p*cap/256.
//   T_F (frequency tier): re-accessed keys and ghost-hit restorations.
//   Ghost carries tier identity for directional feedback.
//
// Feedback (competitive between tiers):
//   Ghost_R hit → p increases → T_R gets more capacity
//   Ghost_F hit → p decreases → T_F gets more capacity
//
// Within each tier, eviction uses REMARC score (sR + sF), not position.

class BareREMARC_Tiered {
    struct Entry { uint8_t sR, sF; bool freqTier; };
    struct GhostEntry { uint8_t sR, sF; bool wasFreqTier; };

    std::unordered_map<uint64_t, Entry> cache_;
    std::unordered_map<uint64_t, GhostEntry> ghost_;
    std::list<uint64_t> ghostOrder_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> ghostIter_;

    uint8_t boostR_, boostF_, decayR_, decayF_;
    size_t recDecayInt_, freqDecayInt_;
    size_t cap_, ghostCap_;
    int p_, pDelta_, pMin_, pMax_;
    bool useRatioAdj_;
    size_t tR_count_ = 0, tF_count_ = 0;
    size_t gR_count_ = 0, gF_count_ = 0;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;

    size_t targetR() const {
        return static_cast<size_t>(std::max(pMin_, std::min(pMax_, p_))) * cap_ / 256;
    }

    uint64_t findVictim(bool fromFreq) {
        uint64_t worst = 0;
        uint16_t worstScore = 65535;
        for (auto& [k, e] : cache_) {
            if (e.freqTier != fromFreq) continue;
            uint16_t score = static_cast<uint16_t>(e.sR) + e.sF;
            if (score < worstScore) { worstScore = score; worst = k; }
        }
        return worst;
    }

    void evictToGhost(uint64_t key) {
        auto it = cache_.find(key);
        if (it == cache_.end()) return;
        bool ft = it->second.freqTier;

        if (ghost_.size() >= ghostCap_) {
            uint64_t old = ghostOrder_.front();
            ghostOrder_.pop_front();
            ghostIter_.erase(old);
            auto og = ghost_.find(old);
            if (og != ghost_.end()) {
                if (og->second.wasFreqTier) gF_count_--; else gR_count_--;
                ghost_.erase(og);
            }
        }
        ghost_[key] = {it->second.sR, it->second.sF, ft};
        ghostOrder_.push_back(key);
        ghostIter_[key] = std::prev(ghostOrder_.end());
        if (ft) { gF_count_++; tF_count_--; } else { gR_count_++; tR_count_--; }
        cache_.erase(it);
        ++evictions_;
    }

public:
    BareREMARC_Tiered(size_t cap,
                      uint8_t boostR = 16, uint8_t boostF = 2,
                      uint8_t decayR = 1, uint8_t decayF = 1,
                      size_t recDecay = 8, size_t freqDecay = 256,
                      int pInit = 128, int pDelta = 4,
                      bool useRatioAdj = true,
                      int pMin = 32, int pMax = 224,
                      size_t ghostCap = 0)
        : boostR_(boostR), boostF_(boostF), decayR_(decayR), decayF_(decayF),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay),
          cap_(cap), ghostCap_(ghostCap > 0 ? ghostCap : cap),
          p_(pInit), pDelta_(pDelta), pMin_(pMin), pMax_(pMax), useRatioAdj_(useRatioAdj) {}

    void access(uint64_t key) {
        ++opCount_;

        if (opCount_ % recDecayInt_ == 0)
            for (auto& [k, e] : cache_) e.sR = (e.sR > decayR_) ? (e.sR - decayR_) : 0;
        if (opCount_ % freqDecayInt_ == 0)
            for (auto& [k, e] : cache_) e.sF = (e.sF > decayF_) ? (e.sF - decayF_) : 0;

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second.sR = std::min<uint16_t>(255, it->second.sR + boostR_);
            it->second.sF = std::min<uint16_t>(255, it->second.sF + boostF_);
            if (!it->second.freqTier) {
                it->second.freqTier = true;
                tR_count_--;
                tF_count_++;
            }
            ++hits_;
            return;
        }

        ++misses_;

        // Save ghost data before potential erasure
        GhostEntry ghostData{0, 0, false};
        auto gi = ghost_.find(key);
        bool ghostHit = (gi != ghost_.end());
        if (ghostHit) {
            ghostData = gi->second;

            int delta = pDelta_;
            if (useRatioAdj_) {
                if (ghostData.wasFreqTier) {
                    // Ghost_F hit: decrease p by |G_R|/|G_F| * pDelta (ARC formula)
                    int r = (gF_count_ > 0) ? static_cast<int>(gR_count_ / gF_count_) : 1;
                    delta = std::max(1, r * pDelta_);
                } else {
                    // Ghost_R hit: increase p by |G_F|/|G_R| * pDelta (ARC formula)
                    int r = (gR_count_ > 0) ? static_cast<int>(gF_count_ / gR_count_) : 1;
                    delta = std::max(1, r * pDelta_);
                }
            }
            if (ghostData.wasFreqTier) {
                p_ = std::max(pMin_, p_ - delta);
            } else {
                p_ = std::min(pMax_, p_ + delta);
            }
            if (ghostData.wasFreqTier) gF_count_--; else gR_count_--;
            auto li = ghostIter_.find(key);
            if (li != ghostIter_.end()) ghostOrder_.erase(li->second);
            ghostIter_.erase(key);
            ghost_.erase(gi);
        }

        if (cache_.size() >= cap_) {
            size_t tR = targetR();
            if (tR_count_ > tR && tR_count_ > 0) {
                evictToGhost(findVictim(false));
            } else if (tF_count_ > 0) {
                evictToGhost(findVictim(true));
            } else if (tR_count_ > 0) {
                evictToGhost(findVictim(false));
            }
        }

        if (ghostHit) {
            Entry rest;
            rest.sR = std::min<uint16_t>(255, static_cast<uint16_t>(ghostData.sR) + boostR_);
            rest.sF = std::min<uint16_t>(255, static_cast<uint16_t>(ghostData.sF) + boostF_);
            rest.freqTier = true;  // ghost hits always go to frequency tier (like ARC)
            cache_[key] = rest;
            tF_count_++;
        } else {
            cache_[key] = {boostR_, boostF_, false};  // fresh miss → recency tier
            tR_count_++;
        }
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    int p() const { return p_; }
};

//
// The three-atom decomposition says proj_s takes (S_i, G, H, Q) → R.
// This variant closes the feedback loop: Q modulates proj_s weights.
//
// Q dynamics (self-modulated axis):
//   Ghost hit (wrong eviction):  Q += (255-Q) >> shift   (adaptive: rare events have more impact)
//   Ghost miss (right eviction): Q -= (Q >> 4) + 1       (mean-reverting: high Q decays faster)
//   Periodic:                    Q -= (Q >> 4) + 1
//
// proj_s (Q-modulated scoring):
//   wR(Q) = wrBase * max(kFloor, 255-Q) / 255   (competitive: high Q suppresses recency)
//   wF(Q) = wfBase * max(kFloor, Q+1)   / 255   (cooperative: high Q boosts frequency)
//   score = wR(Q) * sR + wF(Q) * sF

class BareREMARC_Feedback {
    struct Entry { uint8_t sR, sF; };

    std::unordered_map<uint64_t, Entry> cache_;
    std::unordered_map<uint64_t, Entry> ghost_;
    std::list<uint64_t> ghostOrder_;
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> ghostIter_;

    uint8_t boostR_, boostF_, decayR_, decayF_;
    size_t recDecayInt_, freqDecayInt_;
    size_t cap_, ghostCap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;

    uint8_t Q_ = 0;
    size_t qDecayInt_;
    int qSelfModShift_;
    uint16_t wrBase_, wfBase_;
    int kFloor_;

    uint8_t adaptiveBoostQ() const {
        uint16_t boost = static_cast<uint16_t>(255 - Q_);
        if (qSelfModShift_ > 0) boost >>= qSelfModShift_;
        return static_cast<uint8_t>(std::min<uint16_t>(boost, static_cast<uint16_t>(255 - Q_)));
    }

    uint8_t selfModDecayQ() const {
        return static_cast<uint8_t>((Q_ >> 4) + 1);
    }

    uint32_t computeScore(uint8_t sR, uint8_t sF) const {
        uint32_t wR = (static_cast<uint32_t>(wrBase_) * std::max(kFloor_, static_cast<int>(255 - Q_))) / 255;
        uint32_t wF = (static_cast<uint32_t>(wfBase_) * std::max(kFloor_, static_cast<int>(Q_ + 1))) / 255;
        return wR * sR + wF * sF;
    }

    uint64_t findVictim() {
        uint64_t worst = 0;
        uint32_t worstScore = UINT32_MAX;
        for (auto& [k, e] : cache_) {
            uint32_t score = computeScore(e.sR, e.sF);
            if (score < worstScore) { worstScore = score; worst = k; }
        }
        return worst;
    }

public:
    BareREMARC_Feedback(size_t cap,
                        uint8_t boostR = 16, uint8_t boostF = 2,
                        uint8_t decayR = 1, uint8_t decayF = 1,
                        size_t recDecay = 8, size_t freqDecay = 256,
                        size_t qDecayInt = 64,
                        int qSelfModShift = 2,
                        uint16_t wrBase = 256, uint16_t wfBase = 256,
                        int kFloor = 1,
                        size_t ghostCap = 0)
        : boostR_(boostR), boostF_(boostF), decayR_(decayR), decayF_(decayF),
          recDecayInt_(recDecay), freqDecayInt_(freqDecay),
          cap_(cap), ghostCap_(ghostCap > 0 ? ghostCap : cap),
          qDecayInt_(qDecayInt), qSelfModShift_(qSelfModShift),
          wrBase_(wrBase), wfBase_(wfBase), kFloor_(kFloor) {}

    void access(uint64_t key) {
        ++opCount_;

        if (opCount_ % recDecayInt_ == 0)
            for (auto& [k, e] : cache_) e.sR = (e.sR > decayR_) ? (e.sR - decayR_) : 0;
        if (opCount_ % freqDecayInt_ == 0)
            for (auto& [k, e] : cache_) e.sF = (e.sF > decayF_) ? (e.sF - decayF_) : 0;

        if (opCount_ % qDecayInt_ == 0) {
            uint8_t step = selfModDecayQ();
            Q_ = (Q_ > step) ? (Q_ - step) : 0;
        }

        auto it = cache_.find(key);
        if (it != cache_.end()) {
            it->second.sR = std::min<uint16_t>(255, it->second.sR + boostR_);
            it->second.sF = std::min<uint16_t>(255, it->second.sF + boostF_);
            ++hits_;
            return;
        }

        ++misses_;
        auto gi = ghost_.find(key);
        bool ghostHit = (gi != ghost_.end());

        if (ghostHit) {
            Q_ = std::min<uint16_t>(255, static_cast<uint16_t>(Q_) + adaptiveBoostQ());
        }

        if (cache_.size() >= cap_) {
            uint64_t victim = findVictim();
            auto vit = cache_.find(victim);
            if (ghost_.size() >= ghostCap_) {
                uint64_t old = ghostOrder_.front();
                ghostOrder_.pop_front();
                ghostIter_.erase(old);
                ghost_.erase(old);
            }
            ghost_[victim] = vit->second;
            ghostOrder_.push_back(victim);
            ghostIter_[victim] = std::prev(ghostOrder_.end());
            cache_.erase(vit);
            ++evictions_;
        }

        if (ghostHit) {
            Entry rest = gi->second;
            rest.sR = std::min<uint16_t>(255, rest.sR + boostR_);
            rest.sF = std::min<uint16_t>(255, rest.sF + boostF_);
            auto li = ghostIter_.find(key);
            if (li != ghostIter_.end()) ghostOrder_.erase(li->second);
            ghostIter_.erase(key);
            ghost_.erase(gi);
            cache_[key] = rest;
        } else {
            cache_[key] = {boostR_, boostF_};
        }
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    uint8_t Q() const { return Q_; }
};

// ========== Helpers ==========

struct BenchResult {
    double hitRate;
    size_t hits, misses, evictions;
    double nsPerOp;
};

template <typename Cache>
BenchResult runBench(Cache& cache, const std::vector<uint64_t>& wl) {
    auto t0 = Clock::now();
    for (uint64_t k : wl) cache.access(k);
    auto t1 = Clock::now();
    size_t h = cache.hits(), m = cache.misses();
    return {
        (h + m > 0) ? 100.0 * h / (h + m) : 0.0,
        h, m, cache.evictions(),
        std::chrono::duration<double, std::nano>(t1 - t0).count() / wl.size()
    };
}

static void printRow(const char* name, const BenchResult& r) {
    std::cout << std::left << std::setw(12) << name << " | "
              << std::right << std::setw(7) << std::fixed << std::setprecision(2) << r.hitRate << "% | "
              << std::setw(8) << r.hits << " | "
              << std::setw(8) << r.misses << " | "
              << std::setw(6) << r.evictions << " | "
              << std::setw(8) << std::fixed << std::setprecision(0) << r.nsPerOp << " ns"
              << std::endl;
}

// ========== Workloads ==========

static std::vector<uint64_t> genUniform(size_t ops, size_t keySpace, std::mt19937_64& rng) {
    std::uniform_int_distribution<uint64_t> dist(0, keySpace - 1);
    std::vector<uint64_t> keys(ops);
    for (size_t i = 0; i < ops; i++) keys[i] = dist(rng);
    return keys;
}

static std::vector<uint64_t> genZipfian(size_t ops, size_t keySpace, double theta, std::mt19937_64& rng) {
    std::vector<uint64_t> keys(ops);
    for (size_t i = 0; i < ops; i++) keys[i] = zipfianSample(keySpace, theta, rng);
    return keys;
}

static std::vector<uint64_t> genTemporalShift(size_t ops, size_t keySpace, size_t phases) {
    std::mt19937_64 rng(123);
    size_t phaseLen = ops / phases;
    size_t half = keySpace / 2;
    std::vector<uint64_t> keys;
    keys.reserve(ops);
    for (size_t p = 0; p < phases; p++) {
        size_t lo = (p * half) % keySpace;
        size_t hi = std::min(lo + half, keySpace);
        std::uniform_int_distribution<uint64_t> dist(lo, hi - 1);
        for (size_t i = 0; i < phaseLen; i++) keys.push_back(dist(rng));
    }
    while (keys.size() < ops) keys.push_back(0);
    return keys;
}

static std::vector<uint64_t> genScanResistant(size_t ops, size_t hotSet, size_t keySpace, std::mt19937_64& rng) {
    std::uniform_int_distribution<uint64_t> hotDist(0, hotSet - 1);
    std::vector<uint64_t> keys(ops);
    for (size_t i = 0; i < ops; i++) {
        if ((i % 10) == 0)
            keys[i] = (i / 10) % keySpace;
        else
            keys[i] = hotDist(rng);
    }
    return keys;
}

static std::vector<uint64_t> genLooping(size_t ops, size_t cycleLen) {
    std::vector<uint64_t> keys(ops);
    for (size_t i = 0; i < ops; i++) keys[i] = i % cycleLen;
    return keys;
}

// ========== Main ==========

int main() {
    const size_t cap = 1000;
    const size_t keySpace = 10000;
    const size_t ops = 200000;
    const size_t kpp = 64;
    NuAtlas::RemarcConfig cfg;

    std::cout << "=== REMARC vs ARC vs LRU Microbenchmark ===" << std::endl;
    std::cout << "Capacity=" << cap << "  KeySpace=" << keySpace
              << "  Ops=" << ops << "  KeysPerPage=" << kpp << std::endl;
    std::cout << "REMARC: alpha=" << (int)cfg.AlphaLocal
              << "  theta_evict=" << (int)cfg.ThetaEvict
              << "  theta_migrate=" << (int)cfg.ThetaMigrate << std::endl;
    std::cout << std::endl;

    std::mt19937_64 rng(42);
    auto uniform = genUniform(ops, keySpace, rng);
    rng.seed(42);
    auto zipf = genZipfian(ops, keySpace, 0.99, rng);
    auto temporal = genTemporalShift(ops, keySpace, 4);
    rng.seed(42);
    auto scanResist = genScanResistant(ops, cap, keySpace, rng);
    auto loop = genLooping(ops, cap * 2);

    // === ORIGINAL BENCHMARKS (skipped for dynamic cap study) ===
#if 0
    auto runWL = [&](const char* name, const std::vector<uint64_t>& wl) {
        std::cout << "--- " << name << " ---" << std::endl;
        std::cout << std::left << std::setw(12) << "Policy" << " | "
                  << std::right << " HitRate |     Hits |   Misses | Evicts | Latency" << std::endl;
        std::cout << std::string(72, '-') << std::endl;

        BareLRU lru(cap);
        printRow("LRU", runBench(lru, wl));

        BareARC arc(cap);
        printRow("ARC", runBench(arc, wl));

        BareREMARC remarc(cap, kpp, cfg);
        printRow("REMARC", runBench(remarc, wl));

        BareREMARC_RF remarcRF(cap, kpp, cfg);
        printRow("REMARC-RF", runBench(remarcRF, wl));

        BareREMARC_RF_Key remarcKey(cap, cfg);
        printRow("REMARC-4b", runBench(remarcKey, wl));

        BareREMARC8_Dormant remarcD(cap, 8, 2);
        printRow("REMARC-D", runBench(remarcD, wl));

        BareREMARC_Factored remarcFG0(cap, 0.5, 0.0625, 8, 0.05, 0, 0);
        printRow("FG-Norm", runBench(remarcFG0, wl));

        BareREMARC_Factored remarcFG1(cap, 0.5, 0.0625, 8, 0.05, 0, 1);
        printRow("FG-Plain", runBench(remarcFG1, wl));

        BareREMARC_Factored remarcFG2(cap, 0.5, 0.0625, 8, 0.05, 0, 2);
        printRow("FG-Adapt", runBench(remarcFG2, wl));

        BareREMARC_Factored remarcFG3(cap, 0.5, 0.0625, 8, 0.05, 0, 3);
        printRow("FG-Mult", runBench(remarcFG3, wl));

        BareREMARC_FactoredInt remarcFI0(cap, 64, 2048, 0.05, 0, 0);
        printRow("FI-Mult", runBench(remarcFI0, wl));

        BareREMARC_FactoredInt remarcFI1(cap, 64, 2048, 0.05, 0, 1);
        printRow("FI-Norm", runBench(remarcFI1, wl));

        BareREMARC_FactoredInt remarcFI2(cap, 64, 2048, 0.05, 0, 2);
        printRow("FI-Adapt", runBench(remarcFI2, wl));

        BareREMARC_Log8 log8(cap, 16, 2, 1, 1, 8, 256, false);
        printRow("Log8", runBench(log8, wl));

        BareREMARC_Log8 log8g(cap, 16, 2, 1, 1, 8, 256, true);
        printRow("Log8-Ghost", runBench(log8g, wl));

        BareREMARC_Feedback fbMod(cap, 16, 2, 1, 1, 8, 256, 64, 2, 256, 256, 1);
        printRow("FB-Mod", runBench(fbMod, wl));

        BareREMARC_Feedback fbModH(cap, 8, 8, 1, 1, 8, 1024, 64, 2, 256, 256, 1);
        printRow("FB-High", runBench(fbModH, wl));

        BareREMARC_Log8 log8h(cap, 8, 8, 1, 1, 8, 1024, false);
        printRow("Log8-High", runBench(log8h, wl));

        BareREMARC_Log8 log8hg(cap, 8, 8, 1, 1, 8, 1024, true);
        printRow("Log8-HG", runBench(log8hg, wl));

        BareREMARC_Feedback fbWide(cap, 16, 2, 1, 1, 8, 256, 64, 2, 512, 512, 1);
        printRow("FB-Wide", runBench(fbWide, wl));

        BareREMARC_Feedback fbFast(cap, 16, 2, 1, 1, 8, 256, 16, 2, 256, 256, 1);
        printRow("FB-Fast", runBench(fbFast, wl));

        BareREMARC_Feedback fbFloor(cap, 16, 2, 1, 1, 8, 256, 64, 2, 256, 256, 32);
        printRow("FB-Floor", runBench(fbFloor, wl));

        BareREMARC_Tiered tierRatio(cap, 16, 2, 1, 1, 8, 256, 128, 4, true, 32, 224);
        printRow("Tier-Ratio", runBench(tierRatio, wl));

        BareREMARC_Tiered tierFixed(cap, 16, 2, 1, 1, 8, 256, 128, 4, false, 32, 224);
        printRow("Tier-Fixed", runBench(tierFixed, wl));

        BareREMARC_Tiered tierAgg(cap, 16, 2, 1, 1, 8, 256, 128, 16, true, 32, 224);
        printRow("Tier-Agg", runBench(tierAgg, wl));

        BareREMARC_Tiered tierSmallP(cap, 16, 2, 1, 1, 8, 256, 64, 4, true, 32, 224);
        printRow("Tier-SmallP", runBench(tierSmallP, wl));

        BareRR rr(cap);
        printRow("RR", runBench(rr, wl));

        std::cout << std::endl;
    };

    runWL("Uniform", uniform);
    runWL("Zipfian (0.99)", zipf);
    runWL("Temporal Shift", temporal);
    runWL("Scan-Resistant", scanResist);
    runWL("Looping (2*cap)", loop);

    // === Keys-per-page sensitivity ===
    std::cout << "=== Keys-Per-Page Sensitivity (Zipfian 0.99) ===" << std::endl;
    std::cout << std::left << std::setw(8) << "KPP" << " | "
              << std::right << " HitRate | Evictions | Scans | ns/op" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    BareARC arcRef(cap);
    double arcHR = runBench(arcRef, zipf).hitRate;
    std::cout << std::left << std::setw(8) << "ARC" << " | "
              << std::right << std::setw(7) << std::fixed << std::setprecision(2) << arcHR << "% | "
              << std::setw(9) << arcRef.evictions() << " | "
              << std::setw(5) << "n/a" << " | "
              << "per-key" << std::endl;

    for (size_t k : {16, 32, 64, 128, 256, 512}) {
        BareREMARC_RF remarc(cap, k, cfg);
        auto r = runBench(remarc, zipf);
        std::cout << std::left << std::setw(8) << k << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << r.hitRate << "% | "
                  << std::setw(9) << r.evictions << " | "
                  << std::setw(5) << remarc.scans() << " | "
                  << std::setw(8) << std::fixed << std::setprecision(0) << r.nsPerOp
                  << std::endl;
    }

    std::cout << std::endl;

    // === Theta-evict sensitivity ===
    std::cout << "=== Theta-Evict Sensitivity (Zipfian 0.99, KPP=64) ===" << std::endl;
    std::cout << std::left << std::setw(8) << "Theta" << " | "
              << std::right << " HitRate | Evictions | Scans" << std::endl;
    std::cout << std::string(45, '-') << std::endl;

    for (int te : {6, 8, 10, 12, 14}) {
        NuAtlas::RemarcConfig cfg2;
        cfg2.ThetaEvict = static_cast<uint8_t>(te);
        BareREMARC remarc(cap, kpp, cfg2);
        auto r = runBench(remarc, zipf);
        std::cout << std::left << std::setw(8) << te << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << r.hitRate << "% | "
                  << std::setw(9) << r.evictions << " | "
                  << std::setw(5) << remarc.scans()
                  << std::endl;
    }

    // === OLD SWEEPS (skipped for tuning pass) ===
#endif // original benchmarks
#if 0
    // === freqDecay sweep (LinFreq freqInc=16, Zipfian) ===
    std::cout << "\n=== freqDecay Sweep (LinFreq fi=16, Zipfian 0.99) ===" << std::endl;
    std::cout << std::left << std::setw(10) << "freqDecay" << " | HitRate | Looping" << std::endl;
    std::cout << std::string(40, '-') << std::endl;
    for (int fd : {16, 32, 64, 128, 256, 512, 1024, 2048, 4096}) {
        BareREMARC8_LinFreq remarcZ(cap, 8, 16, 64, static_cast<size_t>(fd));
        auto rz = runBench(remarcZ, zipf);
        BareREMARC8_LinFreq remarcLoop(cap, 8, 16, 64, static_cast<size_t>(fd));
        auto rl = runBench(remarcLoop, loop);
        std::cout << std::left << std::setw(10) << fd << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                  << std::setw(7) << std::fixed << std::setprecision(2) << rl.hitRate << "%"
                  << std::endl;
    }

    // === Poisson tau sweep ===
    std::cout << "\n=== Poisson tau Sweep (Zipfian + Looping) ===" << std::endl;
    std::cout << std::left << std::setw(8) << "tau" << " | Poisson | PoisLin | LoopP | LoopPL" << std::endl;
    std::cout << std::string(55, '-') << std::endl;
    for (double tau : {10.0, 30.0, 100.0, 300.0, 1000.0, 3000.0}) {
        BareREMARC_Poisson pZ(cap, tau, 0.1);
        auto rz = runBench(pZ, zipf);
        BareREMARC_PoissonLin plZ(cap, tau, 0.05);
        auto rlz = runBench(plZ, zipf);
        BareREMARC_Poisson pL(cap, tau, 0.1);
        auto rl = runBench(pL, loop);
        BareREMARC_PoissonLin plL(cap, tau, 0.05);
        auto rll = runBench(plL, loop);
        std::cout << std::left << std::setw(8) << static_cast<int>(tau) << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                  << std::setw(7) << rlz.hitRate << "% | "
                  << std::setw(5) << rl.hitRate << "% | "
                  << std::setw(5) << rll.hitRate << "%"
                  << std::endl;
    }

    // === Feedback REMARC sweep: qDecay × qSelfMod × kFloor (Zipfian + ScanRes + Looping) ===
    std::cout << "\n=== Feedback REMARC Sweep (bR=16, bF=2, rD=8, fD=256) ===" << std::endl;
    std::cout << std::left << std::setw(5) << "qD" << " " << std::setw(3) << "sM" << " " << std::setw(4) << "kF" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << " | "
              << std::setw(4) << "Qf" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    for (int qD : {16, 32, 64, 128, 256}) {
        for (int sM : {1, 2, 3}) {
            for (int kF : {1, 16, 32, 64}) {
                BareREMARC_Feedback bz(cap, 16, 2, 1, 1, 8, 256, qD, sM, 256, 256, kF);
                auto rz = runBench(bz, zipf);
                BareREMARC_Feedback bs(cap, 16, 2, 1, 1, 8, 256, qD, sM, 256, 256, kF);
                auto rs = runBench(bs, scanResist);
                BareREMARC_Feedback bl(cap, 16, 2, 1, 1, 8, 256, qD, sM, 256, 256, kF);
                auto rl = runBench(bl, loop);
                int qf = static_cast<int>(bl.Q());
                if (rz.hitRate > 25.0 || rs.hitRate > 85.0 || rl.hitRate > 40.0) {
                    std::cout << std::left << std::setw(5) << qD << " " << std::setw(3) << sM << " " << std::setw(4) << kF << " | "
                              << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                              << std::setw(7) << rs.hitRate << "% | "
                              << std::setw(7) << rl.hitRate << "% | "
                              << std::setw(4) << qf
                              << std::endl;
                }
            }
        }
    }

    // === Fast Q decay test (high-F params, targeting Temporal Shift) ===
    std::cout << "\n=== Fast Q Decay + Temporal Shift (bR=8, bF=8, fD=1024) ===" << std::endl;
    std::cout << std::left << std::setw(5) << "qD" << " " << std::setw(3) << "sM" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "Temp" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << " | "
              << std::setw(4) << "Qf" << std::endl;
    std::cout << std::string(70, '-') << std::endl;
    for (int qD : {2, 4, 8, 16, 32, 64}) {
        for (int sM : {1, 2, 3}) {
            BareREMARC_Feedback bz(cap, 8, 8, 1, 1, 8, 1024, qD, sM, 256, 256, 1);
            auto rz = runBench(bz, zipf);
            BareREMARC_Feedback bt(cap, 8, 8, 1, 1, 8, 1024, qD, sM, 256, 256, 1);
            auto rt = runBench(bt, temporal);
            BareREMARC_Feedback bs(cap, 8, 8, 1, 1, 8, 1024, qD, sM, 256, 256, 1);
            auto rs = runBench(bs, scanResist);
            BareREMARC_Feedback bl(cap, 8, 8, 1, 1, 8, 1024, qD, sM, 256, 256, 1);
            auto rl = runBench(bl, loop);
            int qf = static_cast<int>(bl.Q());
            std::cout << std::left << std::setw(5) << qD << " " << std::setw(3) << sM << " | "
                      << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                      << std::setw(7) << rt.hitRate << "% | "
                      << std::setw(7) << rs.hitRate << "% | "
                      << std::setw(7) << rl.hitRate << "% | "
                      << std::setw(4) << qf
                      << std::endl;
        }
    }

    // === Tiered REMARC sweep: pInit × pDelta × pMin (Zipfian + Temporal + ScanRes + Looping) ===
    std::cout << "\n=== Tiered REMARC Sweep (bR=16, bF=2, rD=8, fD=256, ratio) ===" << std::endl;
    std::cout << std::left << std::setw(5) << "pI" << " " << std::setw(4) << "pD" << " " << std::setw(4) << "pMn" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "Temp" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << " | "
              << std::setw(4) << "Pf" << std::endl;
    std::cout << std::string(70, '-') << std::endl;
    for (int pMn : {16, 32, 64, 96}) {
        for (int pI : {64, 128, 192}) {
            for (int pD : {1, 2, 4, 8}) {
                int pMx = 256 - pMn;
                BareREMARC_Tiered bz(cap, 16, 2, 1, 1, 8, 256, pI, pD, true, pMn, pMx);
                auto rz = runBench(bz, zipf);
                BareREMARC_Tiered bt(cap, 16, 2, 1, 1, 8, 256, pI, pD, true, pMn, pMx);
                auto rt = runBench(bt, temporal);
                BareREMARC_Tiered bs(cap, 16, 2, 1, 1, 8, 256, pI, pD, true, pMn, pMx);
                auto rs = runBench(bs, scanResist);
                BareREMARC_Tiered bl(cap, 16, 2, 1, 1, 8, 256, pI, pD, true, pMn, pMx);
                auto rl = runBench(bl, loop);
                int pf = bl.p();
                std::cout << std::left << std::setw(5) << pI << " " << std::setw(4) << pD << " " << std::setw(4) << pMn << " | "
                          << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                          << std::setw(7) << rt.hitRate << "% | "
                          << std::setw(7) << rs.hitRate << "% | "
                          << std::setw(7) << rl.hitRate << "% | "
                          << std::setw(4) << pf
                          << std::endl;
            }
        }
    }

    // === Tiered REMARC: high-F params sweep ===
    std::cout << "\n=== Tiered REMARC Sweep (bR=8, bF=8, fD=1024, ratio) ===" << std::endl;
    std::cout << std::left << std::setw(5) << "pI" << " " << std::setw(4) << "pD" << " " << std::setw(4) << "pMn" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "Temp" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << " | "
              << std::setw(4) << "Pf" << std::endl;
    std::cout << std::string(70, '-') << std::endl;
    for (int pMn : {32, 64}) {
        for (int pI : {64, 128, 192}) {
            for (int pD : {1, 2, 4}) {
                int pMx = 256 - pMn;
                BareREMARC_Tiered bz(cap, 8, 8, 1, 1, 8, 1024, pI, pD, true, pMn, pMx);
                auto rz = runBench(bz, zipf);
                BareREMARC_Tiered bt(cap, 8, 8, 1, 1, 8, 1024, pI, pD, true, pMn, pMx);
                auto rt = runBench(bt, temporal);
                BareREMARC_Tiered bs(cap, 8, 8, 1, 1, 8, 1024, pI, pD, true, pMn, pMx);
                auto rs = runBench(bs, scanResist);
                BareREMARC_Tiered bl(cap, 8, 8, 1, 1, 8, 1024, pI, pD, true, pMn, pMx);
                auto rl = runBench(bl, loop);
                int pf = bl.p();
                std::cout << std::left << std::setw(5) << pI << " " << std::setw(4) << pD << " " << std::setw(4) << pMn << " | "
                          << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                          << std::setw(7) << rt.hitRate << "% | "
                          << std::setw(7) << rs.hitRate << "% | "
                          << std::setw(7) << rl.hitRate << "% | "
                          << std::setw(4) << pf
                          << std::endl;
            }
        }
    }

    // === Directional Feedback REMARC: ghost key profile drives Q directionally ===
    // Instead of scalar Q, use the sR/(sR+sF) ratio of the returning ghost key
    // to determine which direction to adjust weights.
    // If ghost key had high sF (frequent but evicted) → increase wF
    // If ghost key had high sR (recent but evicted) → increase wR
    // This is the REMARC analog of ARC's B1/B2 directional feedback.

    std::cout << "\n=== Directional Feedback REMARC ===" << std::endl;
    std::cout << std::left << std::setw(5) << "qD" << " " << std::setw(3) << "sM" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "Temp" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    auto runDirFB = [&](int qD, int sM, uint8_t bR, uint8_t bF, size_t fD) {
        // Directional feedback uses two counters: Q_f (frequency evidence) and Q_r (recency evidence)
        // On ghost hit with key profile (sR, sF):
        //   ratio = sR / max(1, sR + sF)   — 0=all-freq, 1=all-rec
        //   Q_r += adaptive * ratio          — recency evidence increases
        //   Q_f += adaptive * (1-ratio)      — frequency evidence increases
        // Score: wR = wrBase * max(k, Q_r) / 255, wF = wfBase * max(k, Q_f) / 255
        // Both decay periodically (self-modulated)

        struct DEntry { uint8_t sR, sF; };
        auto bench = [&](const std::vector<uint64_t>& wl) -> BenchResult {
            std::unordered_map<uint64_t, DEntry> dcache, dghost;
            std::list<uint64_t> dghostOrder;
            std::unordered_map<uint64_t, std::list<uint64_t>::iterator> dghostIter;
            uint8_t Qr = 0, Qf = 0;
            size_t dhits = 0, dmisses = 0, devictions = 0, dopCount = 0;
            auto t0 = Clock::now();

            for (uint64_t key : wl) {
                ++dopCount;
                if (dopCount % 8 == 0) {
                    uint8_t dsR = 1;
                    for (auto& [k, e] : dcache) e.sR = (e.sR > dsR) ? (e.sR - dsR) : 0;
                }
                if (dopCount % fD == 0) {
                    for (auto& [k, e] : dcache) e.sF = (e.sF > 1) ? (e.sF - 1) : 0;
                }
                // Q decay (self-modulated)
                if (dopCount % qD == 0) {
                    uint8_t stepR = (Qr >> 4) + 1;
                    uint8_t stepF = (Qf >> 4) + 1;
                    Qr = (Qr > stepR) ? (Qr - stepR) : 0;
                    Qf = (Qf > stepF) ? (Qf - stepF) : 0;
                }

                auto it = dcache.find(key);
                if (it != dcache.end()) {
                    it->second.sR = std::min<uint16_t>(255, it->second.sR + bR);
                    it->second.sF = std::min<uint16_t>(255, it->second.sF + bF);
                    ++dhits;
                    continue;
                }
                ++dmisses;

                auto gi = dghost.find(key);
                bool ghostHit = (gi != dghost.end());

                if (ghostHit) {
                    // Directional feedback from ghost key profile
                    uint16_t sum = static_cast<uint16_t>(gi->second.sR) + gi->second.sF;
                    uint16_t ratioR = (sum > 0) ? (static_cast<uint16_t>(gi->second.sR) * 255 / sum) : 128;
                    uint16_t ratioF = 255 - ratioR;
                    // Self-modulated boost
                    uint16_t boostR_q = (255 - Qr) >> sM;
                    uint16_t boostF_q = (255 - Qf) >> sM;
                    Qr = std::min<uint16_t>(255, Qr + static_cast<uint16_t>(ratioR * boostR_q / 255));
                    Qf = std::min<uint16_t>(255, Qf + static_cast<uint16_t>(ratioF * boostF_q / 255));
                }

                if (dcache.size() >= cap) {
                    // Find victim using directional weights
                    uint32_t wRv = std::max<uint32_t>(1, (static_cast<uint32_t>(256) * std::max(1, static_cast<int>(Qr))) / 255);
                    uint32_t wFv = std::max<uint32_t>(1, (static_cast<uint32_t>(256) * std::max(1, static_cast<int>(Qf))) / 255);
                    uint64_t worst = 0;
                    uint32_t worstScore = UINT32_MAX;
                    for (auto& [k, e] : dcache) {
                        uint32_t sc = wRv * e.sR + wFv * e.sF;
                        if (sc < worstScore) { worstScore = sc; worst = k; }
                    }
                    auto vit = dcache.find(worst);
                    if (dghost.size() >= cap) {
                        uint64_t old = dghostOrder.front();
                        dghostOrder.pop_front();
                        dghostIter.erase(old);
                        dghost.erase(old);
                    }
                    dghost[worst] = vit->second;
                    dghostOrder.push_back(worst);
                    dghostIter[worst] = std::prev(dghostOrder.end());
                    dcache.erase(vit);
                    ++devictions;
                }

                if (ghostHit) {
                    DEntry rest = gi->second;
                    rest.sR = std::min<uint16_t>(255, rest.sR + bR);
                    rest.sF = std::min<uint16_t>(255, rest.sF + bF);
                    auto li = dghostIter.find(key);
                    if (li != dghostIter.end()) dghostOrder.erase(li->second);
                    dghostIter.erase(key);
                    dghost.erase(gi);
                    dcache[key] = rest;
                } else {
                    dcache[key] = {bR, bF};
                }
            }
            auto t1 = Clock::now();
            return {
                (dhits + dmisses > 0) ? 100.0 * dhits / (dhits + dmisses) : 0.0,
                dhits, dmisses, devictions,
                std::chrono::duration<double, std::nano>(t1 - t0).count() / wl.size()
            };
        };

        auto rz = bench(zipf);
        auto rt = bench(temporal);
        auto rs = bench(scanResist);
        auto rl = bench(loop);
        std::cout << std::left << std::setw(5) << qD << " " << std::setw(3) << sM << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                  << std::setw(7) << rt.hitRate << "% | "
                  << std::setw(7) << rs.hitRate << "% | "
                  << std::setw(7) << rl.hitRate << "%"
                  << std::endl;
    };

    std::cout << "--- bR=8, bF=8, fD=1024 ---" << std::endl;
    for (int qD : {2, 4, 8, 16, 32}) {
        for (int sM : {1, 2, 3}) {
            runDirFB(qD, sM, 8, 8, 1024);
        }
    }
    std::cout << "--- bR=16, bF=2, fD=256 ---" << std::endl;
    for (int qD : {4, 8, 16, 32, 64}) {
        for (int sM : {1, 2, 3}) {
            runDirFB(qD, sM, 16, 2, 256);
        }
    }
#endif // old sweeps

    // === TUNING PASS (skipped for 2-node study) ===
#if 0
    // === TUNING PASS: Flat integer REMARC Pareto frontier ===
    // Sweep (boostR, boostF, recDecay, freqDecay) on the simplest variant.
    // No ghost, no tiers — just bare integer EMA scoring.
    // Goal: find good defaults and map the Zipf-Loop Pareto frontier.

    auto tuneBench = [&](uint8_t bR, uint8_t bF, size_t rD, size_t fD,
                         const std::vector<uint64_t>& wl) -> BenchResult {
        struct E { uint8_t sR, sF; };
        std::unordered_map<uint64_t, E> c;
        size_t h = 0, m = 0, ev = 0, op = 0;
        auto t0 = Clock::now();
        for (uint64_t k : wl) {
            ++op;
            if (op % rD == 0) for (auto& [_, e] : c) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
            if (op % fD == 0) for (auto& [_, e] : c) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
            auto it = c.find(k);
            if (it != c.end()) {
                it->second.sR = std::min<uint16_t>(255, it->second.sR + bR);
                it->second.sF = std::min<uint16_t>(255, it->second.sF + bF);
                ++h; continue;
            }
            ++m;
            if (c.size() >= cap) {
                uint64_t worst = 0; uint16_t ws = 65535;
                for (auto& [kk, e] : c) { uint16_t s = e.sR + e.sF; if (s < ws) { ws = s; worst = kk; } }
                c.erase(worst); ++ev;
            }
            c[k] = {bR, bF};
        }
        auto t1 = Clock::now();
        return { (h+m>0) ? 100.0*h/(h+m) : 0.0, h, m, ev,
                 std::chrono::duration<double,std::nano>(t1-t0).count()/wl.size() };
    };

    std::cout << "\n=== TUNING PASS: Flat Integer REMARC ===" << std::endl;
    std::cout << std::left << std::setw(3) << "bR" << " " << std::setw(3) << "bF" << " "
              << std::setw(3) << "rD" << " " << std::setw(5) << "fD" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "Temp" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    for (auto [bR, bF, rD, fD] : std::vector<std::tuple<int,int,int,int>>{
        // Max Looping
        {4, 1, 4, 64}, {4, 1, 4, 128}, {4, 1, 4, 256},
        // Balanced
        {16, 2, 8, 256}, {16, 2, 16, 256},
        // ScanRes-optimal
        {8, 4, 8, 512}, {8, 4, 8, 256}, {8, 4, 8, 1024},
        // Zipf+Scan
        {8, 8, 8, 512}, {8, 8, 8, 1024},
        // Max Zipfian
        {12, 8, 16, 1024}, {16, 8, 16, 1024}, {16, 8, 16, 512},
        // More frontier points
        {8, 2, 8, 1024}, {8, 2, 8, 512}, {16, 2, 8, 1024},
        {16, 4, 8, 512}, {16, 4, 8, 1024}, {4, 2, 4, 1024},
        {4, 4, 4, 1024}, {8, 4, 4, 1024}, {12, 4, 8, 1024},
        {24, 8, 16, 1024}, {32, 8, 16, 1024},
        {16, 8, 8, 1024}, {12, 8, 8, 1024}, {16, 8, 8, 512}
    }) {
        auto rz = tuneBench(bR, bF, rD, fD, zipf);
        auto rt = tuneBench(bR, bF, rD, fD, temporal);
        auto rs = tuneBench(bR, bF, rD, fD, scanResist);
        auto rl = tuneBench(bR, bF, rD, fD, loop);
        std::cout << std::left << std::setw(3) << bR << " " << std::setw(3) << bF << " "
                  << std::setw(3) << rD << " " << std::setw(5) << fD << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                  << std::setw(7) << rt.hitRate << "% | "
                  << std::setw(7) << rs.hitRate << "% | "
                  << std::setw(7) << rl.hitRate << "%"
                  << std::endl;
    }

    // === TUNING PASS: With ghost restoration (dormant phi) ===
    // Ghost decay in ghost at same rate as cache (dormant phi)
    std::cout << "\n=== TUNING PASS: Integer REMARC + Ghost ===" << std::endl;
    std::cout << std::left << std::setw(3) << "bR" << " " << std::setw(3) << "bF" << " "
              << std::setw(3) << "rD" << " " << std::setw(5) << "fD" << " | "
              << std::right << std::setw(7) << "Zipf" << " | "
              << std::setw(7) << "Temp" << " | "
              << std::setw(7) << "ScanR" << " | "
              << std::setw(7) << "Loop" << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    auto tuneGhostBench = [&](uint8_t bR, uint8_t bF, size_t rD, size_t fD,
                              const std::vector<uint64_t>& wl) -> BenchResult {
        struct E { uint8_t sR, sF; };
        std::unordered_map<uint64_t, E> c, g;
        std::list<uint64_t> go;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> gi;
        size_t h = 0, m = 0, ev = 0, op = 0;
        for (uint64_t k : wl) {
            ++op;
            if (op % rD == 0) {
                for (auto& [_, e] : c) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
                for (auto& [_, e] : g) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
            }
            if (op % fD == 0) {
                for (auto& [_, e] : c) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
                for (auto& [_, e] : g) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
            }
            auto it = c.find(k);
            if (it != c.end()) {
                it->second.sR = std::min<uint16_t>(255, it->second.sR + bR);
                it->second.sF = std::min<uint16_t>(255, it->second.sF + bF);
                ++h; continue;
            }
            ++m;
            auto ggi = g.find(k);
            bool ghostHit = (ggi != g.end());
            if (c.size() >= cap) {
                uint64_t worst = 0; uint16_t ws = 65535;
                for (auto& [kk, e] : c) { uint16_t s = e.sR + e.sF; if (s < ws) { ws = s; worst = kk; } }
                if (g.size() >= cap) {
                    uint64_t old = go.front(); go.pop_front(); gi.erase(old); g.erase(old);
                }
                auto vit = c.find(worst);
                g[worst] = vit->second;
                go.push_back(worst); gi[worst] = std::prev(go.end());
                c.erase(vit); ++ev;
            }
            if (ghostHit) {
                E rest = ggi->second;
                rest.sR = std::min<uint16_t>(255, rest.sR + bR);
                rest.sF = std::min<uint16_t>(255, rest.sF + bF);
                auto li = gi.find(k); if (li != gi.end()) go.erase(li->second);
                gi.erase(k); g.erase(ggi);
                c[k] = rest;
            } else {
                c[k] = {bR, bF};
            }
        }
        return { (h+m>0) ? 100.0*h/(h+m) : 0.0, h, m, ev, 0.0 };
    };

    for (auto [bR, bF, rD, fD] : std::vector<std::tuple<int,int,int,int>>{
        {16, 2, 8, 256}, {16, 2, 16, 256}, {8, 2, 8, 256}, {8, 4, 8, 512},
        {8, 8, 8, 512}, {8, 8, 8, 1024}, {12, 8, 16, 1024}, {16, 8, 16, 1024},
        {16, 4, 8, 256}, {16, 4, 8, 512}, {8, 2, 8, 1024}, {16, 2, 8, 1024}
    }) {
        auto rz = tuneGhostBench(bR, bF, rD, fD, zipf);
        auto rt = tuneGhostBench(bR, bF, rD, fD, temporal);
        auto rs = tuneGhostBench(bR, bF, rD, fD, scanResist);
        auto rl = tuneGhostBench(bR, bF, rD, fD, loop);
        std::cout << std::left << std::setw(3) << bR << " " << std::setw(3) << bF << " "
                  << std::setw(3) << rD << " " << std::setw(5) << fD << " | "
                  << std::right << std::setw(7) << std::fixed << std::setprecision(2) << rz.hitRate << "% | "
                  << std::setw(7) << rt.hitRate << "% | "
                  << std::setw(7) << rs.hitRate << "% | "
                  << std::setw(7) << rl.hitRate << "%"
                  << std::endl;
    }
#endif // tuning pass

    // ==========================================================================
    // === 3-NODE NUMA SIMULATION ===
    // ==========================================================================
    {
        const size_t cap3 = 67;
        const size_t nK3 = 999;
        const size_t nO3 = 500000;
        const uint8_t bR3 = 8, bF3 = 8;
        const size_t rD3 = 8, fD3 = 1024;

        struct R3 {
            size_t lh[3] = {}, rh[3] = {}, mi[3] = {};
            size_t mg = 0, contention = 0;
            size_t totalLH() const { return lh[0]+lh[1]+lh[2]; }
            size_t totalRH() const { return rh[0]+rh[1]+rh[2]; }
            size_t totalMI() const { return mi[0]+mi[1]+mi[2]; }
            double cost(size_t t) const {
                return (totalLH()*80.0 + totalRH()*150.0 + totalMI()*500.0) / t;
            }
        };

        auto printR3 = [](const char* name, const R3& r, size_t t) {
            std::cout << std::left << std::setw(24) << name
                      << " | " << std::right << std::setw(5) << std::fixed << std::setprecision(1)
                      << (100.0*r.totalLH()/t) << "%"
                      << " | " << std::setw(5) << (100.0*r.totalRH()/t) << "%"
                      << " | " << std::setw(5) << (100.0*r.totalMI()/t) << "%"
                      << " | " << std::setw(6) << std::setprecision(1) << r.cost(t)
                      << " | " << std::setw(5) << r.mg
                      << " | " << std::setw(5) << r.contention
                      << " | ";
            for (int n = 0; n < 3; n++)
                std::cout << std::setw(4) << (int)(100.0*r.lh[n]/(t/3)) << " ";
            std::cout << std::endl;
        };

        std::vector<int> h3(nK3);
        for (size_t i = 0; i < nK3; i++) h3[i] = i % 3;

        auto wl3_shared = [&]() {
            std::mt19937_64 rng(42);
            std::vector<std::pair<uint64_t,int>> wl;
            wl.reserve(nO3);
            for (size_t i = 0; i < nO3; i++) {
                int acc = i % 3;
                uint64_t k;
                if (rng() % 100 < 25) {
                    k = rng() % 10;
                } else {
                    k = acc * 333 + (rng() % 333);
                }
                if (k >= nK3) k = nK3 - 1;
                wl.push_back({k, acc});
            }
            return wl;
        }();

        auto wl3_zipf = [&]() {
            std::vector<std::pair<uint64_t,int>> wl;
            wl.reserve(nO3);
            std::mt19937_64 rng(42);
            for (size_t i = 0; i < nO3; i++) {
                wl.push_back({zipfianSample(nK3, 0.99, rng), (int)(i % 3)});
            }
            return wl;
        }();

        auto wl3_cross = [&]() {
            std::mt19937_64 rng(43);
            std::vector<std::pair<uint64_t,int>> wl;
            wl.reserve(nO3);
            for (size_t i = 0; i < nO3; i++) {
                int acc = i % 3;
                int other1 = (acc + 1) % 3, other2 = (acc + 2) % 3;
                uint64_t k;
                if (rng() % 10 < 4) k = other1 * 333 + (rng() % 333);
                else if (rng() % 10 < 7) k = other2 * 333 + (rng() % 333);
                else k = acc * 333 + (rng() % 333);
                if (k >= nK3) k = nK3 - 1;
                wl.push_back({k, acc});
            }
            return wl;
        }();

        auto wl3_rotate = [&]() {
            std::mt19937_64 rng(44);
            std::vector<std::pair<uint64_t,int>> wl;
            wl.reserve(nO3);
            size_t phaseLen = nO3 / 6;
            for (size_t i = 0; i < nO3; i++) {
                int acc = i % 3;
                int phase = (i / phaseLen) % 3;
                uint64_t lo = phase * 333;
                uint64_t k;
                if (rng() % 100 < 70) {
                    k = lo + zipfianSample(333, 0.99, rng);
                } else {
                    k = rng() % nK3;
                }
                if (k >= nK3) k = nK3 - 1;
                wl.push_back({k, acc});
            }
            return wl;
        }();

        struct E { uint8_t sR, sF; };

        auto run3 = [&](const char* title, const std::vector<std::pair<uint64_t,int>>& wl) {
            std::cout << "\n=== 3-NODE: " << title << " ===" << std::endl;
            std::cout << std::left << std::setw(24) << "Policy"
                      << " | " << std::right << std::setw(6) << "Loc%"
                      << " | " << std::setw(6) << "Rem%"
                      << " | " << std::setw(6) << "Miss%"
                      << " | " << std::setw(6) << "Cost"
                      << " | " << std::setw(5) << "Migr"
                      << " | " << std::setw(5) << "Cont"
                      << " | PerNode LH%"
                      << std::endl;
            std::cout << std::string(95, '-') << std::endl;
            // ratio=0 means no compare (desire-only), ratio=N means need dk > ownerDes*N/10
            auto runExp = [&](int ratio10) -> R3 {
                R3 r;
                std::unordered_map<uint64_t, E> des[3];
                std::unordered_map<uint64_t, E> cch[3];
                std::unordered_set<uint64_t> inC[3];
                std::unordered_map<uint64_t, size_t> lastMig;
                size_t op = 0;
                for (auto& [k, acc] : wl) {
                    ++op;
                    if (op % rD3 == 0) for (int n = 0; n < 3; n++) {
                        for (auto& [_, e] : des[n]) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
                        for (auto& [_, e] : cch[n]) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
                    }
                    if (op % fD3 == 0) for (int n = 0; n < 3; n++) {
                        for (auto& [_, e] : des[n]) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
                        for (auto& [_, e] : cch[n]) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
                    }
                    auto& d = des[acc][k];
                    d.sR = std::min<uint16_t>(255, d.sR + bR3);
                    d.sF = std::min<uint16_t>(255, d.sF + bF3);
                    int loc = -1;
                    for (int n = 0; n < 3; n++) if (inC[n].count(k)) { loc = n; break; }
                    if (loc == acc) { r.lh[acc]++; }
                    else if (loc >= 0) {
                        r.rh[acc]++;
                        cch[loc][k].sR = std::min<uint16_t>(255, cch[loc][k].sR + bR3);
                        cch[loc][k].sF = std::min<uint16_t>(255, cch[loc][k].sF + bF3);
                        uint16_t dk = d.sR + d.sF;
                        bool doMig = (inC[acc].size() < cap3);
                        if (!doMig) {
                            uint64_t wk = 0; uint16_t ws = 65535;
                            for (auto& kk : inC[acc]) {
                                auto& e = des[acc][kk]; uint16_t s = e.sR+e.sF;
                                if (s < ws) { ws = s; wk = kk; }
                            }
                            if (dk > ws) doMig = true;
                        }
                        if (ratio10 > 0 && doMig) {
                            uint16_t ownerDes = 0;
                            auto oit = des[loc].find(k);
                            if (oit != des[loc].end()) ownerDes = oit->second.sR + oit->second.sF;
                            if (dk * 10 <= ownerDes * ratio10) { r.contention++; doMig = false; }
                        }
                        if (ratio10 < 0 && doMig) {
                            auto lmit = lastMig.find(k);
                            if (lmit != lastMig.end() && op - lmit->second < 3000) {
                                r.contention++; doMig = false;
                            } else lastMig[k] = op;
                        }
                        if (doMig) {
                            if (inC[acc].size() >= cap3) {
                                uint64_t wk = 0; uint16_t ws = 65535;
                                for (auto& kk : inC[acc]) {
                                    auto& e = des[acc][kk]; uint16_t s = e.sR+e.sF;
                                    if (s < ws) { ws = s; wk = kk; }
                                }
                                inC[acc].erase(wk);
                            }
                            inC[loc].erase(k); cch[loc].erase(k);
                            inC[acc].insert(k); r.mg++;
                        }
                    } else {
                        r.mi[acc]++;
                        int hn = h3[k];
                        cch[hn][k] = {bR3, bF3}; inC[hn].insert(k);
                        if (inC[hn].size() > cap3) {
                            uint64_t wk = 0; uint16_t ws = 65535;
                            for (auto& kk : inC[hn]) {
                                auto& e = des[hn][kk]; uint16_t s = e.sR+e.sF;
                                if (s < ws) { ws = s; wk = kk; }
                            }
                            if (wk != k) { inC[hn].erase(wk); cch[hn].erase(wk); }
                        }
                    }
                }
                return r;
            };
            printR3("Desire-only", runExp(0), nO3);
            for (int r : {10, 11, 12, 13, 15, 18, 20, 25, 30}) {
                std::string name = "Compare(" + std::to_string(r) + "/10)";
                printR3(name.c_str(), runExp(r), nO3);
            }
            printR3("Cooldown(3K)", runExp(-1), nO3);
        };

        run3("Zipfian contention", wl3_zipf);
        run3("Cross-dominant", wl3_cross);
        run3("Rotating hot set", wl3_rotate);
    }

    // ==========================================================================
    // === SINGLE-NODE: Ghost Map Study ===
    // ==========================================================================
    {
        const size_t snCap = 1000;
        const size_t snKeys = 10000;
        const size_t snOps = 100000;

        std::cout << "\n=== SINGLE-NODE: Ghost Map Study ===" << std::endl;
        std::cout << "cap=" << snCap << " keys=" << snKeys << " ops=" << snOps << std::endl;
        std::cout << std::left << std::setw(24) << "Variant"
                  << " | " << std::right << std::setw(7) << "Zipf%"
                  << " | " << std::setw(7) << "Temp%"
                  << " | " << std::setw(7) << "Scan%"
                  << " | " << std::setw(7) << "Loop%"
                  << std::endl;
        std::cout << std::string(55, '-') << std::endl;

        std::mt19937_64 snRng(42);
        auto snZipf = genZipfian(snOps, snKeys, 0.99, snRng);
        auto snTemp = genTemporalShift(snOps, snKeys, 4);
        snRng.seed(42);
        auto snScan = genScanResistant(snOps, snCap, snKeys, snRng);
        auto snLoop = genLooping(snOps, snCap * 2);

        struct E { uint8_t sR, sF; };

        // Cache-only REMARC (no ghost map — same as old in-cache scoring)
        auto snCacheOnly = [&](const std::vector<uint64_t>& wl, size_t cap,
                               uint8_t bR, uint8_t bF, size_t rD, size_t fD) -> double {
            std::unordered_map<uint64_t, E> scores;
            size_t h = 0, op = 0;
            for (uint64_t k : wl) {
                ++op;
                if (op % rD == 0) for (auto& [_, e] : scores) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
                if (op % fD == 0) for (auto& [_, e] : scores) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
                auto it = scores.find(k);
                if (it != scores.end()) {
                    it->second.sR = std::min<uint16_t>(255, it->second.sR + bR);
                    it->second.sF = std::min<uint16_t>(255, it->second.sF + bF);
                    h++;
                } else {
                    E ne = {bR, bF};
                    if (scores.size() >= cap) {
                        uint64_t wk = 0; uint16_t ws = 65535;
                        for (auto& [kk, e] : scores) { uint16_t s = e.sR+e.sF; if (s < ws) { ws = s; wk = kk; } }
                        scores.erase(wk);
                    }
                    scores[k] = ne;
                }
            }
            return 100.0 * h / wl.size();
        };

        // ARC baseline (O(n) list-based for correctness)
        auto snARC = [&](const std::vector<uint64_t>& wl, size_t cap) -> double {
            std::list<uint64_t> t1, t2, b1, b2;
            size_t p = 0;
            size_t h = 0;
            auto contains = [](const std::list<uint64_t>& l, uint64_t k) {
                return std::find(l.begin(), l.end(), k) != l.end();
            };
            auto replace = [&](bool inB2) {
                if (!t1.empty() && (t1.size() > p || (t1.size() == p && inB2))) {
                    uint64_t old = t1.front(); t1.pop_front(); b1.push_front(old);
                } else if (!t2.empty()) {
                    uint64_t old = t2.front(); t2.pop_front(); b2.push_front(old);
                }
            };
            for (uint64_t k : wl) {
                if (contains(t1, k)) { t1.remove(k); t2.push_back(k); h++; }
                else if (contains(t2, k)) { t2.remove(k); t2.push_back(k); h++; }
                else if (contains(b1, k)) {
                    p = std::min(cap, p + std::max((size_t)1, b2.size() / std::max((size_t)1, b1.size())));
                    replace(false);
                    b1.remove(k); t2.push_back(k);
                } else if (contains(b2, k)) {
                    p = std::max((size_t)0, p - std::max((size_t)1, b1.size() / std::max((size_t)1, b2.size())));
                    replace(true);
                    b2.remove(k); t2.push_back(k);
                } else {
                    if (t1.size() + t2.size() >= cap) replace(false);
                    if (b1.size() + t1.size() + b2.size() + t2.size() >= 2 * cap) {
                        if (!b1.empty()) b1.pop_back(); else if (!b2.empty()) b2.pop_back();
                    }
                    t1.push_back(k);
                }
            }
            return 100.0 * h / wl.size();
        };

        // Single-tier desire (baseline — same as exp_desire but single-node)
        auto snBench = [&](const std::vector<uint64_t>& wl, size_t cap,
                           uint8_t bR, uint8_t bF, size_t rD, size_t fD) -> double {
            std::unordered_map<uint64_t, E> des;
            std::unordered_set<uint64_t> inC;
            size_t h = 0, op = 0;
            for (uint64_t k : wl) {
                ++op;
                if (op % rD == 0) for (auto& [_, e] : des) e.sR = (e.sR > 1) ? e.sR - 1 : 0;
                if (op % fD == 0) for (auto& [_, e] : des) e.sF = (e.sF > 1) ? e.sF - 1 : 0;
                auto& d = des[k];
                d.sR = std::min<uint16_t>(255, d.sR + bR);
                d.sF = std::min<uint16_t>(255, d.sF + bF);
                if (inC.count(k)) { h++; }
                else {
                    if (inC.size() >= cap) {
                        uint64_t wk = 0; uint16_t ws = 65535;
                        for (auto& kk : inC) {
                            auto it = des.find(kk);
                            if (it != des.end()) { uint16_t s = it->second.sR+it->second.sF; if (s < ws) { ws = s; wk = kk; } }
                        }
                        inC.erase(wk);
                    }
                    inC.insert(k);
                }
            }
            return (h + (wl.size()-h) > 0) ? 100.0 * h / wl.size() : 0.0;
        };

        auto snGhostDecayCached = [&](const std::vector<uint64_t>& wl) -> double {
            std::unordered_map<uint64_t, E> des;
            std::unordered_set<uint64_t> inC;
            size_t h = 0, op = 0;
            for (uint64_t k : wl) {
                ++op;
                if (op % 8 == 0) for (auto& kk : inC) { auto it = des.find(kk); if (it != des.end()) it->second.sR = (it->second.sR > 1) ? it->second.sR - 1 : 0; }
                if (op % 1024 == 0) for (auto& kk : inC) { auto it = des.find(kk); if (it != des.end()) it->second.sF = (it->second.sF > 1) ? it->second.sF - 1 : 0; }
                auto& d = des[k];
                d.sR = std::min<uint16_t>(255, d.sR + 8);
                d.sF = std::min<uint16_t>(255, d.sF + 8);
                if (inC.count(k)) { h++; }
                else {
                    if (inC.size() >= snCap) {
                        uint64_t wk = 0; uint16_t ws = 65535;
                        for (auto& kk : inC) {
                            auto it = des.find(kk);
                            if (it != des.end()) { uint16_t s = it->second.sR+it->second.sF; if (s < ws) { ws = s; wk = kk; } }
                        }
                        inC.erase(wk);
                    }
                    inC.insert(k);
                }
            }
            return 100.0 * h / wl.size();
        };

        auto printG = [](const char* name, double z, double t, double s, double l) {
            std::cout << std::left << std::setw(24) << name
                      << " | " << std::right << std::setw(6) << std::fixed << std::setprecision(2) << z << "%"
                      << " | " << std::setw(6) << t << "%"
                      << " | " << std::setw(6) << s << "%"
                      << " | " << std::setw(6) << l << "%"
                      << std::endl;
        };

        printG("ARC", snARC(snZipf, snCap), snARC(snTemp, snCap), snARC(snScan, snCap), snARC(snLoop, snCap));
        printG("Cache-only(no ghost)", snCacheOnly(snZipf, snCap, 8, 8, 8, 1024),
                snCacheOnly(snTemp, snCap, 8, 8, 8, 1024),
                snCacheOnly(snScan, snCap, 8, 8, 8, 1024),
                snCacheOnly(snLoop, snCap, 8, 8, 8, 1024));
        printG("Ghost-decay-cached", snGhostDecayCached(snZipf), snGhostDecayCached(snTemp), snGhostDecayCached(snScan), snGhostDecayCached(snLoop));
        printG("Desire-single(full ghost)", snBench(snZipf, snCap, 8, 8, 8, 1024),
                snBench(snTemp, snCap, 8, 8, 8, 1024),
                snBench(snScan, snCap, 8, 8, 8, 1024),
                snBench(snLoop, snCap, 8, 8, 8, 1024));
    }

    return 0;
}
