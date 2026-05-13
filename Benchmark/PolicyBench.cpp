#include "Remarc.h"
#include "Policy.h"
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <vector>
#include <random>
#include <algorithm>
#include <iomanip>
#include <cmath>
#include <chrono>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <queue>
#include <numeric>

using namespace NuAtlas;

// =====================================================================
//  Workload generators (from RemarcBench, kept minimal)
// =====================================================================

static std::vector<uint64_t> genZipfian(size_t n, size_t universe, double theta, std::mt19937_64& rng) {
    double alpha = 1.0 / (1.0 - theta);
    double zeta = 0.0;
    for (size_t i = 0; i < universe; i++) zeta += 1.0 / std::pow((double)(i + 1), theta);
    double eta = (1.0 - std::pow(2.0 / (double)universe, 1.0 - theta)) /
                 (1.0 - zeta + std::pow(2.0 / (double)universe, 1.0 - theta) / (1.0 - 1.0 / (double)universe));
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) {
        double u = dist(rng);
        double uz = u * zeta;
        if (uz < 1.0) { result[i] = 0; continue; }
        if (uz < 1.0 + std::pow(0.5, theta)) { result[i] = 1; continue; }
        result[i] = (size_t)((double)universe * std::pow(eta * u - eta + 1.0, 1.0 / (1.0 - theta))) % universe;
    }
    return result;
}

static std::vector<uint64_t> genTemporalShift(size_t n, size_t universe, size_t shift) {
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) {
        size_t base = (i / shift) % universe;
        size_t off = i % shift;
        result[i] = (base + off * 137) % universe;
    }
    return result;
}

static std::vector<uint64_t> genScanResistant(size_t n, size_t hotSet, size_t universe, std::mt19937_64& rng) {
    std::uniform_int_distribution<uint64_t> hotDist(0, hotSet - 1);
    std::uniform_int_distribution<uint64_t> coldDist(hotSet, universe - 1);
    std::bernoulli_distribution hotProb(0.9);
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) {
        result[i] = hotProb(rng) ? hotDist(rng) : coldDist(rng);
    }
    return result;
}

static std::vector<uint64_t> genLooping(size_t n, size_t loopSize) {
    std::vector<uint64_t> result(n);
    for (size_t i = 0; i < n; i++) result[i] = i % loopSize;
    return result;
}

// =====================================================================
//  BareARC (from RemarcBench, exact copy)
// =====================================================================

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
    uint64_t front() const { return list_.front(); }
    void pop_front() { if (!list_.empty()) { idx_.erase(list_.front()); list_.pop_front(); } }
    size_t size() const { return list_.size(); }
    bool empty() const { return list_.empty(); }
    auto rbegin() const { return list_.rbegin(); }
    auto rend() const { return list_.rend(); }
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

// =====================================================================
//  ArcFlatList: flat-array replacement for std::list-based ArcList
//  Same interface, O(1) promote via timestamp, O(n) eviction via scan
// =====================================================================

class ArcFlatList {
    std::vector<uint64_t> keys_;
    std::vector<uint64_t> timestamps_;
    std::unordered_map<uint64_t, size_t> idx_;
    uint64_t clock_ = 0;

    size_t findMinTs() const {
        size_t best = 0;
        for (size_t i = 1; i < timestamps_.size(); i++) {
            if (timestamps_[i] < timestamps_[best]) best = i;
        }
        return best;
    }

    size_t findMaxTs() const {
        size_t best = 0;
        for (size_t i = 1; i < timestamps_.size(); i++) {
            if (timestamps_[i] > timestamps_[best]) best = i;
        }
        return best;
    }

    void removeAt(size_t i) {
        idx_.erase(keys_[i]);
        size_t last = keys_.size() - 1;
        if (i != last) {
            keys_[i] = keys_[last];
            timestamps_[i] = timestamps_[last];
            idx_[keys_[i]] = i;
        }
        keys_.pop_back();
        timestamps_.pop_back();
    }

public:
    bool contains(uint64_t k) const { return idx_.count(k) > 0; }

    void push_front(uint64_t k) {
        keys_.push_back(k);
        timestamps_.push_back(++clock_);
        idx_[k] = keys_.size() - 1;
    }

    void pop_back() {
        if (keys_.empty()) return;
        removeAt(findMinTs());
    }

    void erase(uint64_t k) {
        auto it = idx_.find(k);
        if (it == idx_.end()) return;
        removeAt(it->second);
    }

    void splice_front(uint64_t k) {
        auto it = idx_.find(k);
        if (it != idx_.end()) timestamps_[it->second] = ++clock_;
    }

    uint64_t back() const {
        if (keys_.empty()) return 0;
        return keys_[findMinTs()];
    }

    uint64_t front() const {
        if (keys_.empty()) return 0;
        return keys_[findMaxTs()];
    }

    void pop_front() {
        if (keys_.empty()) return;
        removeAt(findMaxTs());
    }

    size_t size() const { return keys_.size(); }
    bool empty() const { return keys_.empty(); }
};

// =====================================================================
//  BareARC_Flat: exact ARC algorithm with ArcFlatList instead of ArcList
// =====================================================================

class BareARC_Flat {
    std::unordered_set<uint64_t> cached_;
    ArcFlatList t1_, t2_, b1_, b2_;
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
    explicit BareARC_Flat(size_t cap) : cap_(cap), p_(0) {}

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

// =====================================================================
//  ArcFlatLinkedList: doubly-linked list in flat array (no heap alloc)
//  O(1) everything, contiguous memory, cache-friendly for n <= 1000
// =====================================================================

class ArcFlatLinkedList {
    struct Slot {
        uint64_t key = 0;
        size_t prev = SIZE_MAX;
        size_t next = SIZE_MAX;
        bool active = false;
    };

    std::vector<Slot> slots_;
    std::unordered_map<uint64_t, size_t> idx_;
    size_t head_ = SIZE_MAX;
    size_t tail_ = SIZE_MAX;
    size_t freeHead_ = SIZE_MAX;
    size_t count_ = 0;

    size_t allocSlot() {
        if (freeHead_ != SIZE_MAX) {
            size_t s = freeHead_;
            freeHead_ = slots_[s].next;
            return s;
        }
        size_t s = slots_.size();
        slots_.emplace_back();
        return s;
    }

    void freeSlot(size_t s) {
        slots_[s].key = 0;
        slots_[s].prev = SIZE_MAX;
        slots_[s].next = freeHead_;
        slots_[s].active = false;
        freeHead_ = s;
    }

public:
    bool contains(uint64_t k) const { return idx_.count(k) > 0; }

    void push_front(uint64_t k) {
        size_t s = allocSlot();
        slots_[s].key = k;
        slots_[s].active = true;
        slots_[s].prev = SIZE_MAX;
        slots_[s].next = head_;
        if (head_ != SIZE_MAX) slots_[head_].prev = s;
        else tail_ = s;
        head_ = s;
        idx_[k] = s;
        count_++;
    }

    void pop_back() {
        if (tail_ == SIZE_MAX) return;
        size_t s = tail_;
        idx_.erase(slots_[s].key);
        size_t p = slots_[s].prev;
        if (p != SIZE_MAX) slots_[p].next = SIZE_MAX;
        else head_ = SIZE_MAX;
        tail_ = p;
        freeSlot(s);
        count_--;
    }

    void erase(uint64_t k) {
        auto it = idx_.find(k);
        if (it == idx_.end()) return;
        size_t s = it->second;
        idx_.erase(it);
        size_t p = slots_[s].prev, n = slots_[s].next;
        if (p != SIZE_MAX) slots_[p].next = n;
        else head_ = n;
        if (n != SIZE_MAX) slots_[n].prev = p;
        else tail_ = p;
        freeSlot(s);
        count_--;
    }

    void splice_front(uint64_t k) {
        auto it = idx_.find(k);
        if (it == idx_.end()) return;
        size_t s = it->second;
        if (s == head_) return;
        size_t p = slots_[s].prev, n = slots_[s].next;
        if (p != SIZE_MAX) slots_[p].next = n;
        if (n != SIZE_MAX) slots_[n].prev = p;
        else tail_ = p;
        slots_[s].prev = SIZE_MAX;
        slots_[s].next = head_;
        if (head_ != SIZE_MAX) slots_[head_].prev = s;
        head_ = s;
    }

    uint64_t back() const {
        return tail_ == SIZE_MAX ? 0 : slots_[tail_].key;
    }

    uint64_t front() const {
        return head_ == SIZE_MAX ? 0 : slots_[head_].key;
    }

    void pop_front() {
        if (head_ == SIZE_MAX) return;
        size_t s = head_;
        idx_.erase(slots_[s].key);
        size_t n = slots_[s].next;
        if (n != SIZE_MAX) slots_[n].prev = SIZE_MAX;
        else tail_ = SIZE_MAX;
        head_ = n;
        freeSlot(s);
        count_--;
    }

    size_t size() const { return count_; }
    bool empty() const { return count_ == 0; }
};

// =====================================================================
//  ArcFlatLinkedList2: fully optimized (flat hash, uint32_t, no cached_)
// =====================================================================

class ArcFlatLinkedList2 {
    static constexpr uint32_t NIL = UINT32_MAX;

    struct Slot {
        uint64_t key;
        uint32_t prev;
        uint32_t next;
        bool     active;
    };

    std::vector<Slot> slots_;
    std::unordered_map<uint64_t, uint32_t> idx_;
    uint32_t head_;
    uint32_t tail_;
    uint32_t freeHead_;
    uint32_t count_;

    uint32_t allocSlot() {
        uint32_t s = freeHead_;
        freeHead_ = slots_[s].next;
        return s;
    }

    void freeSlot(uint32_t s) {
        slots_[s].key = 0;
        slots_[s].prev = NIL;
        slots_[s].next = freeHead_;
        slots_[s].active = false;
        freeHead_ = s;
    }

public:
    explicit ArcFlatLinkedList2(uint32_t maxEntries) {
        slots_.resize(maxEntries);
        for (uint32_t i = 0; i < maxEntries; i++) {
            slots_[i] = {0, NIL, i + 1, false};
        }
        slots_[maxEntries - 1].next = NIL;
        freeHead_ = 0;

        idx_.reserve(maxEntries);

        head_ = NIL;
        tail_ = NIL;
        count_ = 0;
    }

    bool contains(uint64_t k) const { return idx_.count(k) > 0; }

    void push_front(uint64_t k) {
        uint32_t s = allocSlot();
        slots_[s].key = k;
        slots_[s].active = true;
        slots_[s].prev = NIL;
        slots_[s].next = head_;
        if (head_ != NIL) slots_[head_].prev = s;
        else tail_ = s;
        head_ = s;
        idx_[k] = s;
        count_++;
    }

    void pop_back() {
        if (tail_ == NIL) return;
        uint32_t s = tail_;
        idx_.erase(slots_[s].key);
        uint32_t p = slots_[s].prev;
        if (p != NIL) slots_[p].next = NIL;
        else head_ = NIL;
        tail_ = p;
        freeSlot(s);
        count_--;
    }

    void erase(uint64_t k) {
        auto it = idx_.find(k);
        if (it == idx_.end()) return;
        uint32_t s = it->second;
        idx_.erase(it);
        uint32_t p = slots_[s].prev, n = slots_[s].next;
        if (p != NIL) slots_[p].next = n;
        else head_ = n;
        if (n != NIL) slots_[n].prev = p;
        else tail_ = p;
        freeSlot(s);
        count_--;
    }

    void splice_front(uint64_t k) {
        auto it = idx_.find(k);
        if (it == idx_.end()) return;
        uint32_t s = it->second;
        if (s == head_) return;
        uint32_t p = slots_[s].prev, n = slots_[s].next;
        if (p != NIL) slots_[p].next = n;
        if (n != NIL) slots_[n].prev = p;
        else tail_ = p;
        slots_[s].prev = NIL;
        slots_[s].next = head_;
        if (head_ != NIL) slots_[head_].prev = s;
        head_ = s;
    }

    uint64_t back() const {
        return tail_ == NIL ? 0 : slots_[tail_].key;
    }

    uint64_t front() const {
        return head_ == NIL ? 0 : slots_[head_].key;
    }

    void pop_front() {
        if (head_ == NIL) return;
        uint32_t s = head_;
        idx_.erase(slots_[s].key);
        uint32_t n = slots_[s].next;
        if (n != NIL) slots_[n].prev = NIL;
        else tail_ = NIL;
        head_ = n;
        freeSlot(s);
        count_--;
    }

    size_t size() const { return count_; }
    bool empty() const { return count_ == 0; }
};

// =====================================================================
//  BareARC_FlatLL2: optimized ARC (flat hash, uint32_t, no cached_)
// =====================================================================

class BareARC_FlatLL2 {
    ArcFlatLinkedList2 t1_, t2_, b1_, b2_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;

    void replace(uint64_t key) {
        if (!t1_.empty() && (t1_.size() > p_ ||
            (b2_.contains(key) && t1_.size() == p_))) {
            uint64_t old = t1_.back();
            t1_.pop_back();
            b1_.push_front(old);
        } else if (!t2_.empty()) {
            uint64_t old = t2_.back();
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
                t1_.pop_back();
                evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) {
                b2_.pop_back();
            } else if (!t2_.empty()) {
                t2_.pop_back();
                evictions_++;
            }
        }
    }

public:
    explicit BareARC_FlatLL2(size_t cap)
        : cap_(cap), p_(0),
          t1_(cap * 2), t2_(cap * 2), b1_(cap * 2), b2_(cap * 2) {}

    void access(uint64_t key) {
        bool inT1 = t1_.contains(key);
        bool inT2 = t2_.contains(key);
        if (inT1 || inT2) {
            if (inT1) { t1_.erase(key); t2_.push_front(key); }
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
        if (ghost) t2_.push_front(key);
        else t1_.push_front(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
};

// =====================================================================
//  BareARC_FlatLL: exact ARC with ArcFlatLinkedList
// =====================================================================

class BareARC_FlatLL {
    std::unordered_set<uint64_t> cached_;
    ArcFlatLinkedList t1_, t2_, b1_, b2_;
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
    explicit BareARC_FlatLL(size_t cap) : cap_(cap), p_(0) {}

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

// =====================================================================
//  BareLRU (baseline)
// =====================================================================

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

// =====================================================================
//  BareREMARC (using Policy.h atoms — validates framework)
// =====================================================================

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
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;
    size_t decayInterval_;

    void decayAll() {
        using P = StandardRemarc;
        for (auto& pg : pages_) {
            for (auto& tc : pg.tempCtrl) {
                P::TimeDecayKey(tc, cfg_);
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
        using P = StandardRemarc;
        float best = -1.0f;
        size_t bi = SIZE_MAX;
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.empty()) continue;
            uint32_t en = 0;
            for (size_t o = 0; o < pages_[p].tempCtrl.size(); o += 32) {
                auto s = P::ScanBatch(pages_[p].tempCtrl.data(), o,
                    pages_[p].tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float ep = P::EPage(en, pages_[p].tempCtrl.size());
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
    using P = StandardRemarc;

    BareREMARC(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
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
            uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_);
            hits_++;
            return;
        }
        misses_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_LazyInc
//  - Lazy per-key decay (no global decay sweeps)
//  - Incremental eviction scan (bounded pages per miss)
// =====================================================================

class BareREMARC_LazyInc {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
        std::vector<uint32_t> epochTag;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    size_t scanCursor_ = 0;
    size_t scanBudgetPages_;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;
    size_t decayInterval_;
    uint32_t epoch_ = 0;

    float pageScore(const std::vector<uint8_t>& tc) const {
        if (tc.empty()) return -1.0f;
        uint32_t en = 0;
        for (size_t o = 0; o < tc.size(); o += 32) {
            auto s = StandardRemarc::ScanBatch(tc.data(), o, tc.size(), cfg_);
            en += s.ePageNumSum;
        }
        return StandardRemarc::EPage(en, tc.size());
    }

    inline void advanceEpoch() {
        if (++opCount_ % decayInterval_ == 0) {
            ++epoch_;
        }
    }

    inline uint8_t lazyDecay(uint8_t tc, uint32_t lastEpoch) const {
        uint32_t steps = epoch_ - lastEpoch;
        if (steps == 0) return tc;
        // Bound work per touch; large gaps collapse quickly anyway.
        if (steps > 8) steps = 8;
        for (uint32_t i = 0; i < steps; i++) {
            StandardRemarc::TimeDecayKey(tc, cfg_);
        }
        return tc;
    }

    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.size() < kpp_) { next_ = p; return; }
        }
        evictColdestApprox();
        for (size_t p = 0; p < maxPages_; p++) {
            if (pages_[p].keys.size() < kpp_) { next_ = p; return; }
        }
    }

    void evictColdestApprox() {
        if (pages_.empty()) return;
        size_t considered = 0;
        size_t bestPage = SIZE_MAX;
        float bestScore = -1.0f;

        while (considered < std::min(scanBudgetPages_, maxPages_)) {
            size_t p = scanCursor_;
            scanCursor_ = (scanCursor_ + 1) % maxPages_;
            considered++;
            scans_++;

            auto& pg = pages_[p];
            if (pg.keys.empty()) continue;

            // Lazy decay only for scanned pages.
            for (size_t i = 0; i < pg.tempCtrl.size(); i++) {
                uint8_t tc = lazyDecay(pg.tempCtrl[i], pg.epochTag[i]);
                pg.tempCtrl[i] = tc;
                pg.epochTag[i] = epoch_;
            }

            float score = pageScore(pg.tempCtrl);
            if (score > bestScore) {
                bestScore = score;
                bestPage = p;
            }
        }

        if (bestPage == SIZE_MAX) {
            // Fallback: first non-empty page
            for (size_t p = 0; p < maxPages_; p++) {
                if (!pages_[p].keys.empty()) { bestPage = p; break; }
            }
            if (bestPage == SIZE_MAX) return;
        }

        for (uint64_t k : pages_[bestPage].keys) map_.erase(k);
        evictions_ += pages_[bestPage].keys.size();
        pages_[bestPage].keys.clear();
        pages_[bestPage].tempCtrl.clear();
        pages_[bestPage].epochTag.clear();
    }

public:
    BareREMARC_LazyInc(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                       size_t decayInterval = 64, size_t scanBudgetPages = 8)
        : kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          scanBudgetPages_(scanBudgetPages), cfg_(cfg), decayInterval_(decayInterval) {
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        advanceEpoch();

        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            size_t idx = it->second.second;
            uint8_t tc = lazyDecay(pg.tempCtrl[idx], pg.epochTag[idx]);
            tc = StandardRemarc::OnLocalAccess(tc, cfg_);
            pg.tempCtrl[idx] = tc;
            pg.epochTag[idx] = epoch_;
            hits_++;
            return;
        }

        misses_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(StandardRemarc::InitialState());
        pg.epochTag.push_back(epoch_);
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_Opt
//  - No per-hit decay (only promote/demote the 4-bit nibble)
//  - Lazy decay applied only during eviction scan
//  - Tight scan budget (4 pages per miss) + cursor rotation
//  - No page score caching (dirty flag useless on hit-heavy workloads)
// =====================================================================

class BareREMARC_Opt {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    size_t scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;

    using P = StandardRemarc;

    void decayPage(PageData& pg) {
        for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_);
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
        if (pages_.empty()) return;
        size_t considered = 0;
        size_t bestPage = SIZE_MAX;
        float bestScore = -1.0f;

        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_;
            scanCursor_ = (scanCursor_ + 1) % maxPages_;
            considered++;
            scans_++;

            auto& pg = pages_[p];
            if (pg.keys.empty()) continue;

            decayPage(pg);

            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) {
                auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) {
                bestScore = score;
                bestPage = p;
            }
        }

        if (bestPage == SIZE_MAX) {
            for (size_t p = 0; p < maxPages_; p++) {
                if (!pages_[p].keys.empty()) { bestPage = p; break; }
            }
            if (bestPage == SIZE_MAX) return;
        }

        for (uint64_t k : pages_[bestPage].keys) map_.erase(k);
        evictions_ += pages_[bestPage].keys.size();
        pages_[bestPage].keys.clear();
        pages_[bestPage].tempCtrl.clear();
    }

public:
    BareREMARC_Opt(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                   size_t = 64)
        : kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg) {
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_);
            hits_++;
            return;
        }

        misses_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_S3FIFO
//  REMARC-OPT core + S3-FIFO admission filter (two-bit history).
//  On miss: if key has "seen" flag, admit normally.
//           if key is new, mark "seen" and reject (ghost admission).
//  Adapted from S3-FIFO's small/large queue idea: one pass through
//  ghost before real admission gives scan resistance.
// =====================================================================

class BareREMARC_S3FIFO {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    std::unordered_set<uint64_t> ghost_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    size_t scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t ghostHits_ = 0;

    using P = StandardRemarc;

    void decayPage(PageData& pg) {
        for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_);
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
        if (pages_.empty()) return;
        size_t considered = 0;
        size_t bestPage = SIZE_MAX;
        float bestScore = -1.0f;

        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_;
            scanCursor_ = (scanCursor_ + 1) % maxPages_;
            considered++;
            scans_++;

            auto& pg = pages_[p];
            if (pg.keys.empty()) continue;

            decayPage(pg);

            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) {
                auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) {
                bestScore = score;
                bestPage = p;
            }
        }

        if (bestPage == SIZE_MAX) {
            for (size_t p = 0; p < maxPages_; p++) {
                if (!pages_[p].keys.empty()) { bestPage = p; break; }
            }
            if (bestPage == SIZE_MAX) return;
        }

        for (uint64_t k : pages_[bestPage].keys) {
            ghost_.insert(k);
            map_.erase(k);
        }
        evictions_ += pages_[bestPage].keys.size();
        pages_[bestPage].keys.clear();
        pages_[bestPage].tempCtrl.clear();
    }

public:
    BareREMARC_S3FIFO(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                      size_t = 64)
        : kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg) {
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_);
            hits_++;
            return;
        }

        misses_++;

        if (ghost_.count(key)) {
            ghost_.erase(key);
            ghostHits_++;
        } else {
            ghost_.insert(key);
            return;
        }

        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_LFU
//  Pure frequency+recency composition, no local/remote distinction.
//  AtomA = Recency (starts at MAX, boosted on access)
//  AtomB = Frequency (starts at 0, boosted on access)
//  Both decay during eviction scan only.
// =====================================================================

struct AtomRecency {
    static constexpr uint8_t Initial = REMARC_MAX;
    static uint8_t Promote(uint8_t s, uint8_t alpha) noexcept { return RemarcBoost(s, alpha); }
    static uint8_t Demote(uint8_t s, uint8_t) noexcept { return s; }
    static uint8_t TimeDecay(uint8_t s, uint8_t num, uint8_t den) noexcept { return RemarcTimeDecay(s, num, den); }
};

struct AtomFrequency {
    static constexpr uint8_t Initial = 0;
    static uint8_t Promote(uint8_t s, uint8_t alpha) noexcept { return RemarcBoost(s, alpha); }
    static uint8_t Demote(uint8_t s, uint8_t) noexcept { return s; }
    static uint8_t TimeDecay(uint8_t s, uint8_t num, uint8_t den) noexcept { return RemarcTimeDecay(s, num, den); }
};

using LFUPolicy = RemarcPolicy<AtomRecency, AtomFrequency>;

class BareREMARC_LFU {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    size_t scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;

    using P = LFUPolicy;

    void decayPage(PageData& pg) {
        for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_);
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
        if (pages_.empty()) return;
        size_t considered = 0;
        size_t bestPage = SIZE_MAX;
        float bestScore = -1.0f;

        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_;
            scanCursor_ = (scanCursor_ + 1) % maxPages_;
            considered++;
            scans_++;

            auto& pg = pages_[p];
            if (pg.keys.empty()) continue;

            decayPage(pg);

            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) {
                auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) {
                bestScore = score;
                bestPage = p;
            }
        }

        if (bestPage == SIZE_MAX) {
            for (size_t p = 0; p < maxPages_; p++) {
                if (!pages_[p].keys.empty()) { bestPage = p; break; }
            }
            if (bestPage == SIZE_MAX) return;
        }

        for (uint64_t k : pages_[bestPage].keys) map_.erase(k);
        evictions_ += pages_[bestPage].keys.size();
        pages_[bestPage].keys.clear();
        pages_[bestPage].tempCtrl.clear();
    }

public:
    BareREMARC_LFU(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                   size_t = 64)
        : kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg) {
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_);
            hits_++;
            return;
        }

        misses_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_TinyLFU
//  REMARC-OPT core + CountMinSketch frequency sketch for admission.
//  On miss: check sketch frequency. If freq < threshold, reject (ghost).
//  Sketch is updated on every access (hit or miss).
// =====================================================================

class CountMinSketch {
    static constexpr size_t kRows = 4;
    static constexpr size_t kCols = 256;
    std::array<std::array<uint8_t, kCols>, kRows> table_{};
    uint64_t totalInserts_ = 0;

    static size_t hashRow(uint64_t key, size_t row) {
        uint64_t h = key * 2654435761ULL + row * 2246822519ULL;
        return (h >> 32) % kCols;
    }

public:
    void increment(uint64_t key) {
        for (size_t r = 0; r < kRows; r++) {
            size_t c = hashRow(key, r);
            if (table_[r][c] < 255) table_[r][c]++;
        }
        totalInserts_++;
    }

    uint8_t estimate(uint64_t key) const {
        uint8_t minVal = 255;
        for (size_t r = 0; r < kRows; r++) {
            minVal = std::min(minVal, table_[r][hashRow(key, r)]);
        }
        return minVal;
    }

    void reset() {
        for (auto& row : table_) row.fill(0);
        totalInserts_ = 0;
    }
};

class BareREMARC_TinyLFU {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };

    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    CountMinSketch sketch_;
    size_t kpp_;
    size_t maxPages_;
    size_t next_ = 0;
    size_t scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    uint8_t admitThreshold_ = 3;

    using P = StandardRemarc;

    void decayPage(PageData& pg) {
        for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_);
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
        if (pages_.empty()) return;
        size_t considered = 0;
        size_t bestPage = SIZE_MAX;
        float bestScore = -1.0f;

        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_;
            scanCursor_ = (scanCursor_ + 1) % maxPages_;
            considered++;
            scans_++;

            auto& pg = pages_[p];
            if (pg.keys.empty()) continue;

            decayPage(pg);

            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) {
                auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_);
                en += s.ePageNumSum;
            }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) {
                bestScore = score;
                bestPage = p;
            }
        }

        if (bestPage == SIZE_MAX) {
            for (size_t p = 0; p < maxPages_; p++) {
                if (!pages_[p].keys.empty()) { bestPage = p; break; }
            }
            if (bestPage == SIZE_MAX) return;
        }

        for (uint64_t k : pages_[bestPage].keys) map_.erase(k);
        evictions_ += pages_[bestPage].keys.size();
        pages_[bestPage].keys.clear();
        pages_[bestPage].tempCtrl.clear();
    }

public:
    BareREMARC_TinyLFU(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                       size_t = 64, uint8_t admitThreshold = 3)
        : kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg), admitThreshold_(admitThreshold) {
        pages_.resize(maxPages_);
    }

    void access(uint64_t key) {
        sketch_.increment(key);

        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first];
            uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_);
            hits_++;
            return;
        }

        misses_++;

        if (sketch_.estimate(key) < admitThreshold_) return;

        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;

        auto& pg = pages_[next_];
        map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return scans_; }
};

// =====================================================================
//  Bounded ghost field for ghost atom implementations.
//  Stores (hash → score) with LRU eviction when full.
// =====================================================================

class GhostField {
    static constexpr size_t kBucketBits = 12;
    static constexpr size_t kNumBuckets = 1u << kBucketBits;
    static constexpr size_t kBucketMask = kNumBuckets - 1;

    struct Entry {
        uint64_t key;
        uint8_t score;
        uint64_t seq;
        uint32_t next;
    };
    std::vector<Entry> entries_;
    std::vector<uint32_t> heads_;
    uint32_t freeHead_ = 0;
    uint32_t capacity_;
    uint32_t size_ = 0;
    uint64_t seq_ = 0;

    uint32_t allocEntry() {
        if (freeHead_ >= capacity_) return UINT32_MAX;
        return freeHead_++;
    }

    static uint32_t hashKey(uint64_t k) {
        k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return static_cast<uint32_t>(k & kBucketMask);
    }

public:
    explicit GhostField(size_t capacity)
        : capacity_(static_cast<uint32_t>(capacity)) {
        entries_.resize(capacity + 1);
        heads_.assign(kNumBuckets, UINT32_MAX);
    }

    void upsert(uint64_t key, uint8_t score) {
        uint32_t h = hashKey(key);
        uint32_t* prev = &heads_[h];
        while (*prev != UINT32_MAX) {
            if (entries_[*prev].key == key) {
                entries_[*prev].score = score;
                entries_[*prev].seq = seq_++;
                return;
            }
            prev = &entries_[*prev].next;
        }
        uint32_t e = allocEntry();
        if (e == UINT32_MAX) {
            uint64_t minSeq = entries_[0].seq;
            uint32_t victim = 0;
            for (uint32_t i = 1; i < capacity_; i++) {
                if (entries_[i].seq < minSeq) { minSeq = entries_[i].seq; victim = i; }
            }
            uint32_t vh = hashKey(entries_[victim].key);
            uint32_t* vp = &heads_[vh];
            while (*vp != victim) vp = &entries_[*vp].next;
            *vp = entries_[victim].next;
            e = victim;
        }
        entries_[e] = {key, score, seq_++, heads_[h]};
        heads_[h] = e;
        size_++;
    }

    std::optional<uint8_t> query(uint64_t key) {
        uint32_t h = hashKey(key);
        uint32_t e = heads_[h];
        while (e != UINT32_MAX) {
            if (entries_[e].key == key) {
                entries_[e].seq = seq_++;
                return entries_[e].score;
            }
            e = entries_[e].next;
        }
        return std::nullopt;
    }

    size_t size() const { return size_; }
};

// =====================================================================
//  BareREMARC_GhostField  (graded ghost — field atom A_ghost)
//
//  E = P(A_local, A_remote, A_ghost)
//  On eviction: ghost.upsert(K, max(s_local, s_remote))
//  On miss: ghost.has(K) → admit with boosted initial state; else → normal
//  Ghost signal decays on query (older evictions = weaker signal).
// =====================================================================

class BareREMARC_GhostField {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
    };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    GhostField ghost_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    uint8_t ghostThreshold_ = 4;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0, ghostAdmits_ = 0;
    using P = StandardRemarc;

    void decayPage(PageData& pg) { for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_); }

    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX;
        float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[p]; if (pg.keys.empty()) continue;
            decayPage(pg);
            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) { auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_); en += s.ePageNumSum; }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) { bestScore = score; bestPage = p; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) {
            uint64_t k = pages_[bestPage].keys[i]; uint8_t tc = pages_[bestPage].tempCtrl[i];
            ghost_.upsert(k, std::max(NuAtlas::UnpackSLocal(tc), NuAtlas::UnpackSRemote(tc)));
            map_.erase(k);
        }
        evictions_ += pages_[bestPage].keys.size();
        pages_[bestPage].keys.clear(); pages_[bestPage].tempCtrl.clear();
    }
public:
    BareREMARC_GhostField(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                          size_t = 64, uint8_t ghostThreshold = 4)
        : ghost_(capacity * 2), kpp_(keysPerPage),
          maxPages_((capacity + keysPerPage - 1) / keysPerPage), cfg_(cfg), ghostThreshold_(ghostThreshold) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) { auto& pg = pages_[it->second.first]; uint8_t& tc = pg.tempCtrl[it->second.second]; tc = P::OnLocalAccess(tc, cfg_); hits_++; return; }
        misses_++;
        uint8_t ghostScore = 0;
        auto gs = ghost_.query(key);
        if (gs.has_value()) { ghostScore = *gs; if (ghostScore > 0) ghostScore = std::max(uint8_t(1), static_cast<uint8_t>(ghostScore * cfg_.TimeDecayNum / cfg_.TimeDecayDen)); }
        uint8_t initTC = (ghostScore >= ghostThreshold_) ? PackTempCtrl(REMARC_MAX, std::min(ghostScore, REMARC_MAX)) : P::InitialState();
        if (ghostScore >= ghostThreshold_) ghostAdmits_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key); pg.tempCtrl.push_back(initTC);
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_GhostGate  (graded ghost gate — one-pass admission)
//
//  On eviction: ghost.upsert(K, score)
//  On miss: ghost.has(K) → pass gate, admit; else → insert ghost, REJECT
// =====================================================================

class BareREMARC_GhostGate {
    struct PageData { std::vector<uint64_t> keys; std::vector<uint8_t> tempCtrl; };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    GhostField ghost_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0, ghostPasses_ = 0;
    using P = StandardRemarc;

    void decayPage(PageData& pg) { for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_); }
    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[p]; if (pg.keys.empty()) continue; decayPage(pg);
            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) { auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_); en += s.ePageNumSum; }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) { bestScore = score; bestPage = p; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) {
            uint64_t k = pages_[bestPage].keys[i]; uint8_t tc = pages_[bestPage].tempCtrl[i];
            ghost_.upsert(k, std::max(NuAtlas::UnpackSLocal(tc), NuAtlas::UnpackSRemote(tc)));
            map_.erase(k);
        }
        evictions_ += pages_[bestPage].keys.size(); pages_[bestPage].keys.clear(); pages_[bestPage].tempCtrl.clear();
    }
public:
    BareREMARC_GhostGate(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg, size_t = 64)
        : ghost_(capacity * 2), kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage), cfg_(cfg) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) { auto& pg = pages_[it->second.first]; uint8_t& tc = pg.tempCtrl[it->second.second]; tc = P::OnLocalAccess(tc, cfg_); hits_++; return; }
        misses_++;
        if (ghost_.query(key).has_value()) { ghostPasses_++; }
        else { ghost_.upsert(key, 1); return; }
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key); pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  Bloom filter — fixed bitmap, no per-key allocation, O(1) query/set.
//  Implements binary ghost atom A_ghost: S_ghost = {0, 1}.
// =====================================================================

class BloomFilter {
    std::vector<uint64_t> bits_;
    size_t numBits_;
    size_t count_ = 0;

    static uint64_t hash1(uint64_t k) {
        k ^= k >> 33; k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33; k *= 0xc4ceb9fe1a85ec53ULL;
        return k;
    }
    static uint64_t hash2(uint64_t k) {
        k ^= k >> 29; k *= 0x9e3779b97f4a7c15ULL;
        return k;
    }
    static uint64_t hash3(uint64_t k) {
        k ^= k >> 17; k *= 0xbf58476d1ce4e5b9ULL;
        return k;
    }

public:
    explicit BloomFilter(size_t numBits) : numBits_(numBits) {
        bits_.assign((numBits + 63) / 64, 0);
    }
    void set(uint64_t key) {
        size_t n = numBits_;
        bits_[hash1(key) % n / 64] |= 1ULL << (hash1(key) % n % 64);
        bits_[hash2(key) % n / 64] |= 1ULL << (hash2(key) % n % 64);
        bits_[hash3(key) % n / 64] |= 1ULL << (hash3(key) % n % 64);
        count_++;
    }
    bool test(uint64_t key) const {
        size_t n = numBits_;
        return (bits_[hash1(key) % n / 64] >> (hash1(key) % n % 64)) & 1 &&
               (bits_[hash2(key) % n / 64] >> (hash2(key) % n % 64)) & 1 &&
               (bits_[hash3(key) % n / 64] >> (hash3(key) % n % 64)) & 1;
    }
    void clear() { bits_.assign(bits_.size(), 0); count_ = 0; }
    size_t count() const { return count_; }
};

// =====================================================================
//  BareREMARC_BloomGate
//  REMARC-OPT core + Bloom filter as admission gate (binary ghost atom).
//  E = P(A_local, A_remote) gated by A_ghost ∈ {0,1} via Bloom filter.
//
//  On eviction: bloom.set(K)
//  On miss: bloom.test(K) → pass gate, admit; else → bloom.set(K), REJECT
//  Filter reset every resetInterval operations to prevent saturation.
// =====================================================================

class BareREMARC_BloomGate {
    struct PageData { std::vector<uint64_t> keys; std::vector<uint8_t> tempCtrl; };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    BloomFilter bloom_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0, ghostPasses_ = 0;
    size_t opCount_ = 0;
    size_t resetInterval_;
    using P = StandardRemarc;

    void decayPage(PageData& pg) { for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_); }
    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[p]; if (pg.keys.empty()) continue; decayPage(pg);
            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) { auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_); en += s.ePageNumSum; }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) { bestScore = score; bestPage = p; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) bloom_.set(pages_[bestPage].keys[i]);
        evictions_ += pages_[bestPage].keys.size();
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) map_.erase(pages_[bestPage].keys[i]);
        pages_[bestPage].keys.clear(); pages_[bestPage].tempCtrl.clear();
    }
public:
    BareREMARC_BloomGate(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                         size_t = 64, size_t resetInterval = 4096)
        : bloom_(1 << 17),
          kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg), resetInterval_(resetInterval) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        if (++opCount_ % resetInterval_ == 0) bloom_.clear();
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first]; uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_); hits_++; return;
        }
        misses_++;
        if (bloom_.test(key)) { ghostPasses_++; }
        else { bloom_.set(key); return; }
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key); pg.tempCtrl.push_back(P::InitialState());
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_BloomMod  (gate-to-modulate — bloom as initial state modifier)
//
//  Same algebra as BLOOM gate: A_ghost ∈ {0, 1} via Bloom filter.
//  But instead of rejecting on first miss, ALWAYS admit — the ghost signal
//  modulates the initial state:
//    ghost=false (brand new key)  → initial = (MAX, 0)   [full recency]
//    ghost=true  (recently evicted) → initial = (0, 0)     [cold admission]
//
//  This preserves temporal shift behavior (new keys get full state)
//  while adding scan resistance (repeat-evicted keys get penalized).
//  No false-positive damage: false positives only reduce initial state,
//  they never reject.
// =====================================================================

class BareREMARC_BloomMod {
    struct PageData { std::vector<uint64_t> keys; std::vector<uint8_t> tempCtrl; };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    BloomFilter bloom_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0, ghostModulates_ = 0;
    size_t opCount_ = 0;
    size_t resetInterval_;
    using P = StandardRemarc;

    void decayPage(PageData& pg) { for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_); }
    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[p]; if (pg.keys.empty()) continue; decayPage(pg);
            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) { auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_); en += s.ePageNumSum; }
            float score = P::EPage(en, pg.tempCtrl.size());
            if (score > bestScore) { bestScore = score; bestPage = p; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) bloom_.set(pages_[bestPage].keys[i]);
        evictions_ += pages_[bestPage].keys.size();
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) map_.erase(pages_[bestPage].keys[i]);
        pages_[bestPage].keys.clear(); pages_[bestPage].tempCtrl.clear();
    }
public:
    BareREMARC_BloomMod(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                         size_t = 64, size_t resetInterval = 4096)
        : bloom_(1 << 17),
          kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg), resetInterval_(resetInterval) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        if (++opCount_ % resetInterval_ == 0) bloom_.clear();
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first]; uint8_t& tc = pg.tempCtrl[it->second.second];
            tc = P::OnLocalAccess(tc, cfg_); hits_++; return;
        }
        misses_++;
        bool ghost = bloom_.test(key);
        bloom_.set(key);
        uint8_t initTC = ghost ? PackTempCtrl(0, 0) : P::InitialState();
        if (ghost) ghostModulates_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key); pg.tempCtrl.push_back(initTC);
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_Factorized  (multi-timescale: S = S_fast ⊗ S_slow)
//
//  State space factored into two independent temporal scales:
//    S_fast: decay rate num/den (default 7/8)  — recency timescale
//    S_slow: decay rate sn/sd   (default 15/16) — frequency timescale
//
//  E = w_fast * EPage(S_fast) + w_slow * EPage(S_slow)
//
//  New keys: fast = (MAX, 0), slow = (0, 0)
//  On access: boost both fast AND slow (builds frequency evidence)
//  The two timescales make REMARC's implicit multi-timescale explicit.
// =====================================================================

class BareREMARC_Factorized {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempFast;
        std::vector<uint8_t> tempSlow;
    };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfgFast_;
    uint8_t slowNum_, slowDen_;
    float wFast_, wSlow_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    using P = StandardRemarc;

    void decayPage(PageData& pg) {
        for (auto& tc : pg.tempFast) P::TimeDecayKey(tc, cfgFast_);
        for (auto& tc : pg.tempSlow) {
            uint8_t a = AtomSLocal::TimeDecay(UnpackSLocal(tc), slowNum_, slowDen_);
            uint8_t b = AtomSRemote::TimeDecay(UnpackSRemote(tc), slowNum_, slowDen_);
            tc = PackTempCtrl(a, b);
        }
    }
    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    float scorePage(size_t p) {
        auto& pg = pages_[p]; if (pg.keys.empty()) return -1.0f;
        decayPage(pg);
        uint32_t enFast = 0, enSlow = 0;
        for (size_t o = 0; o < pg.tempFast.size(); o += 32) {
            auto sf = P::ScanBatch(pg.tempFast.data(), o, pg.tempFast.size(), cfgFast_);
            enFast += sf.ePageNumSum;
        }
        for (size_t o = 0; o < pg.tempSlow.size(); o += 32) {
            auto ss = P::ScanBatch(pg.tempSlow.data(), o, pg.tempSlow.size(), cfgFast_);
            enSlow += ss.ePageNumSum;
        }
        size_t n = pg.tempFast.size();
        return wFast_ * P::EPage(enFast, n) + wSlow_ * P::EPage(enSlow, n);
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            float score = scorePage(p);
            if (score > bestScore) { bestScore = score; bestPage = p; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        evictions_ += pages_[bestPage].keys.size();
        for (size_t i = 0; i < pages_[bestPage].keys.size(); i++) map_.erase(pages_[bestPage].keys[i]);
        pages_[bestPage].keys.clear(); pages_[bestPage].tempFast.clear(); pages_[bestPage].tempSlow.clear();
    }
public:
    BareREMARC_Factorized(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                           float wFast = 0.5f, float wSlow = 0.5f,
                           uint8_t slowNum = 15, uint8_t slowDen = 16)
        : cfgFast_(cfg), slowNum_(slowNum), slowDen_(slowDen),
          wFast_(wFast), wSlow_(wSlow),
          kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first]; size_t idx = it->second.second;
            pg.tempFast[idx] = P::OnLocalAccess(pg.tempFast[idx], cfgFast_);
            pg.tempSlow[idx] = P::OnLocalAccess(pg.tempSlow[idx], cfgFast_);
            hits_++; return;
        }
        misses_++;
        uint8_t initFast = P::InitialState();
        uint8_t initSlow = PackTempCtrl(0, 0);
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key); pg.tempFast.push_back(initFast); pg.tempSlow.push_back(initSlow);
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_TierGhost  (ARC-inspired: tier atom + directional ghost)
//
//  Per-key tier atom: A_tier ∈ {0, 1}
//    0 = T1 (admitted this residency, not yet promoted)
//    1 = T2 (promoted on first hit in residency)
//
//  Directional ghost via two bloom filters:
//    bloomT1 = keys evicted from T1
//    bloomT2 = keys evicted from T2
//    A_ghost ∈ {0, 1, 2} = {none, from_T1, from_T2}
//
//  Adaptive p (global tier pressure):
//    Re-access of T1 ghost → p++ (T1 was too small)
//    Re-access of T2 ghost → p-- (T2 was too small)
//
//  Eviction: E_page * (1 + tierRatio * tierWeight)
//    Prefers evicting pages from the over-quota tier.
//
//  Algebra: E = P(A_fast, A_tier, A_ghost_direction)
//  Unifies desire-encoding (container identity) with REMARC projection.
// =====================================================================

class BareREMARC_TierGhost {
    struct PageData {
        std::vector<uint64_t> keys;
        std::vector<uint8_t> tempCtrl;
        std::vector<uint8_t> tier;
    };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    BloomFilter bloomT1_, bloomT2_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;
    size_t resetInterval_;
    size_t p_;
    size_t tier0Count_ = 0;
    float tierWeight_;
    size_t pAdjusts_ = 0;
    using P = StandardRemarc;

    void decayPage(PageData& pg) { for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_); }
    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t pg_idx = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[pg_idx]; if (pg.keys.empty()) continue; decayPage(pg);
            uint32_t en = 0;
            for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) { auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_); en += s.ePageNumSum; }
            float baseScore = P::EPage(en, pg.tempCtrl.size());
            int t0 = 0;
            for (auto t : pg.tier) if (t == 0) t0++;
            float tierRatio;
            if (tier0Count_ >= p_) {
                tierRatio = (float)t0 / (float)pg.tier.size();
            } else {
                tierRatio = (float)(pg.tier.size() - t0) / (float)pg.tier.size();
            }
            float score = baseScore * (1.0f + tierRatio * tierWeight_);
            if (score > bestScore) { bestScore = score; bestPage = pg_idx; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        auto& victim = pages_[bestPage];
        for (size_t i = 0; i < victim.keys.size(); i++) {
            if (victim.tier[i] == 0) { bloomT1_.set(victim.keys[i]); tier0Count_--; }
            else { bloomT2_.set(victim.keys[i]); }
            map_.erase(victim.keys[i]);
        }
        evictions_ += victim.keys.size();
        victim.keys.clear(); victim.tempCtrl.clear(); victim.tier.clear();
    }
public:
    BareREMARC_TierGhost(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                          float tierWeight = 2.0f, size_t resetInterval = 4096)
        : bloomT1_(1 << 17), bloomT2_(1 << 17),
          kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg), resetInterval_(resetInterval),
          p_(capacity / 2), tierWeight_(tierWeight) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        if (++opCount_ % resetInterval_ == 0) { bloomT1_.clear(); bloomT2_.clear(); }
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first]; size_t idx = it->second.second;
            pg.tempCtrl[idx] = P::OnLocalAccess(pg.tempCtrl[idx], cfg_);
            if (pg.tier[idx] == 0) { pg.tier[idx] = 1; tier0Count_--; }
            hits_++; return;
        }
        misses_++;
        bool inT1 = bloomT1_.test(key);
        bool inT2 = bloomT2_.test(key);
        if (inT1 && !inT2) { p_ = std::min(p_ + 1, maxPages_ * kpp_); pAdjusts_++; }
        else if (inT2 && !inT1) { p_ = (p_ > 0) ? p_ - 1 : 0; pAdjusts_++; }
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key); pg.tempCtrl.push_back(P::InitialState()); pg.tier.push_back(0);
        tier0Count_++;
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_Dual  (two concurrent REMARC-OPT instances, compositionally)
//
//  E = P(A_recency, A_frequency) decomposes into two independent caches:
//
//    Cache_fast (recency, decay 7/8, cap = p)  — T1-equivalent
//    Cache_slow (frequency, decay slowNum/slowDen, cap = c - p) — T2-equivalent
//
//  Admission: new key → cache_fast (tier 0)
//  Promotion: 1st hit in cache_fast → migrate to cache_slow (tier 1)
//  Re-access: hit in cache_slow → boost in-place
//
//  Directional ghost (two blooms):
//    bloomT1 set on eviction from cache_fast
//    bloomT2 set on eviction from cache_slow
//    Re-access of T1 ghost → p++ (grow recency)
//    Re-access of T2 ghost → p-- (grow frequency)
//
//  Each instance is a complete, autonomous REMARC-OPT with its own pages,
//  state, eviction cursor. No shared data. Pure algebraic composition.
//  The framework ALLOWS this — it's not a bolt-on.
// =====================================================================

class BareREMARC_Dual {
    struct SubCache {
        struct PageData { std::vector<uint64_t> keys; std::vector<uint8_t> tempCtrl; };
        std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
        std::vector<PageData> pages_;
        size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
        static constexpr size_t kScanBudget = 4;
        RemarcConfig cfg_;
        size_t evictions_ = 0, scans_ = 0;
        size_t count_ = 0;
        using P = StandardRemarc;

        SubCache(size_t maxPages, size_t kpp, const RemarcConfig& cfg)
            : kpp_(kpp), maxPages_(std::min(maxPages, (size_t)64)), cfg_(cfg) { pages_.resize(maxPages_); }

        void decayPage(PageData& pg) { for (auto& tc : pg.tempCtrl) P::TimeDecayKey(tc, cfg_); }
        void allocSlot() {
            for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
            evictColdest();
            for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        }
        void evictColdest() {
            if (pages_.empty()) return;
            size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
            while (considered < std::min(kScanBudget, maxPages_)) {
                size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
                auto& pg = pages_[p]; if (pg.keys.empty()) continue; decayPage(pg);
                uint32_t en = 0;
                for (size_t o = 0; o < pg.tempCtrl.size(); o += 32) { auto s = P::ScanBatch(pg.tempCtrl.data(), o, pg.tempCtrl.size(), cfg_); en += s.ePageNumSum; }
                float score = P::EPage(en, pg.tempCtrl.size());
                if (score > bestScore) { bestScore = score; bestPage = p; }
            }
            if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
            auto& victim = pages_[bestPage];
            for (size_t i = 0; i < victim.keys.size(); i++) map_.erase(victim.keys[i]);
            evictions_ += victim.keys.size(); count_ -= victim.keys.size();
            victim.keys.clear(); victim.tempCtrl.clear();
        }
        bool access(uint64_t key) {
            auto it = map_.find(key);
            if (it != map_.end()) {
                auto& pg = pages_[it->second.first]; pg.tempCtrl[it->second.second] = P::OnLocalAccess(pg.tempCtrl[it->second.second], cfg_);
                return true;
            }
            if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
            if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return false;
            auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
            pg.keys.push_back(key); pg.tempCtrl.push_back(P::InitialState());
            count_++;
            if (pg.keys.size() >= kpp_) next_++;
            return false;
        }
        bool remove(uint64_t key) {
            auto it = map_.find(key);
            if (it == map_.end()) return false;
            auto& pg = pages_[it->second.first];
            size_t idx = it->second.second;
            map_.erase(key); count_--;
            pg.keys[idx] = pg.keys.back(); pg.keys.pop_back();
            pg.tempCtrl[idx] = pg.tempCtrl.back(); pg.tempCtrl.pop_back();
            if (!pg.keys.empty()) map_[pg.keys[idx]] = {it->second.first, idx};
            return true;
        }
        void setEvictBloom(BloomFilter& bloom) {
            for (auto& pg : pages_) for (auto k : pg.keys) bloom.set(k);
        }
    };

    SubCache fast_, slow_;
    BloomFilter bloomT1_, bloomT2_;
    size_t cap_, p_;
    uint8_t slowDecayNum_, slowDecayDen_;
    size_t hits_ = 0, misses_ = 0, opCount_ = 0;
    size_t resetInterval_;
    size_t promotions_ = 0, pAdjusts_ = 0;

    void slowDecaySweep() {
        for (auto& pg : slow_.pages_) {
            for (auto& tc : pg.tempCtrl) {
                uint8_t a = AtomSLocal::TimeDecay(UnpackSLocal(tc), slowDecayNum_, slowDecayDen_);
                uint8_t b = AtomSRemote::TimeDecay(UnpackSRemote(tc), slowDecayNum_, slowDecayDen_);
                tc = PackTempCtrl(a, b);
            }
        }
    }
public:
    BareREMARC_Dual(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                     float slowRatio = 0.5f, uint8_t slowNum = 15, uint8_t slowDen = 16,
                     size_t resetInterval = 4096)
        : bloomT1_(1 << 17), bloomT2_(1 << 17),
          cap_(capacity), p_((size_t)(capacity * slowRatio)),
          slowDecayNum_(slowNum), slowDecayDen_(slowDen),
          resetInterval_(resetInterval),
          fast_((capacity - p_) / keysPerPage + 1, keysPerPage, cfg),
          slow_(p_ / keysPerPage + 1, keysPerPage, cfg) {}
    void access(uint64_t key) {
        if (++opCount_ % resetInterval_ == 0) { bloomT1_.clear(); bloomT2_.clear(); }
        if (slow_.access(key)) { hits_++; return; }
        if (fast_.access(key)) {
            hits_++;
            fast_.remove(key);
            slow_.access(key);
            return;
        }
        misses_++;
        bool inT1 = bloomT1_.test(key);
        bool inT2 = bloomT2_.test(key);
        if (inT1 && !inT2) { size_t delta = std::max((size_t)1, cap_ / 100); p_ = std::min(p_ + delta, cap_); pAdjusts_++; }
        else if (inT2 && !inT1) { size_t delta = std::max((size_t)1, cap_ / 100); p_ = (p_ > delta) ? p_ - delta : 0; pAdjusts_++; }
        fast_.access(key);
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return fast_.evictions_ + slow_.evictions_; }
    size_t scans() const { return fast_.scans_ + slow_.scans_; }
};

// =====================================================================
//  BareREMARC_MultiScale  (f_{S_τ}: per-atom decay + feedback loop)
//
//  State space S_τ = {0,...,15} × {0,...,15}:
//    low nibble  = recency  (AtomRecency: fast boost α_R, fast decay τ_R)
//    high nibble = frequency (AtomFrequency: slow boost α_F, slow decay τ_F)
//
//  Both atoms are PROTECTIVE (high = hard to evict).
//  Projection: E(s_r, s_f) = (15 - s_r) + w_freq * (15 - s_f)
//
//  Per-atom decay:
//    Fast sweep (on page scan): decay recency only at τ_R = 7/8
//    Slow sweep (global, every M ops): decay frequency only at τ_F = 15/16
//
//  Directional ghost (two blooms):
//    bloomRecency:  set when key evicted with s_recency > 7
//    bloomFrequency: set when key evicted with s_frequency > 7
//
//  Feedback loop (every feedbackWindow ops):
//    ratio = slowReversals / (fastReversals + slowReversals)
//    ratio > 0.6 → more frequency reversals → w_freq++ (protect frequency)
//    ratio < 0.4 → more recency reversals  → w_freq-- (favor recency)
//    w_freq bounded to [0.25, 4.0]
//
//  On admission: recency = MAX, frequency = 0 (new key has recency but no history)
//  On access: boost recency at α_R=2, boost frequency at α_F=1
//
//  Algebra: f_{S_τ} = REMARC applied to S_τ. The atoms, decay rates,
//  and weights are all chosen per problem. The algorithm f is constant.
// =====================================================================

class BareREMARC_MultiScale {
    struct PageData { std::vector<uint64_t> keys; std::vector<uint8_t> state; };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    BloomFilter bloomRecency_, bloomFreq_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;

    uint8_t fastNum_ = 7, fastDen_ = 8;
    uint8_t slowNum_ = 15, slowDen_ = 16;
    size_t slowSweepInterval_;

    float wFreq_ = 1.0f;
    static constexpr float wFreqMin_ = 0.25f, wFreqMax_ = 4.0f, wFreqStep_ = 0.1f;
    static constexpr uint8_t kGhostThresh = 7;
    static constexpr size_t kFeedbackWindow = 4096;
    size_t fastReversals_ = 0, slowReversals_ = 0;

    void fastDecay(PageData& pg) {
        for (auto& s : pg.state) {
            uint8_t r = UnpackSLocal(s);
            r = RemarcTimeDecay(r, fastNum_, fastDen_);
            s = PackTempCtrl(r, UnpackSRemote(s));
        }
    }
    void slowDecayAll() {
        for (auto& pg : pages_) {
            for (auto& s : pg.state) {
                uint8_t f = UnpackSRemote(s);
                f = RemarcTimeDecay(f, slowNum_, slowDen_);
                s = PackTempCtrl(UnpackSLocal(s), f);
            }
        }
    }
    void allocSlot() {
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
        evictColdest();
        for (size_t p = 0; p < maxPages_; p++) { if (pages_[p].keys.size() < kpp_) { next_ = p; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX; float bestScore = -1.0f;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t p = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[p]; if (pg.keys.empty()) continue;
            fastDecay(pg);
            float total = 0;
            for (auto s : pg.state) {
                total += (float)(REMARC_MAX - UnpackSLocal(s)) + wFreq_ * (float)(REMARC_MAX - UnpackSRemote(s));
            }
            float score = total / (float)pg.state.size();
            if (score > bestScore) { bestScore = score; bestPage = p; }
        }
        if (bestPage == SIZE_MAX) { for (size_t p = 0; p < maxPages_; p++) { if (!pages_[p].keys.empty()) { bestPage = p; break; } } if (bestPage == SIZE_MAX) return; }
        auto& victim = pages_[bestPage];
        for (size_t i = 0; i < victim.keys.size(); i++) {
            uint8_t r = UnpackSLocal(victim.state[i]);
            uint8_t f = UnpackSRemote(victim.state[i]);
            if (r >= kGhostThresh) bloomRecency_.set(victim.keys[i]);
            if (f >= kGhostThresh) bloomFreq_.set(victim.keys[i]);
            map_.erase(victim.keys[i]);
        }
        evictions_ += victim.keys.size();
        victim.keys.clear(); victim.state.clear();
    }
    void feedbackStep() {
        float total = (float)(fastReversals_ + slowReversals_);
        if (total > 0) {
            float ratio = (float)slowReversals_ / total;
            if (ratio > 0.6f) wFreq_ = std::min(wFreqMax_, wFreq_ + wFreqStep_);
            else if (ratio < 0.4f) wFreq_ = std::max(wFreqMin_, wFreq_ - wFreqStep_);
        }
        fastReversals_ = 0; slowReversals_ = 0;
    }
public:
    BareREMARC_MultiScale(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                           size_t slowSweepInterval = 64, size_t = 0)
        : bloomRecency_(1 << 17), bloomFreq_(1 << 17),
          kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg), slowSweepInterval_(slowSweepInterval) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        opCount_++;
        if (opCount_ % slowSweepInterval_ == 0) slowDecayAll();
        if (opCount_ % kFeedbackWindow == 0) {
            bloomRecency_.clear(); bloomFreq_.clear(); feedbackStep();
        }
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first]; size_t idx = it->second.second;
            uint8_t r = UnpackSLocal(pg.state[idx]);
            uint8_t f = UnpackSRemote(pg.state[idx]);
            r = RemarcBoost(r, cfg_.AlphaLocal);
            f = RemarcBoost(f, cfg_.AlphaRemote);
            pg.state[idx] = PackTempCtrl(r, f);
            hits_++; return;
        }
        misses_++;
        if (bloomRecency_.test(key)) fastReversals_++;
        if (bloomFreq_.test(key)) slowReversals_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.state.push_back(PackTempCtrl(REMARC_MAX, 0));
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_Step  (discrete projection P: S -> {0,1}, page-batch)
//
//  State S_τ = {0,...,15} × {0,...,15}:
//    low nibble  = recency
//    high nibble = frequency
//  Both atoms PROTECTIVE (high = hard to evict).
//
//  Discrete projection (step function):
//    E(key) = 1  if (recency + frequency > p)   [protected, T2]
//    E(key) = 0  otherwise                        [evictable, T1]
//
//  Page-level eviction with HARD boundary:
//    For each candidate page, count tier1 = |{k : E(k)=1}|
//    Score = tier1 * 1000000 + base_sum(recency+freq)
//    Evict page with LOWEST score → pure T1 pages first, then mixed
//
//  Per-atom decay (same as MultiScale):
//    Fast sweep (on page scan): recency decays at τ_R = 7/8
//    Slow sweep (global, every M ops): frequency decays at τ_F = 15/16
//
//  Directional ghost + feedback (same as MultiScale):
//    bloomCold: set when key evicted while E=0 (correct cold eviction)
//    bloomWarm: set when key evicted while E=1 (mistaken warm eviction)
//    On re-access: count cold/warm reversals
//    Feedback: warmRatio > 0.3 → raise p; < 0.1 → lower p
//
//  Key difference from TIER variant:
//    TIER used soft boundary: score = base * (1 + tierRatio * weight)
//    → mixed pages dilute signal
//    STEP uses hard boundary: tier1 * 1M separates pure-cold from mixed
//    → clean page-level segregation
//
//  Algebra: E = P_step(A_recency, A_frequency) where P_step is discrete
// =====================================================================

class BareREMARC_Step {
    struct PageData { std::vector<uint64_t> keys; std::vector<uint8_t> state; };
    std::unordered_map<uint64_t, std::pair<size_t, size_t>> map_;
    std::vector<PageData> pages_;
    BloomFilter bloomCold_, bloomWarm_;
    size_t kpp_, maxPages_, next_ = 0, scanCursor_ = 0;
    static constexpr size_t kScanBudget = 4;
    RemarcConfig cfg_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;

    uint8_t fastNum_ = 7, fastDen_ = 8;
    uint8_t slowNum_ = 15, slowDen_ = 16;
    size_t slowSweepInterval_;

    size_t p_;
    static constexpr size_t pMin_ = 4, pMax_ = 24;
    static constexpr size_t kFeedbackWindow = 4096;
    size_t coldReversals_ = 0, warmReversals_ = 0;

    void fastDecay(PageData& pg) {
        for (auto& s : pg.state) {
            uint8_t r = UnpackSLocal(s);
            r = RemarcTimeDecay(r, fastNum_, fastDen_);
            s = PackTempCtrl(r, UnpackSRemote(s));
        }
    }
    void slowDecayAll() {
        for (auto& pg : pages_) {
            for (auto& s : pg.state) {
                uint8_t f = UnpackSRemote(s);
                f = RemarcTimeDecay(f, slowNum_, slowDen_);
                s = PackTempCtrl(UnpackSLocal(s), f);
            }
        }
    }
    void allocSlot() {
        for (size_t pp = 0; pp < maxPages_; pp++) { if (pages_[pp].keys.size() < kpp_) { next_ = pp; return; } }
        evictColdest();
        for (size_t pp = 0; pp < maxPages_; pp++) { if (pages_[pp].keys.size() < kpp_) { next_ = pp; return; } }
    }
    void evictColdest() {
        if (pages_.empty()) return;
        size_t considered = 0, bestPage = SIZE_MAX;
        size_t bestTier1 = SIZE_MAX;
        size_t bestBase = SIZE_MAX;
        while (considered < std::min(kScanBudget, maxPages_)) {
            size_t pp = scanCursor_; scanCursor_ = (scanCursor_ + 1) % maxPages_; considered++; scans_++;
            auto& pg = pages_[pp]; if (pg.keys.empty()) continue;
            fastDecay(pg);
            size_t tier1 = 0, base = 0;
            for (auto s : pg.state) {
                uint8_t r = UnpackSLocal(s), f = UnpackSRemote(s);
                size_t sum = (size_t)r + (size_t)f;
                if (sum > p_) tier1++;
                base += sum;
            }
            if (tier1 < bestTier1 || (tier1 == bestTier1 && base < bestBase)) {
                bestTier1 = tier1; bestBase = base; bestPage = pp;
            }
        }
        if (bestPage == SIZE_MAX) { for (size_t pp = 0; pp < maxPages_; pp++) { if (!pages_[pp].keys.empty()) { bestPage = pp; break; } } if (bestPage == SIZE_MAX) return; }
        auto& victim = pages_[bestPage];
        for (size_t i = 0; i < victim.keys.size(); i++) {
            uint8_t r = UnpackSLocal(victim.state[i]), f = UnpackSRemote(victim.state[i]);
            size_t sum = (size_t)r + (size_t)f;
            if (sum > p_) bloomWarm_.set(victim.keys[i]);
            else bloomCold_.set(victim.keys[i]);
            map_.erase(victim.keys[i]);
        }
        evictions_ += victim.keys.size();
        victim.keys.clear(); victim.state.clear();
    }
    void feedbackStep() {
        size_t total = coldReversals_ + warmReversals_;
        if (total > 10) {
            float warmRatio = (float)warmReversals_ / (float)total;
            if (warmRatio > 0.3f) p_ = std::min(pMax_, p_ + 1);
            else if (warmRatio < 0.1f) p_ = (p_ > pMin_) ? p_ - 1 : p_;
        }
        coldReversals_ = 0; warmReversals_ = 0;
    }
public:
    BareREMARC_Step(size_t capacity, size_t keysPerPage, const RemarcConfig& cfg,
                     size_t slowSweepInterval = 64, size_t = 0)
        : bloomCold_(1 << 17), bloomWarm_(1 << 17),
          kpp_(keysPerPage), maxPages_((capacity + keysPerPage - 1) / keysPerPage),
          cfg_(cfg),
          slowSweepInterval_(slowSweepInterval), p_(10) { pages_.resize(maxPages_); }
    void access(uint64_t key) {
        opCount_++;
        if (opCount_ % slowSweepInterval_ == 0) slowDecayAll();
        if (opCount_ % kFeedbackWindow == 0) { bloomCold_.clear(); bloomWarm_.clear(); feedbackStep(); }
        auto it = map_.find(key);
        if (it != map_.end()) {
            auto& pg = pages_[it->second.first]; size_t idx = it->second.second;
            uint8_t r = UnpackSLocal(pg.state[idx]);
            uint8_t f = UnpackSRemote(pg.state[idx]);
            r = RemarcBoost(r, cfg_.AlphaLocal);
            f = RemarcBoost(f, cfg_.AlphaRemote);
            pg.state[idx] = PackTempCtrl(r, f);
            hits_++; return;
        }
        misses_++;
        if (bloomCold_.test(key)) coldReversals_++;
        if (bloomWarm_.test(key)) warmReversals_++;
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) allocSlot();
        if (next_ >= maxPages_ || pages_[next_].keys.size() >= kpp_) return;
        auto& pg = pages_[next_]; map_[key] = {next_, pg.keys.size()};
        pg.keys.push_back(key);
        pg.state.push_back(PackTempCtrl(REMARC_MAX, 0));
        if (pg.keys.size() >= kpp_) next_++;
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  BareREMARC_StepK  (discrete projection P: S -> {0,1}, per-key eviction)
//
//  Same state space, step function, decay, and feedback as Step variant.
//  BUT: evicts ONE key at a time (like ARC), not a full page.
//
//  This removes the page-batch delivery bottleneck to test:
//    "When delivery is equal, does the algebra match ARC?"
//
//  Implementation: keys stored in a flat map (key → state), no pages.
//  Eviction: scan budget keys via cursor, find worst T1 key.
//    Prefers E=0 (evictable) over E=1 (protected).
//    Among E=0 keys, pick lowest (recency+frequency) sum.
//
//  Decay mode controlled by constructor:
//    slowNum_/slowDen_ = 0/0 → no frequency decay (recency only)
//    slowNum_/slowDen_ = N/D → frequency decays at N/D rate
// =====================================================================

class BareREMARC_StepK {
    struct Entry { uint8_t state; };
    std::unordered_map<uint64_t, Entry> store_;
    std::vector<uint64_t> keyOrder_;
    BloomFilter bloomCold_, bloomWarm_;
    size_t capacity_;
    size_t scanCursor_ = 0;
    static constexpr size_t kScanBudget = 32;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0, scans_ = 0;
    size_t opCount_ = 0;

    uint8_t fastNum_ = 7, fastDen_ = 8;
    uint8_t slowNum_, slowDen_;
    size_t slowSweepInterval_;
    bool hasSlowDecay_;

    size_t p_;
    static constexpr size_t pMin_ = 4, pMax_ = 24;
    static constexpr size_t kFeedbackWindow = 4096;
    size_t coldReversals_ = 0, warmReversals_ = 0;

    void slowDecayAll() {
        if (!hasSlowDecay_) return;
        for (auto& [k, e] : store_) {
            uint8_t f = UnpackSRemote(e.state);
            f = RemarcTimeDecay(f, slowNum_, slowDen_);
            e.state = PackTempCtrl(UnpackSLocal(e.state), f);
        }
    }
    void evictOne() {
        if (store_.empty()) return;
        size_t considered = 0;
        uint64_t worstKey = 0;
        bool found = false;
        size_t worstTier1 = SIZE_MAX;
        size_t worstBase = SIZE_MAX;

        while (considered < std::min(kScanBudget, store_.size())) {
            size_t idx = scanCursor_ % keyOrder_.size();
            scanCursor_++;
            considered++; scans_++;
            uint64_t k = keyOrder_[idx];
            auto it = store_.find(k);
            if (it == store_.end()) continue;
            fastDecayOne(it->second);
            uint8_t r = UnpackSLocal(it->second.state), f = UnpackSRemote(it->second.state);
            size_t sum = (size_t)r + (size_t)f;
            size_t tier1 = (sum > p_) ? 1 : 0;
            if (!found || tier1 < worstTier1 || (tier1 == worstTier1 && sum < worstBase)) {
                worstTier1 = tier1; worstBase = sum; worstKey = k; found = true;
            }
        }
        if (!found) return;

        auto it = store_.find(worstKey);
        if (it == store_.end()) return;
        uint8_t r = UnpackSLocal(it->second.state), f = UnpackSRemote(it->second.state);
        size_t sum = (size_t)r + (size_t)f;
        if (sum > p_) bloomWarm_.set(worstKey);
        else bloomCold_.set(worstKey);
        store_.erase(it);
        for (size_t i = 0; i < keyOrder_.size(); i++) {
            if (keyOrder_[i] == worstKey) {
                keyOrder_[i] = keyOrder_.back();
                keyOrder_.pop_back();
                break;
            }
        }
        evictions_++;
    }
    inline void fastDecayOne(Entry& e) {
        uint8_t r = UnpackSLocal(e.state);
        r = RemarcTimeDecay(r, fastNum_, fastDen_);
        e.state = PackTempCtrl(r, UnpackSRemote(e.state));
    }
    void feedbackStep() {
        size_t total = coldReversals_ + warmReversals_;
        if (total > 10) {
            float warmRatio = (float)warmReversals_ / (float)total;
            if (warmRatio > 0.3f) p_ = std::min(pMax_, p_ + 1);
            else if (warmRatio < 0.1f) p_ = (p_ > pMin_) ? p_ - 1 : p_;
        }
        coldReversals_ = 0; warmReversals_ = 0;
    }
public:
    BareREMARC_StepK(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                      size_t slowSweepInterval = 64, uint8_t slowNum = 15, uint8_t slowDen = 16)
        : bloomCold_(1 << 17), bloomWarm_(1 << 17),
          capacity_(capacity),
          slowNum_(slowNum), slowDen_(slowDen),
          slowSweepInterval_(slowSweepInterval),
          hasSlowDecay_(slowNum > 0 && slowDen > 0),
          p_(10) {
        (void)keysPerPage;
    }
    void access(uint64_t key) {
        opCount_++;
        if (opCount_ % slowSweepInterval_ == 0) slowDecayAll();
        if (opCount_ % kFeedbackWindow == 0) { bloomCold_.clear(); bloomWarm_.clear(); feedbackStep(); }
        auto it = store_.find(key);
        if (it != store_.end()) {
            uint8_t r = UnpackSLocal(it->second.state);
            uint8_t f = UnpackSRemote(it->second.state);
            r = RemarcBoost(r, 2);
            f = RemarcBoost(f, 1);
            it->second.state = PackTempCtrl(r, f);
            hits_++; return;
        }
        misses_++;
        if (bloomCold_.test(key)) coldReversals_++;
        if (bloomWarm_.test(key)) warmReversals_++;
        if (store_.size() >= capacity_) evictOne();
        if (store_.size() >= capacity_) return;
        store_[key] = {PackTempCtrl(REMARC_MAX, 0)};
        keyOrder_.push_back(key);
    }
    size_t hits() const { return hits_; } size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; } size_t scans() const { return scans_; }
};

// =====================================================================
//  AUG family: REMARC atoms + ARC quota structure
//
//  All variants share:
//    - ARC-identical ghost lists (b1_/b2_) for p-adaptation
//    - REMARC state S = (recency, frequency) per key
//    - Evidence-based demotion: T2 hit with low state → T1
//    - Unconditional promotion: T1 hit → T2 (like ARC)
//
//  Decay strategies differ:
//    Aug-TS: Timestamp-based. Store last-access epoch, decay
//            recency on re-access proportional to idle epochs.
//            O(1) per access. Demotion fires on re-access only.
//    Aug-GS: Global sweep. Every N ops, decay ALL cached keys.
//            O(capacity) per sweep, O(capacity/N) amortized.
//            Demotion fires for idle keys during sweep.
//    Aug-CB: Combined. Periodic sweep decays idle keys.
//            Timestamp corrects for partial epochs on re-access.
// =====================================================================

struct AugEntry {
    uint8_t state = 0;
    uint32_t lastEpoch = 0;
    uint32_t avgGap = 0;
};

static constexpr uint8_t AUG_REMARC_MAX = 15;
static constexpr uint8_t AUG_DECAY_NUM = 7;
static constexpr uint8_t AUG_DECAY_DEN = 8;

static inline uint8_t augDecayR(uint8_t r) {
    return (uint8_t)((uint16_t)r * AUG_DECAY_NUM / AUG_DECAY_DEN);
}

static inline uint8_t augBoostR(uint8_t r) {
    uint16_t boosted = (uint16_t)r + (uint16_t)(AUG_REMARC_MAX - r) * 2 / 15;
    return boosted < AUG_REMARC_MAX ? (uint8_t)boosted : AUG_REMARC_MAX;
}

static inline uint8_t augBoostF(uint8_t f) {
    uint16_t boosted = (uint16_t)f + (uint16_t)(AUG_REMARC_MAX - f) * 1 / 15;
    return boosted < AUG_REMARC_MAX ? (uint8_t)boosted : AUG_REMARC_MAX;
}

static inline uint8_t augGetR(uint8_t s) { return s & 0xF; }
static inline uint8_t augGetF(uint8_t s) { return (s >> 4) & 0xF; }
static inline uint8_t augPack(uint8_t r, uint8_t f) { return (f << 4) | r; }

// --- Aug-TS: Timestamp-based decay ---

class BareREMARC_Aug_TS {
    ArcList t1_, t2_, b1_, b2_;
    std::unordered_set<uint64_t> cached_;
    std::unordered_map<uint64_t, AugEntry> stateMap_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;
    uint8_t demoteThresh_;

    void replace(uint64_t key) {
        if (!t1_.empty() && (t1_.size() > p_ ||
            (b2_.contains(key) && t1_.size() == p_))) {
            uint64_t old = t1_.back();
            cached_.erase(old); stateMap_.erase(old);
            t1_.pop_back(); b1_.push_front(old);
        } else if (!t2_.empty()) {
            uint64_t old = t2_.back();
            cached_.erase(old); stateMap_.erase(old);
            t2_.pop_back(); b2_.push_front(old);
        }
        evictions_++;
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) b1_.pop_back();
            else if (!t1_.empty()) {
                uint64_t old = t1_.back();
                cached_.erase(old); stateMap_.erase(old);
                t1_.pop_back(); evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) b2_.pop_back();
            else if (!t2_.empty()) {
                uint64_t old = t2_.back();
                cached_.erase(old); stateMap_.erase(old);
                t2_.pop_back(); evictions_++;
            }
        }
    }

    void touchState(uint64_t key) {
        auto it = stateMap_.find(key);
        if (it == stateMap_.end()) {
            stateMap_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_};
            return;
        }
        uint32_t idle = epoch_ - it->second.lastEpoch;
        it->second.lastEpoch = epoch_;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < idle && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        it->second.state = augPack(r, f);
    }

    void demoteCheck(uint64_t key) {
        if (demoteThresh_ == 0) { t2_.splice_front(key); return; }
        auto it = stateMap_.find(key);
        if (it == stateMap_.end()) { t2_.splice_front(key); return; }
        uint32_t idle = epoch_ - it->second.lastEpoch;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < idle && r > 0; i++) r = augDecayR(r);
        if ((uint16_t)r + f <= demoteThresh_) {
            it->second.lastEpoch = epoch_;
            it->second.state = augPack(augDecayR(augGetR(s)), f);
            t2_.erase(key); t1_.push_front(key);
        } else {
            it->second.lastEpoch = epoch_;
            t2_.splice_front(key);
        }
    }

public:
    BareREMARC_Aug_TS(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                       size_t = 64, uint8_t demoteThresh = 4)
        : cap_(capacity), p_(0), demoteThresh_(demoteThresh) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        epoch_++;
        if (cached_.count(key)) {
            touchState(key);
            if (t1_.contains(key)) { t1_.erase(key); t2_.push_front(key); }
            else demoteCheck(key);
            hits_++; return;
        }
        misses_++;
        if (b1_.contains(key)) {
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            doEvict(); replace(key); b1_.erase(key);
            cached_.insert(key); touchState(key); t2_.push_front(key);
            return;
        }
        if (b2_.contains(key)) {
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            doEvict(); replace(key); b2_.erase(key);
            cached_.insert(key); touchState(key); t2_.push_front(key);
            return;
        }
        doEvict();
        cached_.insert(key);
        stateMap_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_};
        t1_.push_front(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return 0; }
};

// --- Aug-PRED: TS + inter-arrival prediction, merged hash + variance atom ---
//
//  Atoms: A_recency (decay+boost), A_frequency (boost), A_period (EMA of gaps),
//         A_confidence (MAD of gaps — NEW)
//
//  Optimization: single idx_ map replaces cached_ + ArcList::idx_ + stateMap_.
//  One hash lookup per access instead of three.
//
//  Eviction scoring:
//    Confident keys (MAD ≤ μ/2): evict by predicted next access
//    Uncertain keys (MAD > μ/2): evict by time since last access (LRU fallback)

class BareREMARC_Aug_PRED {
    enum Tier : uint8_t { NONE = 0, T1 = 1, T2 = 2, B1 = 3, B2 = 4 };

    struct PredEntry {
        uint8_t state = 0;
        uint32_t lastEpoch = 0;
        uint32_t avgGap = 0;
        uint32_t madGap = 0;
        Tier tier = NONE;
        std::list<uint64_t>::iterator pos;
    };

    std::list<uint64_t> t1_, t2_, b1_, b2_;
    std::unordered_map<uint64_t, PredEntry> idx_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;
    uint8_t demoteThresh_;
    static constexpr size_t EVICT_SCAN_ = 8;

    std::list<uint64_t>& listFor(Tier t) {
        static std::list<uint64_t> dummy;
        switch (t) {
            case T1: return t1_;
            case T2: return t2_;
            case B1: return b1_;
            case B2: return b2_;
            default: return dummy;
        }
    }

    void pushFront(uint64_t key, Tier tier) {
        auto& l = listFor(tier);
        l.push_front(key);
        auto& e = idx_[key];
        e.tier = tier;
        e.pos = l.begin();
    }

    void eraseFromList(uint64_t key) {
        auto it = idx_.find(key);
        if (it != idx_.end() && it->second.tier != NONE) {
            listFor(it->second.tier).erase(it->second.pos);
            it->second.tier = NONE;
        }
    }

    void spliceToFront(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        auto& l = listFor(it->second.tier);
        l.splice(l.begin(), l, it->second.pos);
        it->second.pos = l.begin();
    }

    void moveTo(uint64_t key, Tier newTier) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        if (it->second.tier != NONE)
            listFor(it->second.tier).erase(it->second.pos);
        auto& l = listFor(newTier);
        l.push_front(key);
        it->second.tier = newTier;
        it->second.pos = l.begin();
    }

    uint64_t evictionScore(const PredEntry& e) const {
        if (e.avgGap > 0) {
            return (uint64_t)e.lastEpoch + e.avgGap;
        }
        return (uint64_t)(epoch_ - e.lastEpoch);
    }

    uint64_t findWorstInList(std::list<uint64_t>& l, size_t scanN) {
        auto it = l.rbegin();
        uint64_t worstKey = *it;
        uint64_t worstScore = evictionScore(idx_[worstKey]);
        size_t n = std::min((size_t)l.size(), scanN);
        for (size_t i = 1; i < n; i++) {
            ++it;
            uint64_t s = evictionScore(idx_[*it]);
            if (s > worstScore) { worstScore = s; worstKey = *it; }
        }
        return worstKey;
    }

    void evictFrom(std::list<uint64_t>& l, bool toGhost, Tier ghostTier) {
        if (l.empty()) return;
        uint64_t victim = findWorstInList(l, EVICT_SCAN_);
        eraseFromList(victim);
        idx_.erase(victim);
        if (toGhost) pushFront(victim, ghostTier);
        evictions_++;
    }

    void replace(uint64_t key) {
        bool evictT1 = !t1_.empty() && (t1_.size() > p_ ||
            (idx_.count(key) && idx_[key].tier == B2 && t1_.size() == p_));

        if (evictT1) {
            uint64_t victim = findWorstInList(t1_, EVICT_SCAN_);
            eraseFromList(victim);
            pushFront(victim, B1);
        } else if (!t2_.empty()) {
            uint64_t victim = findWorstInList(t2_, EVICT_SCAN_);
            eraseFromList(victim);
            pushFront(victim, B2);
        }
        evictions_++;
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) {
                uint64_t old = b1_.back();
                b1_.pop_back();
                idx_.erase(old);
            } else if (!t1_.empty()) {
                uint64_t victim = findWorstInList(t1_, EVICT_SCAN_);
                eraseFromList(victim);
                idx_.erase(victim);
                evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) {
                uint64_t old = b2_.back();
                b2_.pop_back();
                idx_.erase(old);
            } else if (!t2_.empty()) {
                uint64_t victim = findWorstInList(t2_, EVICT_SCAN_);
                eraseFromList(victim);
                idx_.erase(victim);
                evictions_++;
            }
        }
    }

    void touchState(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) {
            idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, 0, NONE, {}};
            return;
        }
        PredEntry& e = it->second;
        uint32_t gap = epoch_ - e.lastEpoch;
        e.lastEpoch = epoch_;
        if (gap > 0) {
            uint32_t absDiff = gap > e.avgGap ? gap - e.avgGap : e.avgGap - gap;
            e.madGap = e.madGap * 3 / 4 + absDiff / 4;
            e.avgGap = e.avgGap * 3 / 4 + gap / 4;
        }
        uint8_t s = e.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        e.state = augPack(r, f);
    }

    void demoteCheck(uint64_t key) {
        if (demoteThresh_ == 0) { spliceToFront(key); return; }
        auto it = idx_.find(key);
        if (it == idx_.end()) { spliceToFront(key); return; }
        uint32_t gap = epoch_ - it->second.lastEpoch;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        if ((uint16_t)r + f <= demoteThresh_) {
            it->second.lastEpoch = epoch_;
            it->second.state = augPack(augDecayR(augGetR(s)), f);
            moveTo(key, T1);
        } else {
            it->second.lastEpoch = epoch_;
            spliceToFront(key);
        }
    }

public:
    BareREMARC_Aug_PRED(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                         size_t = 64, uint8_t demoteThresh = 4)
        : cap_(capacity), p_(0), demoteThresh_(demoteThresh) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        epoch_++;
        auto it = idx_.find(key);
        bool cached = it != idx_.end() && (it->second.tier == T1 || it->second.tier == T2);

        if (cached) {
            touchState(key);
            if (it->second.tier == T1) {
                moveTo(key, T2);
            } else {
                demoteCheck(key);
            }
            hits_++; return;
        }
        misses_++;

        if (it != idx_.end() && it->second.tier == B1) {
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            return;
        }
        if (it != idx_.end() && it->second.tier == B2) {
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            return;
        }

        doEvict();
        idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, 0, NONE, {}};
        pushFront(key, T1);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return 0; }
};

// --- Aug-HEAP: ARC quota + heap-based eviction (predict furthest-in-future) ---
//
//  Same ARC structure (T1/T2, p, b1/b2) + REMARC state, but eviction selects
//  from a max-heap keyed by predicted_next_access = lastEpoch + avgGap.
//  Lazy deletion via version stamps: each (push) increments the key's version;
//  stale heap entries (wrong version) are skipped on pop.
//
//  This breaks the structural barrier for looping: the MRU key (front of list)
//  has predicted_next = epoch + cycle_length, which is the highest, so it gets
//  evicted first — exactly what Belady's algorithm would do.

class BareREMARC_Aug_HEAP {
    enum Tier : uint8_t { NONE = 0, T1 = 1, T2 = 2, B1 = 3, B2 = 4 };

    struct HeapEntry {
        uint64_t predictedNext;
        uint64_t key;
        uint64_t version;
        bool operator<(const HeapEntry& o) const {
            if (predictedNext != o.predictedNext) return predictedNext < o.predictedNext;
            return version < o.version;
        }
    };

    struct PredEntry {
        uint8_t state = 0;
        uint32_t lastEpoch = 0;
        uint32_t avgGap = 0;
        Tier tier = NONE;
        std::list<uint64_t>::iterator pos;
        uint64_t version = 0;
    };

    std::list<uint64_t> t1_, t2_, b1_, b2_;
    std::unordered_map<uint64_t, PredEntry> idx_;
    std::priority_queue<HeapEntry> heap_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;
    uint8_t demoteThresh_;

    std::list<uint64_t>& listFor(Tier t) {
        static std::list<uint64_t> dummy;
        switch (t) {
            case T1: return t1_; case T2: return t2_;
            case B1: return b1_; case B2: return b2_;
            default: return dummy;
        }
    }

    void pushFront(uint64_t key, Tier tier) {
        auto& l = listFor(tier);
        l.push_front(key);
        auto& e = idx_[key];
        e.tier = tier;
        e.pos = l.begin();
    }

    void eraseFromList(uint64_t key) {
        auto it = idx_.find(key);
        if (it != idx_.end() && it->second.tier != NONE) {
            listFor(it->second.tier).erase(it->second.pos);
            it->second.tier = NONE;
        }
    }

    void spliceToFront(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        auto& l = listFor(it->second.tier);
        l.splice(l.begin(), l, it->second.pos);
        it->second.pos = l.begin();
    }

    void moveTo(uint64_t key, Tier newTier) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        if (it->second.tier != NONE)
            listFor(it->second.tier).erase(it->second.pos);
        auto& l = listFor(newTier);
        l.push_front(key);
        it->second.tier = newTier;
        it->second.pos = l.begin();
    }

    void pushToHeap(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        PredEntry& e = it->second;
        e.version++;
        uint64_t pred = (e.avgGap > 0) ? (uint64_t)e.lastEpoch + e.avgGap
                                       : (uint64_t)epoch_;
        heap_.push({pred, key, e.version});
    }

    uint64_t popBestVictim(Tier targetTier) {
        uint64_t bestKey = 0;
        uint64_t bestPred = 0;
        size_t attempts = 0;
        while (!heap_.empty() && attempts < 64) {
            attempts++;
            HeapEntry top = heap_.top();
            auto it = idx_.find(top.key);
            if (it == idx_.end()) { heap_.pop(); continue; }
            PredEntry& e = it->second;
            if (e.version != top.version) { heap_.pop(); continue; }
            if (e.tier != targetTier) { heap_.pop(); continue; }
            if (top.predictedNext > bestPred) {
                bestPred = top.predictedNext;
                bestKey = top.key;
            }
            heap_.pop();
        }
        if (bestKey != 0) return bestKey;
        if (targetTier == T1 && !t1_.empty()) return t1_.back();
        if (targetTier == T2 && !t2_.empty()) return t2_.back();
        return 0;
    }

    void evictKey(uint64_t victim, Tier ghostTier) {
        eraseFromList(victim);
        pushFront(victim, ghostTier);
        evictions_++;
    }

    void replace(uint64_t key) {
        bool evictT1 = !t1_.empty() && (t1_.size() > p_ ||
            (idx_.count(key) && idx_[key].tier == B2 && t1_.size() == p_));

        if (evictT1) {
            uint64_t victim = popBestVictim(T1);
            evictKey(victim, B1);
        } else if (!t2_.empty()) {
            uint64_t victim = popBestVictim(T2);
            evictKey(victim, B2);
        }
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) {
                uint64_t old = b1_.back(); b1_.pop_back(); idx_.erase(old);
            } else if (!t1_.empty()) {
                uint64_t victim = popBestVictim(T1);
                eraseFromList(victim); idx_.erase(victim); evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) {
                uint64_t old = b2_.back(); b2_.pop_back(); idx_.erase(old);
            } else if (!t2_.empty()) {
                uint64_t victim = popBestVictim(T2);
                eraseFromList(victim); idx_.erase(victim); evictions_++;
            }
        }
    }

    void touchState(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) {
            idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, NONE, {}, 0};
            return;
        }
        PredEntry& e = it->second;
        uint32_t gap = epoch_ - e.lastEpoch;
        e.lastEpoch = epoch_;
        if (gap > 0) {
            e.avgGap = e.avgGap * 3 / 4 + gap / 4;
        }
        uint8_t s = e.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        e.state = augPack(r, f);
        pushToHeap(key);
    }

    void demoteCheck(uint64_t key) {
        if (demoteThresh_ == 0) { spliceToFront(key); pushToHeap(key); return; }
        auto it = idx_.find(key);
        if (it == idx_.end()) { spliceToFront(key); pushToHeap(key); return; }
        uint32_t gap = epoch_ - it->second.lastEpoch;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        if ((uint16_t)r + f <= demoteThresh_) {
            it->second.lastEpoch = epoch_;
            it->second.state = augPack(augDecayR(augGetR(s)), f);
            moveTo(key, T1);
        } else {
            it->second.lastEpoch = epoch_;
            spliceToFront(key);
        }
        pushToHeap(key);
    }

public:
    BareREMARC_Aug_HEAP(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                         size_t = 64, uint8_t demoteThresh = 4)
        : cap_(capacity), p_(0), demoteThresh_(demoteThresh) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        epoch_++;
        auto it = idx_.find(key);
        bool cached = it != idx_.end() && (it->second.tier == T1 || it->second.tier == T2);
        if (cached) {
            touchState(key);
            if (it->second.tier == T1) {
                moveTo(key, T2);
            } else {
                demoteCheck(key);
            }
            hits_++; return;
        }
        misses_++;
        if (it != idx_.end() && it->second.tier == B1) {
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            return;
        }
        if (it != idx_.end() && it->second.tier == B2) {
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            return;
        }
        doEvict();
        idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, NONE, {}, 0};
        pushFront(key, T1);
        pushToHeap(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return 0; }
};

// --- Aug-ADAPT: Self-tuning ARC with feedback-driven delivery selection ---
//
//  Combines two signals to decide between heap-based and LRU-based eviction:
//
//  1. Population CV (feedforward): EMA of avgGap mean and variance across
//     all cached keys. Low CV = homogeneous pattern (looping) → trust heap.
//     High CV = heterogeneous pattern (Zipfian) → fall back to LRU.
//
//  2. Prediction regret (feedback): When a ghost-hit key is re-accessed,
//     compare actual gap since eviction to predicted gap (avgGap). High
//     regret = predictions are wrong → fall back to LRU.
//
//  The two signals are combined: confidence = (1 - CV) * (1 - regret).
//  When confidence > 0.5: use heap eviction.
//  When confidence <= 0.5: use LRU eviction.
//
//  This is a proper control loop: the plant is the cache workload, the
//  controller is the delivery mechanism, the setpoint is "high hit rate,"
//  and the feedback signal is prediction regret.

class BareREMARC_Aug_ADAPT {
    enum Tier : uint8_t { NONE = 0, T1 = 1, T2 = 2, B1 = 3, B2 = 4 };

    struct HeapEntry {
        uint64_t predictedNext;
        uint64_t key;
        uint64_t version;
        bool operator<(const HeapEntry& o) const {
            if (predictedNext != o.predictedNext) return predictedNext < o.predictedNext;
            return version < o.version;
        }
    };

    struct PredEntry {
        uint8_t state = 0;
        uint32_t lastEpoch = 0;
        uint32_t avgGap = 0;
        Tier tier = NONE;
        std::list<uint64_t>::iterator pos;
        uint64_t version = 0;
    };

    std::list<uint64_t> t1_, t2_, b1_, b2_;
    std::unordered_map<uint64_t, PredEntry> idx_;
    std::priority_queue<HeapEntry> heap_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;
    uint8_t demoteThresh_;

    double popMeanGap_ = 0.0;
    double popM2Gap_ = 0.0;
    double regretEMA_ = 0.5;
    static constexpr double STATS_ALPHA = 0.002;
    static constexpr double REGRET_ALPHA = 0.001;

    std::list<uint64_t>& listFor(Tier t) {
        static std::list<uint64_t> dummy;
        switch (t) {
            case T1: return t1_; case T2: return t2_;
            case B1: return b1_; case B2: return b2_;
            default: return dummy;
        }
    }

    void pushFront(uint64_t key, Tier tier) {
        auto& l = listFor(tier);
        l.push_front(key);
        auto& e = idx_[key];
        e.tier = tier;
        e.pos = l.begin();
    }

    void eraseFromList(uint64_t key) {
        auto it = idx_.find(key);
        if (it != idx_.end() && it->second.tier != NONE) {
            listFor(it->second.tier).erase(it->second.pos);
            it->second.tier = NONE;
        }
    }

    void spliceToFront(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        auto& l = listFor(it->second.tier);
        l.splice(l.begin(), l, it->second.pos);
        it->second.pos = l.begin();
    }

    void moveTo(uint64_t key, Tier newTier) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        if (it->second.tier != NONE)
            listFor(it->second.tier).erase(it->second.pos);
        auto& l = listFor(newTier);
        l.push_front(key);
        it->second.tier = newTier;
        it->second.pos = l.begin();
    }

    void pushToHeap(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        PredEntry& e = it->second;
        e.version++;
        uint64_t pred = (e.avgGap > 0) ? (uint64_t)e.lastEpoch + e.avgGap
                                       : (uint64_t)epoch_;
        heap_.push({pred, key, e.version});
    }

    void updatePopStats(const PredEntry& e) {
        if (e.avgGap == 0) return;
        double g = (double)e.avgGap;
        double delta = g - popMeanGap_;
        popMeanGap_ += STATS_ALPHA * delta;
        popM2Gap_ += STATS_ALPHA * delta * (g - popMeanGap_);
    }

    void updateRegret(Tier oldTier) {
        auto it = idx_.end();
        for (auto* list : {&b1_, &b2_}) {
            uint64_t back = list->back();
            auto found = idx_.find(back);
            if (found != idx_.end()) { it = found; break; }
        }
        (void)oldTier;
    }

    void updateRegretOnGhostHit(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        PredEntry& e = it->second;
        if (e.avgGap == 0 || e.lastEpoch == 0) return;
        uint32_t actualGap = epoch_ - e.lastEpoch;
        double regret = (double)((actualGap > e.avgGap) ? (actualGap - e.avgGap)
                                                         : (e.avgGap - actualGap))
                        / (double)e.avgGap;
        regret = std::min(regret, 5.0);
        regretEMA_ = (1.0 - REGRET_ALPHA) * regretEMA_ + REGRET_ALPHA * regret;
    }

    bool shouldUseHeap() const {
        double cv = (popMeanGap_ > 1.0) ? std::sqrt(std::max(0.0, popM2Gap_)) / popMeanGap_
                                        : 1.0;
        double cvFactor = std::max(0.0, 1.0 - cv * 3.0);
        double regretFactor = std::max(0.0, 1.0 - regretEMA_);
        double confidence = cvFactor * regretFactor;
        return confidence > 0.5;
    }

    uint64_t popBestVictim(Tier targetTier) {
        uint64_t bestKey = 0;
        uint64_t bestPred = 0;
        size_t attempts = 0;
        while (!heap_.empty() && attempts < 64) {
            attempts++;
            HeapEntry top = heap_.top();
            auto it = idx_.find(top.key);
            if (it == idx_.end()) { heap_.pop(); continue; }
            PredEntry& e = it->second;
            if (e.version != top.version) { heap_.pop(); continue; }
            if (e.tier != targetTier) { heap_.pop(); continue; }
            if (top.predictedNext > bestPred) {
                bestPred = top.predictedNext;
                bestKey = top.key;
            }
            heap_.pop();
        }
        if (bestKey != 0) return bestKey;
        if (targetTier == T1 && !t1_.empty()) return t1_.back();
        if (targetTier == T2 && !t2_.empty()) return t2_.back();
        return 0;
    }

    void evictVictim(Tier targetTier, Tier ghostTier) {
        uint64_t victim;
        if (shouldUseHeap()) {
            victim = popBestVictim(targetTier);
        } else {
            if (targetTier == T1 && !t1_.empty()) victim = t1_.back();
            else if (targetTier == T2 && !t2_.empty()) victim = t2_.back();
            else return;
        }
        if (victim == 0) return;
        eraseFromList(victim);
        pushFront(victim, ghostTier);
        evictions_++;
    }

    void replace(uint64_t key) {
        bool evictT1 = !t1_.empty() && (t1_.size() > p_ ||
            (idx_.count(key) && idx_[key].tier == B2 && t1_.size() == p_));
        if (evictT1 && !t1_.empty()) {
            evictVictim(T1, B1);
        } else if (!t2_.empty()) {
            evictVictim(T2, B2);
        }
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) {
                uint64_t old = b1_.back(); b1_.pop_back(); idx_.erase(old);
            } else if (!t1_.empty()) {
                evictVictim(T1, NONE);
                if (evictions_ == 0) return;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) {
                uint64_t old = b2_.back(); b2_.pop_back(); idx_.erase(old);
            } else if (!t2_.empty()) {
                evictVictim(T2, NONE);
            }
        }
    }

    void touchState(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) {
            idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, NONE, {}, 0};
            return;
        }
        PredEntry& e = it->second;
        uint32_t gap = epoch_ - e.lastEpoch;
        e.lastEpoch = epoch_;
        if (gap > 0) {
            e.avgGap = e.avgGap * 3 / 4 + gap / 4;
        }
        uint8_t s = e.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        e.state = augPack(r, f);
        if (e.tier == T1 || e.tier == T2) updatePopStats(e);
        pushToHeap(key);
    }

    void demoteCheck(uint64_t key) {
        if (demoteThresh_ == 0) { spliceToFront(key); pushToHeap(key); return; }
        auto it = idx_.find(key);
        if (it == idx_.end()) { spliceToFront(key); pushToHeap(key); return; }
        uint32_t gap = epoch_ - it->second.lastEpoch;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        if ((uint16_t)r + f <= demoteThresh_) {
            it->second.lastEpoch = epoch_;
            it->second.state = augPack(augDecayR(augGetR(s)), f);
            moveTo(key, T1);
        } else {
            it->second.lastEpoch = epoch_;
            spliceToFront(key);
        }
        pushToHeap(key);
    }

public:
    BareREMARC_Aug_ADAPT(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                          size_t = 64, uint8_t demoteThresh = 4)
        : cap_(capacity), p_(0), demoteThresh_(demoteThresh) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        epoch_++;
        auto it = idx_.find(key);
        bool cached = it != idx_.end() && (it->second.tier == T1 || it->second.tier == T2);
        if (cached) {
            touchState(key);
            if (it->second.tier == T1) {
                moveTo(key, T2);
            } else {
                demoteCheck(key);
            }
            hits_++; return;
        }
        misses_++;
        if (it != idx_.end() && it->second.tier == B1) {
            updateRegretOnGhostHit(key);
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            pushToHeap(key);
            return;
        }
        if (it != idx_.end() && it->second.tier == B2) {
            updateRegretOnGhostHit(key);
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            pushToHeap(key);
            return;
        }
        doEvict();
        idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, NONE, {}, 0};
        pushFront(key, T1);
        pushToHeap(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return 0; }
    double confidence() const {
        double cv = (popMeanGap_ > 1.0) ? std::sqrt(std::max(0.0, popM2Gap_)) / popMeanGap_
                                        : 1.0;
        double cvFactor = std::max(0.0, 1.0 - cv * 3.0);
        double regretFactor = std::max(0.0, 1.0 - regretEMA_);
        return cvFactor * regretFactor;
    }
};

// --- Quota-Free: Pure predictive eviction, no ARC structure ---
//
//  Research direction 1 from the delivery-quota tension: eliminate all ARC
//  machinery (T1/T2/B1/B2, p-adaptation, ghost lists) and rely entirely
//  on prediction (predictedNext = lastEpoch + avgGap) for eviction.
//
//  Design:
//  - Single unordered_map<uint64_t, Entry> (no lists, no tiers)
//  - Max-heap keyed by predictedNext with lazy deletion via version stamps
//  - On miss + cache full: pop heap for highest predictedNext, evict it
//  - On hit: update REMARC state, push new heap entry
//  - No ghost lists, no p-adaptation, no quota mechanism
//
//  This tests whether prediction alone suffices without ARC's structural
//  information (scan resistance from ghost lists, p-adaptation from B1/B2).

class BareREMARC_QuotaFree {

    struct HeapEntry {
        uint64_t predictedNext;
        uint64_t key;
        uint64_t version;
        bool operator<(const HeapEntry& o) const {
            if (predictedNext != o.predictedNext) return predictedNext < o.predictedNext;
            return version < o.version;
        }
    };

    struct Entry {
        uint8_t state = 0;
        uint32_t lastEpoch = 0;
        uint32_t avgGap = 0;
        uint64_t version = 0;
    };

    std::unordered_map<uint64_t, Entry> idx_;
    std::priority_queue<HeapEntry> heap_;
    size_t cap_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;

    void pushToHeap(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        Entry& e = it->second;
        e.version++;
        uint64_t pred = (e.avgGap > 0) ? (uint64_t)e.lastEpoch + e.avgGap
                                       : (uint64_t)epoch_;
        heap_.push({pred, key, e.version});
    }

    void evictOne() {
        size_t attempts = 0;
        while (!heap_.empty() && attempts < 64) {
            attempts++;
            HeapEntry top = heap_.top();
            auto it = idx_.find(top.key);
            if (it == idx_.end()) { heap_.pop(); continue; }
            Entry& e = it->second;
            if (e.version != top.version) { heap_.pop(); continue; }
            idx_.erase(top.key);
            heap_.pop();
            evictions_++;
            return;
        }
        if (!idx_.empty()) {
            auto it = idx_.begin();
            idx_.erase(it);
            evictions_++;
        }
    }

    void touchState(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        Entry& e = it->second;
        uint32_t gap = epoch_ - e.lastEpoch;
        e.lastEpoch = epoch_;
        if (gap > 0) {
            e.avgGap = e.avgGap * 3 / 4 + gap / 4;
        }
        uint8_t s = e.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        e.state = augPack(r, f);
        pushToHeap(key);
    }

public:
    BareREMARC_QuotaFree(size_t capacity, size_t, const RemarcConfig&, size_t = 64, uint8_t = 4)
        : cap_(capacity) {}

    void access(uint64_t key) {
        epoch_++;
        auto it = idx_.find(key);
        if (it != idx_.end()) {
            touchState(key);
            hits_++;
            return;
        }
        misses_++;
        if (idx_.size() >= cap_) {
            evictOne();
        }
        idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, 0};
        pushToHeap(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return 0; }
};

// --- Aug-BOTHEND: ARC quota + MRU/LRU dual-end eviction ---
//
//  Same as AUG-TS4 but in replace(), compares predicted_next of front (MRU)
//  and back (LRU) of the target list. Evicts the key with the HIGHEST
//  predicted_next (furthest in the future). O(1) per eviction.
//
//  For looping: MRU's predicted_next = epoch + cycle_length > LRU's.
//  For Zipfian: LRU's predicted_next > MRU's (cold key has large gap).
//  Falls back to LRU if avgGap = 0 (no history).

class BareREMARC_Aug_BothEnd {
    enum Tier : uint8_t { NONE = 0, T1 = 1, T2 = 2, B1 = 3, B2 = 4 };

    struct BEntry {
        uint8_t state = 0;
        uint32_t lastEpoch = 0;
        uint32_t avgGap = 0;
        Tier tier = NONE;
        std::list<uint64_t>::iterator pos;
    };

    std::list<uint64_t> t1_, t2_, b1_, b2_;
    std::unordered_map<uint64_t, BEntry> idx_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;
    uint8_t demoteThresh_;

    std::list<uint64_t>& listFor(Tier t) {
        static std::list<uint64_t> dummy;
        switch (t) {
            case T1: return t1_; case T2: return t2_;
            case B1: return b1_; case B2: return b2_;
            default: return dummy;
        }
    }

    void pushFront(uint64_t key, Tier tier) {
        auto& l = listFor(tier);
        l.push_front(key);
        auto& e = idx_[key];
        e.tier = tier;
        e.pos = l.begin();
    }

    void eraseFromList(uint64_t key) {
        auto it = idx_.find(key);
        if (it != idx_.end() && it->second.tier != NONE) {
            listFor(it->second.tier).erase(it->second.pos);
            it->second.tier = NONE;
        }
    }

    void spliceToFront(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        auto& l = listFor(it->second.tier);
        l.splice(l.begin(), l, it->second.pos);
        it->second.pos = l.begin();
    }

    void moveTo(uint64_t key, Tier newTier) {
        auto it = idx_.find(key);
        if (it == idx_.end()) return;
        if (it->second.tier != NONE)
            listFor(it->second.tier).erase(it->second.pos);
        auto& l = listFor(newTier);
        l.push_front(key);
        it->second.tier = newTier;
        it->second.pos = l.begin();
    }

    uint64_t predictedNext(const BEntry& e) const {
        if (e.avgGap > 0) return (uint64_t)e.lastEpoch + e.avgGap;
        return e.lastEpoch;
    }

    uint64_t pickWorstEnd(std::list<uint64_t>& l) {
        if (l.size() <= 1) return l.front();
        uint64_t frontKey = l.front();
        uint64_t backKey = l.back();
        auto& fEntry = idx_[frontKey];
        auto& bEntry = idx_[backKey];
        if (fEntry.avgGap == 0) return backKey;
        uint64_t fPred = predictedNext(fEntry);
        uint64_t bPred = bEntry.avgGap > 0 ? predictedNext(bEntry) : bEntry.lastEpoch;
        return (fPred >= bPred) ? frontKey : backKey;
    }

    void replace(uint64_t key) {
        bool evictT1 = !t1_.empty() && (t1_.size() > p_ ||
            (idx_.count(key) && idx_[key].tier == B2 && t1_.size() == p_));
        std::list<uint64_t>& target = evictT1 ? t1_ : t2_;
        if (target.empty()) return;

        uint64_t victim = pickWorstEnd(target);
        eraseFromList(victim);
        pushFront(victim, evictT1 ? B1 : B2);
        evictions_++;
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) {
                uint64_t old = b1_.back(); b1_.pop_back(); idx_.erase(old);
            } else if (!t1_.empty()) {
                uint64_t victim = pickWorstEnd(t1_);
                eraseFromList(victim); idx_.erase(victim); evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) {
                uint64_t old = b2_.back(); b2_.pop_back(); idx_.erase(old);
            } else if (!t2_.empty()) {
                uint64_t victim = pickWorstEnd(t2_);
                eraseFromList(victim); idx_.erase(victim); evictions_++;
            }
        }
    }

    void touchState(uint64_t key) {
        auto it = idx_.find(key);
        if (it == idx_.end()) {
            idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, NONE, {}};
            return;
        }
        BEntry& e = it->second;
        uint32_t gap = epoch_ - e.lastEpoch;
        e.lastEpoch = epoch_;
        if (gap > 0) {
            e.avgGap = e.avgGap * 3 / 4 + gap / 4;
        }
        uint8_t s = e.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        e.state = augPack(r, f);
    }

    void demoteCheck(uint64_t key) {
        if (demoteThresh_ == 0) { spliceToFront(key); return; }
        auto it = idx_.find(key);
        if (it == idx_.end()) { spliceToFront(key); return; }
        uint32_t gap = epoch_ - it->second.lastEpoch;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < gap && r > 0; i++) r = augDecayR(r);
        if ((uint16_t)r + f <= demoteThresh_) {
            it->second.lastEpoch = epoch_;
            it->second.state = augPack(augDecayR(augGetR(s)), f);
            moveTo(key, T1);
        } else {
            it->second.lastEpoch = epoch_;
            spliceToFront(key);
        }
    }

public:
    BareREMARC_Aug_BothEnd(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                            size_t = 64, uint8_t demoteThresh = 4)
        : cap_(capacity), p_(0), demoteThresh_(demoteThresh) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        epoch_++;
        auto it = idx_.find(key);
        bool cached = it != idx_.end() && (it->second.tier == T1 || it->second.tier == T2);
        if (cached) {
            touchState(key);
            if (it->second.tier == T1) {
                moveTo(key, T2);
            } else {
                demoteCheck(key);
            }
            hits_++; return;
        }
        misses_++;
        if (it != idx_.end() && it->second.tier == B1) {
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            return;
        }
        if (it != idx_.end() && it->second.tier == B2) {
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            eraseFromList(key);
            doEvict(); replace(key);
            touchState(key);
            pushFront(key, T2);
            return;
        }
        doEvict();
        idx_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_, 0, NONE, {}};
        pushFront(key, T1);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return 0; }
};

// --- Aug-GS: Global sweep decay ---

class BareREMARC_Aug_GS {
    ArcList t1_, t2_, b1_, b2_;
    std::unordered_set<uint64_t> cached_;
    std::unordered_map<uint64_t, uint8_t> stateMap_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    size_t opCount_ = 0;
    size_t sweepInterval_;
    uint8_t demoteThresh_;

    void replace(uint64_t key) {
        if (!t1_.empty() && (t1_.size() > p_ ||
            (b2_.contains(key) && t1_.size() == p_))) {
            uint64_t old = t1_.back();
            cached_.erase(old); stateMap_.erase(old);
            t1_.pop_back(); b1_.push_front(old);
        } else if (!t2_.empty()) {
            uint64_t old = t2_.back();
            cached_.erase(old); stateMap_.erase(old);
            t2_.pop_back(); b2_.push_front(old);
        }
        evictions_++;
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) b1_.pop_back();
            else if (!t1_.empty()) {
                uint64_t old = t1_.back();
                cached_.erase(old); stateMap_.erase(old);
                t1_.pop_back(); evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) b2_.pop_back();
            else if (!t2_.empty()) {
                uint64_t old = t2_.back();
                cached_.erase(old); stateMap_.erase(old);
                t2_.pop_back(); evictions_++;
            }
        }
    }

    void boostState(uint64_t key) {
        auto it = stateMap_.find(key);
        if (it == stateMap_.end()) { stateMap_[key] = augPack(AUG_REMARC_MAX, 0); return; }
        uint8_t s = it->second;
        uint8_t r = augBoostR(augGetR(s)), f = augBoostF(augGetF(s));
        it->second = augPack(r, f);
    }

    void globalDecay() {
        std::vector<uint64_t> toDemote;
        for (auto& [key, entry] : stateMap_) {
            uint8_t s = entry;
            uint8_t r = augDecayR(augGetR(s));
            entry = augPack(r, augGetF(s));
            if (t2_.contains(key) && demoteThresh_ > 0 &&
                (uint16_t)r + augGetF(s) <= demoteThresh_) {
                toDemote.push_back(key);
            }
        }
        for (uint64_t k : toDemote) {
            if (!cached_.count(k) || !t2_.contains(k)) continue;
            t2_.erase(k); t1_.push_front(k);
        }
    }

public:
    BareREMARC_Aug_GS(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                       size_t sweepInterval = 64)
        : cap_(capacity), p_(0), sweepInterval_(sweepInterval), demoteThresh_(4) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        opCount_++;
        if (opCount_ % sweepInterval_ == 0) globalDecay();

        if (cached_.count(key)) {
            boostState(key);
            if (t1_.contains(key)) { t1_.erase(key); t2_.push_front(key); }
            else {
                if (demoteThresh_ > 0) {
                    auto it = stateMap_.find(key);
                    if (it != stateMap_.end()) {
                        uint8_t s = it->second;
                        if ((uint16_t)augGetR(s) + augGetF(s) <= demoteThresh_) {
                            t2_.erase(key); t1_.push_front(key);
                        } else t2_.splice_front(key);
                    } else t2_.splice_front(key);
                } else t2_.splice_front(key);
            }
            hits_++; return;
        }
        misses_++;
        if (b1_.contains(key)) {
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            doEvict(); replace(key); b1_.erase(key);
            cached_.insert(key); boostState(key); t2_.push_front(key);
            return;
        }
        if (b2_.contains(key)) {
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            doEvict(); replace(key); b2_.erase(key);
            cached_.insert(key); boostState(key); t2_.push_front(key);
            return;
        }
        doEvict();
        cached_.insert(key);
        stateMap_[key] = augPack(AUG_REMARC_MAX, 0);
        t1_.push_front(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return sweepInterval_ > 0 ? stateMap_.size() * opCount_ / sweepInterval_ : 0; }
};

// --- Aug-CB: Combined (global sweep + timestamp correction) ---

class BareREMARC_Aug_CB {
    ArcList t1_, t2_, b1_, b2_;
    std::unordered_set<uint64_t> cached_;
    std::unordered_map<uint64_t, AugEntry> stateMap_;
    size_t cap_, p_;
    size_t hits_ = 0, misses_ = 0, evictions_ = 0;
    uint32_t epoch_ = 0;
    size_t sweepInterval_;
    uint8_t demoteThresh_;

    void replace(uint64_t key) {
        if (!t1_.empty() && (t1_.size() > p_ ||
            (b2_.contains(key) && t1_.size() == p_))) {
            uint64_t old = t1_.back();
            cached_.erase(old); stateMap_.erase(old);
            t1_.pop_back(); b1_.push_front(old);
        } else if (!t2_.empty()) {
            uint64_t old = t2_.back();
            cached_.erase(old); stateMap_.erase(old);
            t2_.pop_back(); b2_.push_front(old);
        }
        evictions_++;
    }

    void doEvict() {
        if (t1_.size() + b1_.size() >= cap_) {
            if (t1_.size() < cap_ && !b1_.empty()) b1_.pop_back();
            else if (!t1_.empty()) {
                uint64_t old = t1_.back();
                cached_.erase(old); stateMap_.erase(old);
                t1_.pop_back(); evictions_++;
            }
        }
        if (t1_.size() + t2_.size() + b1_.size() + b2_.size() >= 2 * cap_) {
            if (!b2_.empty()) b2_.pop_back();
            else if (!t2_.empty()) {
                uint64_t old = t2_.back();
                cached_.erase(old); stateMap_.erase(old);
                t2_.pop_back(); evictions_++;
            }
        }
    }

    void touchState(uint64_t key) {
        auto it = stateMap_.find(key);
        if (it == stateMap_.end()) {
            stateMap_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_};
            return;
        }
        uint32_t idle = epoch_ - it->second.lastEpoch;
        it->second.lastEpoch = epoch_;
        uint8_t s = it->second.state;
        uint8_t r = augGetR(s), f = augGetF(s);
        for (uint32_t i = 0; i < idle && r > 0; i++) r = augDecayR(r);
        r = augBoostR(r);
        f = augBoostF(f);
        it->second.state = augPack(r, f);
    }

    void globalDecay() {
        std::vector<uint64_t> toDemote;
        for (auto& [key, entry] : stateMap_) {
            uint32_t idle = epoch_ - entry.lastEpoch;
            if (idle == 0) continue;
            uint8_t s = entry.state;
            uint8_t r = augGetR(s), f = augGetF(s);
            for (uint32_t i = 0; i < idle && r > 0; i++) r = augDecayR(r);
            entry.state = augPack(r, f);
            entry.lastEpoch = epoch_;
            if (t2_.contains(key) && demoteThresh_ > 0 &&
                (uint16_t)r + f <= demoteThresh_) {
                toDemote.push_back(key);
            }
        }
        for (uint64_t k : toDemote) {
            if (!cached_.count(k) || !t2_.contains(k)) continue;
            t2_.erase(k); t1_.push_front(k);
        }
    }

public:
    BareREMARC_Aug_CB(size_t capacity, size_t keysPerPage, const RemarcConfig&,
                       size_t sweepInterval = 64)
        : cap_(capacity), p_(0), sweepInterval_(sweepInterval), demoteThresh_(4) {
        (void)keysPerPage;
    }

    void access(uint64_t key) {
        epoch_++;
        if (epoch_ % sweepInterval_ == 0) globalDecay();

        if (cached_.count(key)) {
            touchState(key);
            if (t1_.contains(key)) { t1_.erase(key); t2_.push_front(key); }
            else {
                if (demoteThresh_ > 0) {
                    auto it = stateMap_.find(key);
                    if (it != stateMap_.end()) {
                        uint8_t s = it->second.state;
                        if ((uint16_t)augGetR(s) + augGetF(s) <= demoteThresh_) {
                            t2_.erase(key); t1_.push_front(key);
                        } else t2_.splice_front(key);
                    } else t2_.splice_front(key);
                } else t2_.splice_front(key);
            }
            hits_++; return;
        }
        misses_++;
        if (b1_.contains(key)) {
            size_t d = b1_.size() > 0 ? std::max(b2_.size() / b1_.size(), (size_t)1) : 1;
            p_ = std::min(cap_, p_ + d);
            doEvict(); replace(key); b1_.erase(key);
            cached_.insert(key); touchState(key); t2_.push_front(key);
            return;
        }
        if (b2_.contains(key)) {
            size_t d = b2_.size() > 0 ? std::max(b1_.size() / b2_.size(), (size_t)1) : 1;
            p_ = (p_ >= d) ? p_ - d : 0;
            doEvict(); replace(key); b2_.erase(key);
            cached_.insert(key); touchState(key); t2_.push_front(key);
            return;
        }
        doEvict();
        cached_.insert(key);
        stateMap_[key] = {augPack(AUG_REMARC_MAX, 0), epoch_};
        t1_.push_front(key);
    }

    size_t hits() const { return hits_; }
    size_t misses() const { return misses_; }
    size_t evictions() const { return evictions_; }
    size_t scans() const { return sweepInterval_ > 0 ? stateMap_.size() * (size_t)epoch_ / sweepInterval_ : 0; }
};

struct BenchResult {
    double hitRate;
    double missRate;
    double hits, misses, evictions;
    double scans;
    double elapsedMs;
    double throughputOps;
    double evictionPerMiss;
};

struct AggBench {
    BenchResult mean;
    BenchResult stddev;
};

static double Mean(const std::vector<double>& v) {
    if (v.empty()) return 0.0;
    double s = 0.0;
    for (double x : v) s += x;
    return s / (double)v.size();
}

static double StdDev(const std::vector<double>& v, double mean) {
    if (v.size() <= 1) return 0.0;
    double s = 0.0;
    for (double x : v) {
        double d = x - mean;
        s += d * d;
    }
    return std::sqrt(s / (double)v.size());
}

static AggBench Aggregate(const std::vector<BenchResult>& samples) {
    auto make = [&](auto get) {
        std::vector<double> v;
        v.reserve(samples.size());
        for (const auto& s : samples) v.push_back(get(s));
        double m = Mean(v);
        double sd = StdDev(v, m);
        return std::pair<double, double>{m, sd};
    };

    AggBench out{};

    auto [hitM, hitS] = make([](const BenchResult& r){ return r.hitRate; });
    auto [missM, missS] = make([](const BenchResult& r){ return r.missRate; });
    auto [hitsM, hitsS] = make([](const BenchResult& r){ return r.hits; });
    auto [mM, mS] = make([](const BenchResult& r){ return r.misses; });
    auto [eM, eS] = make([](const BenchResult& r){ return r.evictions; });
    auto [scM, scS] = make([](const BenchResult& r){ return r.scans; });
    auto [tM, tS] = make([](const BenchResult& r){ return r.elapsedMs; });
    auto [opsM, opsS] = make([](const BenchResult& r){ return r.throughputOps; });
    auto [epmM, epmS] = make([](const BenchResult& r){ return r.evictionPerMiss; });

    out.mean = {hitM, missM, hitsM, mM, eM, scM, tM, opsM, epmM};
    out.stddev = {hitS, missS, hitsS, mS, eS, scS, tS, opsS, epmS};
    return out;
}

template<typename Cache>
BenchResult runBench(Cache& cache, const std::vector<uint64_t>& wl) {
    auto t0 = std::chrono::high_resolution_clock::now();
    for (uint64_t k : wl) cache.access(k);
    auto t1 = std::chrono::high_resolution_clock::now();

    BenchResult r;
    r.hits = cache.hits();
    r.misses = cache.misses();
    r.evictions = cache.evictions();
    if constexpr (requires (const Cache& c) { c.scans(); }) {
        r.scans = cache.scans();
    } else {
        r.scans = 0;
    }
    r.hitRate = 100.0 * r.hits / wl.size();
    r.missRate = 100.0 - r.hitRate;
    r.elapsedMs = std::chrono::duration<double, std::milli>(t1 - t0).count();
    r.throughputOps = (r.elapsedMs > 0.0) ? (wl.size() / (r.elapsedMs / 1000.0)) : 0.0;
    r.evictionPerMiss = (r.misses > 0) ? (double)r.evictions / (double)r.misses : 0.0;
    return r;
}

void printRow(const char* name, const BenchResult& r) {
    std::cout << std::left << std::setw(20) << name
              << " | " << std::right << std::setw(7) << std::fixed << std::setprecision(2) << r.hitRate << "%"
              << " | " << std::setw(8) << r.hits
              << " | " << std::setw(8) << r.misses
              << " | " << std::setw(6) << r.evictions
              << " | " << std::setw(6) << r.scans
              << " | " << std::setw(9) << std::setprecision(1) << r.throughputOps
              << " | " << std::setw(8) << r.elapsedMs << " ms"
              << std::endl;
}

void printAggRow(const char* name, const AggBench& r) {
    std::cout << std::left << std::setw(20) << name
              << " | " << std::right << std::setw(6) << std::fixed << std::setprecision(2) << r.mean.hitRate
              << "±" << std::setw(4) << std::setprecision(2) << r.stddev.hitRate
              << " | " << std::setw(7) << std::setprecision(0) << r.mean.hits
              << " | " << std::setw(7) << std::setprecision(0) << r.mean.misses
              << " | " << std::setw(6) << std::setprecision(0) << r.mean.evictions
              << " | " << std::setw(5) << std::setprecision(0) << r.mean.scans
              << " | " << std::setw(7) << std::setprecision(1) << r.mean.throughputOps
              << "±" << std::setw(6) << std::setprecision(1) << r.stddev.throughputOps
              << " | " << std::setw(6) << std::setprecision(2) << r.mean.evictionPerMiss
              << "±" << std::setw(5) << std::setprecision(2) << r.stddev.evictionPerMiss
              << std::endl;
}

struct CsvRow {
    std::string workload;
    std::string policy;
    AggBench result;
    double workloadScore;
};

struct ModeConfig {
    const char* name;
    size_t ops;
    int iterations;
    bool runExtended;
};

static void PrintUsage() {
    std::cout << "Usage: PolicyBench [--quick|--paper]\n"
              << "  --quick : fast local profile (3 iterations, reduced sections).\n"
              << "  --paper : publication mode (10 iterations, full sections).\n";
}

void RunLimitTests() {
    std::cout << "=== ARC-FLATLL2 LIMIT TESTS ===" << std::endl;
    int pass = 0, fail = 0;

    auto check = [&](const char* name, bool cond) {
        if (cond) { std::cout << "  PASS: " << name << std::endl; pass++; }
        else { std::cout << "  FAIL: " << name << std::endl; fail++; }
    };

    auto checkMatch = [&](const char* name, size_t cap,
                          const std::vector<uint64_t>& wl) {
        BareARC ref(cap);
        BareARC_FlatLL2 opt(cap);
        for (auto k : wl) { ref.access(k); opt.access(k); }
        bool ok = (ref.hits() == opt.hits() && ref.misses() == opt.misses()
                   && ref.evictions() == opt.evictions());
        if (ok) { std::cout << "  PASS: " << name << " (h=" << ref.hits()
                  << " m=" << ref.misses() << " e=" << ref.evictions() << ")" << std::endl; pass++; }
        else { std::cout << "  FAIL: " << name
                  << " ARC(h=" << ref.hits() << ",m=" << ref.misses() << ",e=" << ref.evictions() << ")"
                  << " FLATLL2(h=" << opt.hits() << ",m=" << opt.misses() << ",e=" << opt.evictions() << ")"
                  << std::endl; fail++; }
    };

    {
        std::cout << "\n--- Cap=1 (minimum) ---" << std::endl;
        BareARC_FlatLL2 arc(1);
        arc.access(1); arc.access(2); arc.access(1); arc.access(2); arc.access(3);
        check("no crash cap=1", arc.hits() + arc.misses() == 5);
        checkMatch("cap=1 alternating 3 keys", 1, {1,2,1,2,3,1,2,3,4,5});
    }

    {
        std::cout << "\n--- Cap=2 ---" << std::endl;
        checkMatch("cap=2 sequential scan", 2, {1,2,3,4,5,6,7,8,9,10});
        checkMatch("cap=2 repeating 3 keys", 2, {1,2,3,1,2,3,1,2,3,1,2,3});
        checkMatch("cap=2 all same key", 2, {5,5,5,5,5,5,5,5,5,5});
    }

    {
        std::cout << "\n--- Cap=universe (no eviction expected after warmup) ---" << std::endl;
        std::vector<uint64_t> wl;
        for (uint64_t i = 0; i < 10; i++) wl.push_back(i);
        for (uint64_t i = 0; i < 10; i++) wl.push_back(i);
        checkMatch("cap=10 universe=10 (revisit all)", 10, wl);
        check("cap=10 all hits on second pass", [&]{
            BareARC_FlatLL2 a(10);
            for (uint64_t i = 0; i < 10; i++) a.access(i);
            size_t h0 = a.hits();
            for (uint64_t i = 0; i < 10; i++) a.access(i);
            return a.hits() - h0 == 10;
        }());
    }

    {
        std::cout << "\n--- Pure sequential scan (every access misses) ---" << std::endl;
        std::vector<uint64_t> wl;
        for (uint64_t i = 0; i < 5000; i++) wl.push_back(i);
        checkMatch("cap=100 pure scan 5000 keys", 100, wl);
    }

    {
        std::cout << "\n--- Ghost list pressure (drive p to extremes) ---" << std::endl;
        std::vector<uint64_t> wl;
        for (int r = 0; r < 20; r++) {
            for (uint64_t i = 0; i < 50; i++) wl.push_back(i);
            for (uint64_t i = 50; i < 150; i++) wl.push_back(i);
        }
        checkMatch("cap=50 alternating hot/cold blocks", 50, wl);
    }

    {
        std::cout << "\n--- Single key repeated ---" << std::endl;
        std::vector<uint64_t> wl(1000, 42);
        checkMatch("cap=100 single key 1000 times", 100, wl);
        checkMatch("cap=1 single key 100 times", 1, wl);
    }

    {
        std::cout << "\n--- Large cap (verify no uint32_t overflow) ---" << std::endl;
        BareARC_FlatLL2 arc(100000);
        std::mt19937_64 rng(123);
        for (int i = 0; i < 500000; i++) arc.access(rng() % 500000);
        check("cap=100000 no crash", arc.hits() + arc.misses() == 500000);

        BareARC ref(100000);
        rng.seed(123);
        for (int i = 0; i < 500000; i++) ref.access(rng() % 500000);
        check("cap=100000 hit rate match", ref.hits() == arc.hits());
    }

    {
        std::cout << "\n--- Max list occupancy (force T2 to 2*cap) ---" << std::endl;
        size_t cap = 100;
        std::vector<uint64_t> wl;
        for (uint64_t i = 0; i < (uint64_t)cap; i++) wl.push_back(i);
        for (int r = 0; r < 10; r++) {
            for (uint64_t i = 0; i < (uint64_t)cap; i++) wl.push_back(i);
            for (uint64_t i = (uint64_t)cap; i < (uint64_t)(cap * 3); i++) wl.push_back(i);
        }
        checkMatch("cap=100 T2 pressure via ghost hits", cap, wl);
    }

    {
        std::cout << "\n--- Empty workload ---" << std::endl;
        BareARC_FlatLL2 arc(10);
        check("zero ops", arc.hits() == 0 && arc.misses() == 0 && arc.evictions() == 0);
    }

    std::cout << "\n=== LIMIT TESTS: " << pass << " passed, " << fail << " failed ===" << std::endl;
}

int main(int argc, char** argv) {
    const size_t capacity = 1000;
    const size_t universe = 10000;
    const size_t kpp = 32;

    ModeConfig mode{"quick", 100000, 3, false};
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--paper") mode = {"paper", 300000, 10, true};
        else if (arg == "--quick") mode = {"quick", 100000, 3, false};
        else if (arg == "--limits") { RunLimitTests(); return 0; }
        else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            return 0;
        }
    }

    std::cout << "=== POLICY BENCHMARK ===" << std::endl;
    std::cout << "mode=" << mode.name << " iterations=" << mode.iterations << "\n";
    std::cout << "capacity=" << capacity << " universe=" << universe
              << " ops=" << mode.ops << " kpp=" << kpp << std::endl;
    std::cout << std::endl;

    std::vector<CsvRow> csvRows;

    std::mt19937_64 rng(42);
    auto wlZipf = genZipfian(mode.ops, universe, 0.99, rng);
    rng.seed(42);
    auto wlScan = genScanResistant(mode.ops, capacity, universe, rng);
    auto wlTemp = genTemporalShift(mode.ops, universe, 4);
    auto wlLoop = genLooping(mode.ops, capacity * 2);

    RemarcConfig defaultCfg;

    {
        BareARC verifyARC(capacity);
        BareARC_Flat verifyARCFlat(capacity);
        BareARC_FlatLL verifyARCFlatLL(capacity);
        BareARC_FlatLL2 verifyARCFlatLL2(capacity);
        BareREMARC_Aug_TS verifyTS(capacity, kpp, defaultCfg);
        BareREMARC_Aug_GS verifyGS(capacity, kpp, defaultCfg);
        BareREMARC_Aug_CB verifyCB(capacity, kpp, defaultCfg);
        BareREMARC_Aug_PRED verifyPRED(capacity, kpp, defaultCfg);
        for (auto k : wlZipf) {
            verifyARC.access(k); verifyARCFlat.access(k); verifyARCFlatLL.access(k); verifyARCFlatLL2.access(k); verifyTS.access(k);
            verifyGS.access(k); verifyCB.access(k);
            verifyPRED.access(k);
        }
        std::cout << "\nVERIFY ARC:      " << verifyARC.hits() << " hits, " << verifyARC.misses() << " misses, " << verifyARC.evictions() << " evictions" << std::endl;
        std::cout << "VERIFY ARC-FLAT: " << verifyARCFlat.hits() << " hits, " << verifyARCFlat.misses() << " misses, " << verifyARCFlat.evictions() << " evictions" << std::endl;
        std::cout << "VERIFY ARC-FLATLL: " << verifyARCFlatLL.hits() << " hits, " << verifyARCFlatLL.misses() << " misses, " << verifyARCFlatLL.evictions() << " evictions" << std::endl;
        std::cout << "VERIFY ARC-FLATLL2: " << verifyARCFlatLL2.hits() << " hits, " << verifyARCFlatLL2.misses() << " misses, " << verifyARCFlatLL2.evictions() << " evictions" << std::endl;
        if (verifyARC.hits() != verifyARCFlat.hits() || verifyARC.misses() != verifyARCFlat.misses() || verifyARC.hits() != verifyARCFlatLL.hits() || verifyARC.misses() != verifyARCFlatLL.misses() || verifyARC.hits() != verifyARCFlatLL2.hits() || verifyARC.misses() != verifyARCFlatLL2.misses()) {
            std::cout << "*** MISMATCH: ARC / ARC-FLAT / ARC-FLATLL / ARC-FLATLL2 produced different hit/miss counts! ***" << std::endl;
        } else {
            std::cout << "MATCH: All ARC variants produce identical hit/miss counts." << std::endl;
        }
        std::cout << "VERIFY AUG-TS: " << verifyTS.hits() << " hits, " << verifyTS.misses() << " misses, " << verifyTS.evictions() << " evictions" << std::endl;
        std::cout << "VERIFY AUG-PRED: " << verifyPRED.hits() << " hits, " << verifyPRED.misses() << " misses, " << verifyPRED.evictions() << " evictions" << std::endl;
        std::cout << "VERIFY AUG-GS: " << verifyGS.hits() << " hits, " << verifyGS.misses() << " misses, " << verifyGS.evictions() << " evictions" << std::endl;
        std::cout << "VERIFY AUG-CB: " << verifyCB.hits() << " hits, " << verifyCB.misses() << " misses, " << verifyCB.evictions() << " evictions" << std::endl;
    }

    std::map<std::string, double> weightedTotals;
    std::map<std::string, int> weightedCounts;

    const char* const policyNames[] = {
        "LRU",
        "ARC",
        "ARC-FLAT",
        "ARC-FLATLL",
        "ARC-FLATLL2",
        "REMARC",
        "REMARC-OPT",
        "REMARC-LazyInc",
        "REMARC-S3",
        "REMARC-LFU",
        "REMARC-TinyLFU",
        "REMARC-GhostField",
        "REMARC-GhostGate",
        "REMARC-BloomGate",
        "REMARC-BloomMod",
        "REMARC-FACT",
        "REMARC-TIER",
        "REMARC-Dual",
        "REMARC-MS",
        "REMARC-STEP",
        "REMARC-STEPK",
        "AUG-TS4",
        "AUG-PRED",
        "AUG-HEAP",
        "AUG-BOTHEND",
        "AUG-ADAPT",
        "QuotaFree",
    };
    constexpr int NPOL = sizeof(policyNames) / sizeof(policyNames[0]);

    auto runWorkload = [&](const char* name, const std::vector<uint64_t>& wl) {
        std::map<std::string, std::vector<BenchResult>> samples;
        for (int i = 0; i < NPOL; i++) samples[policyNames[i]] = {};

        for (int it = 0; it < mode.iterations; it++) {
            BareLRU lru(capacity);
            samples["LRU"].push_back(runBench(lru, wl));
            BareARC arc(capacity);
            samples["ARC"].push_back(runBench(arc, wl));
            BareARC_Flat arcflat(capacity);
            samples["ARC-FLAT"].push_back(runBench(arcflat, wl));
            BareARC_FlatLL arcflatll(capacity);
            samples["ARC-FLATLL"].push_back(runBench(arcflatll, wl));
            BareARC_FlatLL2 arcflatll2(capacity);
            samples["ARC-FLATLL2"].push_back(runBench(arcflatll2, wl));
            BareREMARC remarc(capacity, kpp, defaultCfg);
            samples["REMARC"].push_back(runBench(remarc, wl));
            BareREMARC_Opt opt(capacity, kpp, defaultCfg);
            samples["REMARC-OPT"].push_back(runBench(opt, wl));
            BareREMARC_LazyInc li(capacity, kpp, defaultCfg);
            samples["REMARC-LazyInc"].push_back(runBench(li, wl));
            BareREMARC_S3FIFO s3(capacity, kpp, defaultCfg);
            samples["REMARC-S3"].push_back(runBench(s3, wl));
            BareREMARC_LFU lfu(capacity, kpp, defaultCfg);
            samples["REMARC-LFU"].push_back(runBench(lfu, wl));
            BareREMARC_TinyLFU tlfu(capacity, kpp, defaultCfg);
            samples["REMARC-TinyLFU"].push_back(runBench(tlfu, wl));
            BareREMARC_GhostField gf(capacity, kpp, defaultCfg);
            samples["REMARC-GhostField"].push_back(runBench(gf, wl));
            BareREMARC_GhostGate gg(capacity, kpp, defaultCfg);
            samples["REMARC-GhostGate"].push_back(runBench(gg, wl));
            BareREMARC_BloomGate bg(capacity, kpp, defaultCfg);
            samples["REMARC-BloomGate"].push_back(runBench(bg, wl));
            BareREMARC_BloomMod bm(capacity, kpp, defaultCfg);
            samples["REMARC-BloomMod"].push_back(runBench(bm, wl));
            BareREMARC_Factorized fac(capacity, kpp, defaultCfg);
            samples["REMARC-FACT"].push_back(runBench(fac, wl));
            BareREMARC_TierGhost tg(capacity, kpp, defaultCfg);
            samples["REMARC-TIER"].push_back(runBench(tg, wl));
            BareREMARC_Dual dual(capacity, kpp, defaultCfg);
            samples["REMARC-Dual"].push_back(runBench(dual, wl));
            BareREMARC_MultiScale ms(capacity, kpp, defaultCfg);
            samples["REMARC-MS"].push_back(runBench(ms, wl));
            BareREMARC_Step step(capacity, kpp, defaultCfg);
            samples["REMARC-STEP"].push_back(runBench(step, wl));
            BareREMARC_StepK stepk(capacity, kpp, defaultCfg);
            samples["REMARC-STEPK"].push_back(runBench(stepk, wl));
            BareREMARC_Aug_TS augTS4(capacity, kpp, defaultCfg, 64, 4);
            samples["AUG-TS4"].push_back(runBench(augTS4, wl));
            BareREMARC_Aug_PRED augPRED(capacity, kpp, defaultCfg, 64, 4);
            samples["AUG-PRED"].push_back(runBench(augPRED, wl));
            BareREMARC_Aug_HEAP augHEAP(capacity, kpp, defaultCfg, 64, 4);
            samples["AUG-HEAP"].push_back(runBench(augHEAP, wl));
            BareREMARC_Aug_BothEnd augBE(capacity, kpp, defaultCfg, 64, 4);
            samples["AUG-BOTHEND"].push_back(runBench(augBE, wl));
            BareREMARC_Aug_ADAPT augADAPT(capacity, kpp, defaultCfg, 64, 4);
            samples["AUG-ADAPT"].push_back(runBench(augADAPT, wl));
            BareREMARC_QuotaFree qf(capacity, kpp, defaultCfg, 64, 4);
            samples["QuotaFree"].push_back(runBench(qf, wl));
        }

        std::map<std::string, AggBench> agg;
        for (auto& [policy, vec] : samples) agg[policy] = Aggregate(vec);

        double maxHit = 0.0, maxOps = 0.0;
        double minEpm = 1e18;
        for (const auto& [policy, a] : agg) {
            maxHit = std::max(maxHit, a.mean.hitRate);
            maxOps = std::max(maxOps, a.mean.throughputOps);
            if (a.mean.evictionPerMiss > 0.0) minEpm = std::min(minEpm, a.mean.evictionPerMiss);
        }
        if (minEpm == 1e18) minEpm = 1.0;

        std::cout << "--- " << name << " ---" << std::endl;
        std::cout << std::left << std::setw(20) << "Policy"
                  << " | " << std::right << std::setw(12) << "Hit%(avg±sd)"
                  << " | " << std::setw(7) << "Hits"
                  << " | " << std::setw(7) << "Misses"
                  << " | " << std::setw(6) << "Evicts"
                  << " | " << std::setw(5) << "Scan"
                  << " | " << std::setw(15) << "Ops/s(avg±sd)"
                  << " | " << std::setw(14) << "Evict/M(avg±sd)"
                  << std::endl;
        std::cout << std::string(110, '-') << std::endl;

        for (int i = 0; i < NPOL; i++) {
            const char* policy = policyNames[i];
            printAggRow(policy, agg[policy]);

            double hitNorm = (maxHit > 0.0) ? (agg[policy].mean.hitRate / maxHit) : 0.0;
            double opsNorm = (maxOps > 0.0) ? (agg[policy].mean.throughputOps / maxOps) : 0.0;
            double effNorm = (agg[policy].mean.evictionPerMiss > 0.0)
                ? (minEpm / agg[policy].mean.evictionPerMiss) : 1.0;
            double score = 0.60 * hitNorm + 0.25 * opsNorm + 0.15 * effNorm;

            csvRows.push_back({name, policy, agg[policy], score});
            weightedTotals[policy] += score;
            weightedCounts[policy] += 1;
        }

        std::cout << std::endl;
    };

    runWorkload("Zipfian (theta=0.99)", wlZipf);
    runWorkload("Scan-Resistant (90% hot)", wlScan);
    runWorkload("Temporal Shift (period=4)", wlTemp);
    runWorkload("Looping (loop=2000)", wlLoop);

    std::cout << "=== KPI SCOREBOARD (weights: Hit 0.60, Ops/s 0.25, EvictEff 0.15) ===" << std::endl;
    std::vector<std::pair<std::string, double>> ranking;
    for (const auto& [policy, total] : weightedTotals) {
        double avgScore = total / (double)std::max(1, weightedCounts[policy]);
        ranking.push_back({policy, avgScore * 100.0});
    }
    std::sort(ranking.begin(), ranking.end(), [](const auto& a, const auto& b){ return a.second > b.second; });
    for (const auto& [policy, score] : ranking) {
        std::cout << "  " << std::left << std::setw(8) << policy
                  << " : " << std::fixed << std::setprecision(2) << score << std::endl;
    }
    std::cout << std::endl;

    if (mode.runExtended) {
        std::cout << "=== CAPACITY SWEEP (Zipfian theta=0.99, avg±sd) ===" << std::endl;
        std::cout << std::left << std::setw(12) << "Cap/Keys%"
                  << " | " << std::right << std::setw(13) << "ARC Hit%"
                  << " | " << std::setw(13) << "REMARC%"
                  << " | " << std::setw(8) << "Delta"
                  << std::endl;
        std::cout << std::string(60, '-') << std::endl;

        for (double ratio : {0.05, 0.10, 0.15, 0.20, 0.25, 0.30}) {
            size_t cap = (size_t)(universe * ratio);
            std::vector<BenchResult> arcSamples;
            std::vector<BenchResult> remarcSamples;
            for (int it = 0; it < mode.iterations; it++) {
                BareARC arcSweep(cap);
                BareREMARC remarcSweep(cap, kpp, defaultCfg);
                arcSamples.push_back(runBench(arcSweep, wlZipf));
                remarcSamples.push_back(runBench(remarcSweep, wlZipf));
            }
            auto aArc = Aggregate(arcSamples);
            auto aRemarc = Aggregate(remarcSamples);
            double delta = aRemarc.mean.hitRate - aArc.mean.hitRate;

            std::cout << std::left << std::setw(12) << (std::to_string((int)(ratio * 100)) + "%")
                      << " | " << std::right << std::setw(6) << std::fixed << std::setprecision(2) << aArc.mean.hitRate
                      << "±" << std::setw(5) << aArc.stddev.hitRate
                      << " | " << std::setw(6) << aRemarc.mean.hitRate
                      << "±" << std::setw(5) << aRemarc.stddev.hitRate
                      << " | " << std::setw(7) << std::showpos << delta << std::noshowpos
                      << std::endl;
        }
        std::cout << std::endl;

        std::cout << "=== REMARC-OPT PARAMETER SENSITIVITY ===" << std::endl;
        std::cout << std::left << std::setw(30) << "Config"
                  << " | " << std::right
                  << std::setw(10) << "Zipf%"
                  << " | " << std::setw(10) << "ScanRes%"
                  << " | " << std::setw(10) << "TempShift%"
                  << " | " << std::setw(10) << "Loop%"
                  << std::endl;
        std::cout << std::string(80, '-') << std::endl;

        size_t cap10 = universe / 10;
        auto testCfgOpt = [&](const char* label, RemarcConfig cfg) {
            std::vector<BenchResult> z, s, t, l;
            for (int it = 0; it < mode.iterations; it++) {
                BareREMARC_Opt rz(cap10, kpp, cfg);
                z.push_back(runBench(rz, wlZipf));
                BareREMARC_Opt rs(cap10, kpp, cfg);
                s.push_back(runBench(rs, wlScan));
                BareREMARC_Opt rt(cap10, kpp, cfg);
                t.push_back(runBench(rt, wlTemp));
                BareREMARC_Opt rl(cap10, kpp, cfg);
                l.push_back(runBench(rl, wlLoop));
            }
            auto az = Aggregate(z), as = Aggregate(s), at = Aggregate(t), al = Aggregate(l);
            std::cout << std::left << std::setw(30) << label
                      << " | " << std::right << std::setw(6) << std::fixed << std::setprecision(1) << az.mean.hitRate
                      << "±" << std::setw(3) << std::setprecision(1) << az.stddev.hitRate
                      << " | " << std::setw(6) << as.mean.hitRate
                      << "±" << std::setw(3) << as.stddev.hitRate
                      << " | " << std::setw(6) << at.mean.hitRate
                      << "±" << std::setw(3) << at.stddev.hitRate
                      << " | " << std::setw(6) << al.mean.hitRate
                      << "±" << std::setw(3) << al.stddev.hitRate
                      << std::endl;
        };

        testCfgOpt("default (aL=2,aR=1,tE=12,td=7/8)", defaultCfg);
        testCfgOpt("high local boost (aL=4,aR=1)", {4, 1, 12, 10, 13, 7, 8});
        testCfgOpt("symmetric (aL=2,aR=2)", {2, 2, 12, 10, 13, 7, 8});
        testCfgOpt("aggressive (aL=4,aR=2,tE=10)", {4, 2, 10, 10, 13, 7, 8});
        testCfgOpt("conservative (aL=1,aR=1,tE=14)", {1, 1, 14, 10, 13, 7, 8});
        testCfgOpt("slow scan decay (7/16)", {2, 1, 12, 10, 13, 7, 16});
        testCfgOpt("fast scan decay (7/4)", {2, 1, 12, 10, 13, 7, 4});
        testCfgOpt("low evict thresh (tE=8)", {2, 1, 8, 10, 13, 7, 8});
        testCfgOpt("high evict thresh (tE=14)", {2, 1, 14, 10, 13, 7, 8});
        testCfgOpt("max boost (aL=7,aR=3)", {7, 3, 12, 10, 13, 7, 8});
        testCfgOpt("tiny boost (aL=1,aR=1)", {1, 1, 12, 10, 13, 7, 8});
        std::cout << std::endl;

        std::cout << "=== ARC BASELINE (same workloads) ===" << std::endl;
        std::cout << std::left << std::setw(30) << "Config"
                  << " | " << std::right
                  << std::setw(10) << "Zipf%"
                  << " | " << std::setw(10) << "ScanRes%"
                  << " | " << std::setw(10) << "TempShift%"
                  << " | " << std::setw(10) << "Loop%"
                  << std::endl;
        std::cout << std::string(80, '-') << std::endl;
        {
            std::vector<BenchResult> bz, bs, bt, bl;
            for (int it = 0; it < mode.iterations; it++) {
                BareARC rz(cap10); bz.push_back(runBench(rz, wlZipf));
                BareARC rs(cap10); bs.push_back(runBench(rs, wlScan));
                BareARC rt(cap10); bt.push_back(runBench(rt, wlTemp));
                BareARC rl(cap10); bl.push_back(runBench(rl, wlLoop));
            }
            auto azr = Aggregate(bz), asr = Aggregate(bs), atr = Aggregate(bt), alr = Aggregate(bl);
            std::cout << std::left << std::setw(30) << "ARC"
                      << " | " << std::right << std::setw(6) << std::fixed << std::setprecision(1) << azr.mean.hitRate
                      << "±" << std::setw(3) << std::setprecision(1) << azr.stddev.hitRate
                      << " | " << std::setw(6) << asr.mean.hitRate
                      << "±" << std::setw(3) << asr.stddev.hitRate
                      << " | " << std::setw(6) << atr.mean.hitRate
                      << "±" << std::setw(3) << atr.stddev.hitRate
                      << " | " << std::setw(6) << alr.mean.hitRate
                      << "±" << std::setw(3) << alr.stddev.hitRate
                      << std::endl;
        }
        std::cout << std::endl;
    }

    std::ofstream csv("policy-bench.csv", std::ios::trunc);
    if (csv.is_open()) {
        csv << "workload,policy,hit_rate_mean,hit_rate_std,miss_rate_mean,miss_rate_std,hits_mean,hits_std,misses_mean,misses_std,evictions_mean,evictions_std,scans_mean,scans_std,throughput_ops_mean,throughput_ops_std,elapsed_ms_mean,elapsed_ms_std,evict_per_miss_mean,evict_per_miss_std,workload_score\n";
        for (const auto& row : csvRows) {
            csv << '"' << row.workload << "\"," << row.policy << ','
                << std::fixed << std::setprecision(4)
                << row.result.mean.hitRate << ',' << row.result.stddev.hitRate << ','
                << row.result.mean.missRate << ',' << row.result.stddev.missRate << ','
                << row.result.mean.hits << ',' << row.result.stddev.hits << ','
                << row.result.mean.misses << ',' << row.result.stddev.misses << ','
                << row.result.mean.evictions << ',' << row.result.stddev.evictions << ','
                << row.result.mean.scans << ',' << row.result.stddev.scans << ','
                << row.result.mean.throughputOps << ',' << row.result.stddev.throughputOps << ','
                << row.result.mean.elapsedMs << ',' << row.result.stddev.elapsedMs << ','
                << row.result.mean.evictionPerMiss << ',' << row.result.stddev.evictionPerMiss << ','
                << row.workloadScore << "\n";
        }
        std::cout << "\nWrote KPI CSV: policy-bench.csv" << std::endl;
    }

    // =====================================================================
    //  THREAD SCALING (single-node, bare policy throughput under contention)
    //  Each thread gets its own cache instance (shared-nothing), then we
    //  measure aggregate throughput vs thread count.  This isolates policy
    //  overhead from any shared data structure contention.
    // =====================================================================
    {
        std::cout << "\n=== THREAD SCALING (shared-nothing, Zipfian 100k ops/thread, cap=1000) ===" << std::endl;
        std::cout << std::left
                  << std::setw(8) << "Threads"
                  << " | " << std::right
                  << std::setw(12) << "LRU Mops"
                  << " | " << std::setw(12) << "ARC Mops"
                  << " | " << std::setw(12) << "AUG Mops"
                  << " | " << std::setw(7) << "LRU Hit%"
                  << " | " << std::setw(7) << "ARC Hit%"
                  << " | " << std::setw(7) << "AUG Hit%"
                  << std::endl;
        std::cout << std::string(85, '-') << std::endl;

        size_t opsPerThread = 50000;
        size_t tcap = 1000;
        size_t tuniv = 10000;

        auto runMT = [&]<typename Cache>(auto makeCache, const std::vector<uint64_t>& wl,
                                          int nt, std::vector<double>& times, std::vector<double>& hits) {
            times.clear(); hits.clear();
            std::vector<Cache> caches;
            caches.reserve(nt);
            for (int i = 0; i < nt; i++) caches.push_back(makeCache());

            std::vector<std::thread> threads(nt);
            std::vector<std::pair<size_t, double>> results(nt);

            for (int t = 0; t < nt; t++) {
                threads[t] = std::thread([&, t]() {
                    auto t0 = std::chrono::high_resolution_clock::now();
                    caches[t].access(wl[0]);
                    caches[t].access(wl[wl.size() - 1]);
                    size_t h = caches[t].hits();
                    for (size_t i = 0; i < wl.size(); i++) {
                        caches[t].access(wl[i]);
                    }
                    h = caches[t].hits();
                    auto t1 = std::chrono::high_resolution_clock::now();
                    results[t] = {h, std::chrono::duration<double, std::milli>(t1 - t0).count()};
                });
            }
            for (auto& th : threads) th.join();

            for (auto& [h, ms] : results) {
                times.push_back(ms);
                hits.push_back(100.0 * (double)h / (double)(wl.size()));
            }
        };

        for (int nt : {1, 2, 4, 8}) {
            std::vector<double> lruTimes, arcTimes, augTimes;
            std::vector<double> lruHits, arcHits, augHits;

            for (int it = 0; it < mode.iterations; it++) {
                std::mt19937_64 rngT(42);
                auto wl = genZipfian(opsPerThread, tuniv, 0.99, rngT);

                runMT.operator()<BareLRU>([&](){ return BareLRU(tcap); }, wl, nt, lruTimes, lruHits);
                runMT.operator()<BareARC>([&](){ return BareARC(tcap); }, wl, nt, arcTimes, arcHits);
                runMT.operator()<BareREMARC_Aug_CB>([&](){ return BareREMARC_Aug_CB(tcap, kpp, defaultCfg); }, wl, nt, augTimes, augHits);
            }

            double lruAvg = Mean(lruTimes), arcAvg = Mean(arcTimes), augAvg = Mean(augTimes);
            double totalOps = (double)(nt * opsPerThread);

            auto formatMops = [&](double avgMs) -> std::string {
                double mops = totalOps / (avgMs / 1000.0) / 1e6;
                std::ostringstream ss;
                ss << std::fixed << std::setprecision(2) << mops;
                return ss.str();
            };

            std::cout << std::left << std::setw(8) << nt
                      << " | " << std::right << std::setw(12) << formatMops(lruAvg)
                      << " | " << std::setw(12) << formatMops(arcAvg)
                      << " | " << std::setw(12) << formatMops(augAvg)
                      << " | " << std::setw(7) << std::fixed << std::setprecision(1) << Mean(lruHits)
                      << " | " << std::setw(7) << Mean(arcHits)
                      << " | " << std::setw(7) << Mean(augHits)
                      << std::endl;
        }
        std::cout << std::endl;
    }

    return 0;
}
