#pragma once
#include <atomic>
#include <cstring>
#include <type_traits>

namespace StreamLine {

    template<typename T>
    requires std::is_trivially_copyable_v<T>
    class SeqLock {
        alignas(64) std::atomic<unsigned> seq{0};
        alignas(64) T storage;

        static void cpu_pause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
            __asm__ __volatile__("pause");
#elif defined(__aarch64__)
            __asm__ __volatile__("yield");
#endif
        }

    public:
        SeqLock() = default;
        explicit SeqLock(const T& init) : storage(init) {}

        T Read() const noexcept {
            T copy;
            unsigned s1, s2;
            do {
                s1 = seq.load(std::memory_order_acquire);
                if (s1 & 1u) { cpu_pause(); continue; }
                std::atomic_thread_fence(std::memory_order_acquire);
                std::memcpy(&copy, &storage, sizeof(T));
                std::atomic_thread_fence(std::memory_order_acquire);
                s2 = seq.load(std::memory_order_acquire);
            } while (s1 != s2);
            return copy;
        }

        void Write(const T& val) noexcept {
            seq.fetch_add(1, std::memory_order_acq_rel);
            std::atomic_thread_fence(std::memory_order_release);
            std::memcpy(&storage, &val, sizeof(T));
            std::atomic_thread_fence(std::memory_order_release);
            seq.fetch_add(1, std::memory_order_release);
        }

        class ScopedWrite {
            SeqLock& lock;
        public:
            explicit ScopedWrite(SeqLock& l) : lock(l) {
                lock.seq.fetch_add(1, std::memory_order_acq_rel);
            }
            ~ScopedWrite() {
                lock.seq.fetch_add(1, std::memory_order_release);
            }
            T& Get() noexcept { return lock.storage; }
            ScopedWrite(const ScopedWrite&) = delete;
            ScopedWrite& operator=(const ScopedWrite&) = delete;
        };
    };
}
