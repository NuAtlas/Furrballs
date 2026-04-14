#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>


namespace StreamLine::Locks
{
    /** \addtogroup Barriers
     *  @{
     */

    /// @brief A barrier that spins for a limited count before falling back to a condition variable.
    class HybridBarrier {
    private:
        std::atomic<bool> ready{false};
        std::mutex mtx;
        std::condition_variable cv;
        std::atomic<unsigned int> spinCount{100}; // Default value
    public:
        inline HybridBarrier& SetSpinCount(unsigned int count)noexcept {
            spinCount.store(count, std::memory_order_relaxed);
            return *this;
        }
        void Wait() noexcept {
            const unsigned int currentSpinCount = spinCount.load(std::memory_order_relaxed);
            // Spin phase
            for (unsigned int i = 0; i < currentSpinCount; ++i) {
                if (ready.load(std::memory_order_acquire)) return;
                std::this_thread::yield(); // Give CPU to other threads
            }

            // Fallback to CV
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&] { return ready.load(std::memory_order_acquire); });
        }

        inline void Signal() noexcept {
            std::lock_guard<std::mutex> lock(mtx);
            ready.store(true, std::memory_order_release);
            cv.notify_all(); // Only wakes if someone already in the cv wait
        }

        inline void Reset() noexcept {
            std::lock_guard<std::mutex> lock(mtx);
            ready.store(false, std::memory_order_release);
        }

        /**
         * @brief Informational only.
         */
        inline bool PeekReady() const noexcept {
            return ready.load(std::memory_order_acquire);
        }
    };
    /// @brief A Spin-lock Barrier. Useful for short wait loops. Resettable.
    class SpinBarrier {
    private:
        std::atomic<bool> ready{false};
    public:
        void Wait() noexcept {
            while (!ready.load(std::memory_order_acquire)) {
                std::this_thread::yield(); // Let scheduler threads run
            }
        }

        inline void Signal() noexcept {
            ready.store(true, std::memory_order_release);
        }

        inline void Reset() noexcept{
            ready.store(false, std::memory_order_release);
        }

        /**
         * @brief Informational only.
         */
        inline bool PeekReady() const noexcept {
            return ready.load(std::memory_order_acquire);
        }            
    };

    /// @brief A Barrier with a mutex/cv. Resettable.
    class Barrier {
    private:
        std::mutex mtx;
        std::condition_variable cv;
        std::atomic<bool> ready = false;

    public:
        void Wait() noexcept {
            std::unique_lock<std::mutex> lock(mtx);
            cv.wait(lock, [&]() noexcept { return ready.load(std::memory_order_acquire); });
        }

        inline void Signal() noexcept {
            std::lock_guard<std::mutex> lock(mtx);
            ready.store(true, std::memory_order_release);
            cv.notify_all();
        }

        inline void Reset() noexcept {
            std::lock_guard<std::mutex> lock(mtx);
            ready.store(false, std::memory_order_release);
        }
        
        /**
         * @brief Informational only.
         */
        inline bool PeekReady() const noexcept {
            return ready.load(std::memory_order_acquire);
        }
    };
    ///@}
} // namespace StreamLine::Locks
