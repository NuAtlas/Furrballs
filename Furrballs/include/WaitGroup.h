#pragma once
#include <iostream>
#include <vector>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <string>
#include "Concept.h"

namespace StreamLine {

    /** \addtogroup Exceptions
     *  @{
     */

    class WaitGroupOwnershipException : public std::exception {
    public:
        const char* what() const noexcept override {
            return "Wait can only be called by the owner thread";
        }
    };
    class WaitGroupUseAfterWait : public std::exception {
    public:
        const char* what() const noexcept override {
            return "Cannot add work after waiting has begun";
        }
    };

    /**
     * @brief Exception representing multiple exceptions that occurred during the execution of tasks.
     *
     * This exception is thrown when a `WaitGroup` encounters multiple exceptions from its tasks.
     * It aggregates all the exceptions into a single container, allowing the caller to inspect
     * and handle each individual exception.
     *
     * @details
     * - The `AggregatedException` stores a vector of `std::exception_ptr` objects, each representing
     *   an exception thrown by a task.
     * - The `what()` method provides a summary message indicating the number of exceptions.
     * - The `getExceptions()` method allows access to the individual exceptions for detailed handling.
     *
     * Example usage:
     * @code
     * try {
     *     waitGroup.Wait();
     * } catch (const AggregatedException& ex) {
     *     for (const auto& e : ex.getExceptions()) {
     *         try {
     *             if (e) std::rethrow_exception(e);
     *         } catch (const std::exception& innerEx) {
     *             std::cerr << "Task exception: " << innerEx.what() << std::endl;
     *         }
     *     }
     * }
     * @endcode
     */
    class AggregatedException : public std::exception {
    private:
        std::vector<std::exception_ptr> exceptions = std::vector<std::exception_ptr>();
        mutable std::string message_cache = "";
    public:
        AggregatedException() noexcept = default;

        explicit AggregatedException(std::vector<std::exception_ptr>&& exps) noexcept
            : exceptions(std::move(exps))
        {
            message_cache = "Multiple exceptions occurred (count: " +
                std::to_string(exceptions.size()) + ")";
        }

        const std::vector<std::exception_ptr>& getExceptions() const noexcept {
            return exceptions;
        }

        const char* what() const noexcept override {
            return message_cache.c_str();
        }
    };
    /// @}

    //Next Step (opt.): make it on the user to manage exceptions.
    //(AggregatedException with possible another shared structure.)

    /**
     * @brief The WaitGroup is a synchronization construct designed to coordinate the execution of multiple tasks in a multithreaded environment.
     * It allows the owner thread to wait until all tasks have completed.
     * The WaitGroup is strictly a synchronization mechanism and does not manage task execution or scheduling.
     *
     */
    template<Lockable mutex = std::mutex>
    class WaitGroup {
    private:
        mutable std::atomic<unsigned int> count{ 0 };            // Counter for tracking the number of tasks
        std::thread::id owner = std::this_thread::get_id(); // ID of the thread that created the WaitGroup
        mutable mutex mtx;                         // Mutex for coordinating condition variable
        mutable std::condition_variable cv;             // Condition variable for wait signaling
        std::atomic<bool> waiting{ false };     // Flag to prevent multiple waits

        //For reset.
        int snapshot_count = 0;
    public:
        WaitGroup() = default;
        ~WaitGroup() = default;
        WaitGroup(const WaitGroup&) = delete;
        WaitGroup& operator=(const WaitGroup&) = delete;
        WaitGroup(WaitGroup&& wg) = delete;
        WaitGroup& operator=(WaitGroup&& wg) = delete;

        // Increment the counter, No other thread besides the creating thread can Add.
        // If called after Waiting has started WaitGroupUseAfterWait exception will be thrown.
        void Add(int n) {
            if (std::this_thread::get_id() != owner) {
                throw WaitGroupOwnershipException();
            }
            if (waiting) {
                throw WaitGroupUseAfterWait(); //TODO: i can log instead, as this is a trival error, but i don't have a logger now.
            }
            count.fetch_add(n, std::memory_order_relaxed);

            snapshot_count = count.load(std::memory_order_relaxed);
        }

        /**
         * @brief Decrement the counter and notify waiting threads if it reaches zero, Called by working threads.
         *
         * @note This function must under no circumstance fail. 
         * It's marked const to allow enforcement of ownership, by providing the workers a const WaitGroup that can only call Done(). 
         *
         * @param ex if ex is not NULL, the waitgroup is considered not successful.
         */
        void Done()const noexcept {
            if (std::this_thread::get_id() == owner) {
#ifdef DEBUG
                //Reaching this is a API design violation, but yeah, safeguards. safeguards.
                std::cout << "Done is called by the owner, This is not the correct usage of WaitGroup\n";
#endif
                return;
            }
            if (count.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                cv.notify_all();  // Notify waiting threads
            }
        }

        /**
         * @brief Explicitly move a non-waiting WaitGroup to a new owner thread
         *
         * @note This implementation relies on Return Value Optimization (RVO) to
         * avoid invoking the private move constructor when returning the new WaitGroup.
         * Modern C++ compilers should perform this optimization automatically.
         *
         * @param original The original WaitGroup to move from
         * @return WaitGroup A new WaitGroup with transferred state
         * @throws WaitGroupUseAfterWait if the original WaitGroup is already waiting
         */
        static WaitGroup Transfer(WaitGroup& original) {
            // Can't move a WaitGroup that's already waiting
            if (original.waiting.load(std::memory_order_acquire)) {
                throw WaitGroupUseAfterWait();
            }

            WaitGroup new_wg;

            // Transfer ownership to current thread
            new_wg.owner = std::this_thread::get_id();

            std::unique_lock<mutex> lock(original.mtx);

            // Transfer count
            new_wg.count.store(original.count.load(std::memory_order_acquire),
                std::memory_order_release);
            original.count.store(0, std::memory_order_release);

            // Transfer success state

            new_wg.snapshot_count = original.snapshot_count;
            original.snapshot_count = 0;

            //Rely on RVO (or NRVO)
            return new_wg;
        }

        // Wait for the counter to reach zero, this will never throw on the main thread.
        void Wait() {
            if (std::this_thread::get_id() != owner) {
                //Only owner thread can wait (to avoid deadlocks).
                throw WaitGroupOwnershipException();
            }
            bool expected = false;
            if (!waiting.compare_exchange_strong(expected, true)) {
                //This is a recoverable error, a reused WaitGroup can simply return, albeit it can be misleading without an explicit error.
                throw std::runtime_error("WaitGroup instance is one-use only.");
                //TODO: I'm planning an error management system forcing the user to add a call back, this will be handled there.
                //returning the state to waiting and exiting.
                //waiting.store(true, std::memory_order_release);
                //return;
            }
            std::unique_lock<mutex> lock(mtx);
            cv.wait(lock, [&] { return count.load(std::memory_order_acquire) == 0; });
            // Reset the waiting flag after the wait is done, out of precaution.
            //waiting.store(false, std::memory_order_release);
        }
        /**
         * @brief Waits for all the workers to Call Done or until timeout has elapsed.
         * @return true on success, false on timeout
         */
        [[nodiscard]]
        bool WaitFor(std::chrono::milliseconds timeout) {
            if (std::this_thread::get_id() != owner) {
                throw WaitGroupOwnershipException();
            }

            bool expected = false;
            if (!waiting.compare_exchange_strong(expected, true)) {
                throw std::runtime_error("WaitGroup instance is one-use only.");
            }

            std::unique_lock<mutex> lock(mtx);
            bool success = cv.wait_for(lock, timeout, [&] {
                return count.load(std::memory_order_acquire) == 0;
            });

            return success;
        }
        /**
         * @brief Resets the WaitGroup to the count of the last Add.
         * 
         */
        void Reset() {
            if (owner != std::this_thread::get_id()) {
                throw WaitGroupOwnershipException();
            }

            std::unique_lock<mutex> lock(mtx);

            if (count.load(std::memory_order_acquire) != 0 && waiting.load(std::memory_order_acquire) == true) {
                throw std::runtime_error("Cannot reset WaitGroup that hasn't finished waiting.");
            }
            count.store(snapshot_count, std::memory_order_release);
            waiting.store(false, std::memory_order_release);
        }
        /**
         * @brief Get the Count object, Use only for information before waiting.
         * 
         * @note Since this is actively used (decremented concurrently), 
         * it can not reliably return a correct value when the workers are running. 
         */
        inline const unsigned int GetCount()const noexcept {
            return count.load(std::memory_order_relaxed);
        }
        
        /**
         * *@brief Get the value at the last Add, This is informational only and cannot be reliably
         * as the counter isn't concurrent read safe (it is not needed for operation)
         */
        inline const unsigned int GetSnapshotCount() const noexcept {
            return snapshot_count;
        }
    };
}