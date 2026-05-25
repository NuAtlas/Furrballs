#pragma once
#include "Logger.h"
#include <Numatic.h>
#include <thread>
#include <atomic>
#include <vector>
#include <deque>
#include <functional>
#include <future>
#include <condition_variable>
#include <chrono>

namespace NuAtlas
{
    class NodeJob
    {
    private:
        static void NO_OP() { return; }

        // --- Queue worker (existing) ---
        std::thread queueWorker_;
        std::deque<std::function<void()>> funcQueue_;
        std::mutex queueMutex_;
        std::condition_variable queueCv_;

        // --- Maintenance worker (new) ---
        std::thread maintWorker_;
        std::function<void(int)> maintenanceFn_;
        std::chrono::microseconds maintInterval_{1000};
        std::mutex maintMutex_;
        std::condition_variable_any maintCv_;
        std::atomic<bool> MaintRunning{false};
        bool maintWakeRequested_{false};

        // --- Shared ---
        std::atomic<bool> IsRunning{false};
        const int NodeId;

        inline static std::atomic<bool> GlobalHasStarted{false};
        inline static std::function<void()> globalStartFunction = &NodeJob::NO_OP;

        void QueueLoop();
        void MaintenanceLoop();

    public:
        NodeJob(int nodeId) : NodeId(nodeId) {}
        ~NodeJob() { if (IsRunning) Stop(); }
        NodeJob(const NodeJob &) = delete;
        NodeJob &operator=(const NodeJob &) = delete;
        NodeJob(NodeJob &&) = delete;
        NodeJob &operator=(NodeJob &&) = delete;

        void Start(std::function<void()> nodeStartup = &NodeJob::NO_OP);
        void StartMaintenance(std::function<void(int)> fn,
                              std::chrono::milliseconds interval = std::chrono::milliseconds(10));
        void Stop();
        void WakeMaintenance();

        static void SetGlobalStartupFunction(std::function<void()> func)
        {
            if (GlobalHasStarted.load(std::memory_order_acquire))
            {
                Logger::getInstance().critical("Attempted to Change Start Up function after the Node Job started.");
                throw std::bad_function_call();
            }
            globalStartFunction = func;
        }

        void Submit(const std::function<void()> &new_job);
    };
} // namespace NuAtlas
