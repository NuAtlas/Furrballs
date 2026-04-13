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

namespace NuAtlas
{

    class NodeJob
    {
    private:
        static void NO_OP() { return; }

        std::thread Worker;
        std::deque<std::function<void()>> funcQueue;
        std::atomic<bool> IsRunning{false};
        std::atomic<bool> FinishedWork{true};
        std::mutex NodeMutex;

        const int NodeId;

        inline static std::atomic<bool> GlobalHasStarted{false};
        inline static std::function<void()> globalStartFunction = &NodeJob::NO_OP; // Either Protect it, or make a global flag.
        std::condition_variable condition;                                         // since Each Node will operate its queue, This shouldn't be static. Until i add multiple worker per node, Keep this simple

    public:
        // Does not start any thread. Start() is the "entrypoint".
        NodeJob(int nodeId) : NodeId(nodeId)
        {
            // Kept incase i need per-numa worker information at construction.
        }
        ~NodeJob() { if (IsRunning) Stop(); }
        NodeJob(const NodeJob &) = delete;
        NodeJob &operator=(const NodeJob &) = delete;
        NodeJob(NodeJob &&) = delete;
        NodeJob &operator=(NodeJob &&) = delete;

        void Start(std::function<void()> nodeStartup = &NodeJob::NO_OP);
        void Stop();

        void Loop();
        // This is only usuable to Set a function to be called on any NodeJob Start. This can only be set before any NodeJob starts.
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
