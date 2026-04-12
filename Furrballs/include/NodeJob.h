#pragma once
#include "Logger.h"
#include <Numatic.h>
#include <thread>
#include <atomic>
#include <vector>
#include <functional>
#include <future>
#include <condition_variable>

namespace NuAtlas
{

    class NodeJob{
    private:
        static void NO_OP() { return; }

        std::thread Worker;
        inline static thread_local std::atomic<bool> IsRunning{ false };
		inline static thread_local std::atomic<bool> FinishedWork{ true };
		inline static std::atomic<bool> GlobalHasStarted{ false };
        inline static std::function<void()> globalStartFunction = &NodeJob::NO_OP; //Either Protect it, or make a global flag.
		std::condition_variable condition; //since Each Node will operate its queue, This shouldn't be static. Until i add multiple worker per node, Keep this simple

    public:
        NodeJob(){

        }

        static void Loop();
        //This is only usuable to Set a function to be called on any NodeJob Start. This can only be set before any NodeJob starts.
        static void SetGlobalStartupFunction(std::function<void()> func){
            if(GlobalHasStarted.load(std::memory_order_acquire)){
                Logger::getInstance().critical("Attempted to Change Start Up function after the Node Job started.");
                throw std::bad_function_call();
            }
            globalStartFunction = func;
        }

        static void Join();
    };
} // namespace NuAtlas
