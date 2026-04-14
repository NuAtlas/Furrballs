#include "NodeJob.h"

void NuAtlas::NodeJob::Start(std::function<void()> nodeStartup)
{
    Worker = std::thread([&, nodeStartup](){
        Numatic::PinCurrentThreadToNode(NodeId);
        NodeJob::globalStartFunction();
        nodeStartup();
        this->Loop();
    });
    GlobalHasStarted.store(true, std::memory_order_release);
    IsRunning.store(true, std::memory_order_release);
}

void NuAtlas::NodeJob::Stop()
{
    IsRunning.store(false);
    condition.notify_all();
    Worker.join();
    Logger::getInstance().info("Worker Joined.");
}

void NuAtlas::NodeJob::Loop()
{
    while (IsRunning) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> ul(NodeMutex);
            condition.wait(ul, [&]() {return !funcQueue.empty() || !IsRunning; });
            if (!IsRunning && funcQueue.empty()) break;
            job = funcQueue.front();
            funcQueue.pop_front();
        }
        job();
    }
}

void NuAtlas::NodeJob::Submit(const std::function<void()> &new_job)
{
    {
        std::unique_lock<std::mutex> ul(NodeMutex);
        funcQueue.push_back(new_job);
    }
    condition.notify_one();
}
