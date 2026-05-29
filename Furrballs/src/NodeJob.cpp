#include "NodeJob.h"

void NuAtlas::NodeJob::Start(std::function<void()> nodeStartup)
{
    queueWorker_ = std::thread([&, nodeStartup](){
        Numatic::PinCurrentThreadToNode(NodeId);
        NodeJob::globalStartFunction();
        nodeStartup();
        this->QueueLoop();
    });
    GlobalHasStarted.store(true, std::memory_order_release);
    IsRunning.store(true, std::memory_order_release);
}

void NuAtlas::NodeJob::StartMaintenance(std::function<void(int)> fn,
                                        std::chrono::milliseconds interval)
{
    bool expected = false;
    if (!MaintRunning.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        return;
    }
    maintenanceFn_ = std::move(fn);
    maintInterval_ = interval;
    maintWorker_ = std::thread([this](){
        Numatic::PinCurrentThreadToNode(NodeId);
        NodeJob::globalStartFunction();
        this->MaintenanceLoop();
    });
    Logger::getInstance().info("Maintenance worker started on node " + std::to_string(NodeId));
}

void NuAtlas::NodeJob::Stop()
{
    IsRunning.store(false);
    queueCv_.notify_all();
    if (queueWorker_.joinable()) queueWorker_.join();
    if (MaintRunning.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lk(maintMutex_);
            maintWakeRequested_ = true;
        }
        maintCv_.notify_one();
        if (maintWorker_.joinable()) maintWorker_.join();
    }
    Logger::getInstance().info("Worker Joined.");
}

void NuAtlas::NodeJob::WakeMaintenance()
{
    {
        std::unique_lock<std::mutex> lk(maintMutex_);
        maintWakeRequested_ = true;
    }
    maintCv_.notify_one();
}

void NuAtlas::NodeJob::QueueLoop()
{
    while (IsRunning.load(std::memory_order_relaxed)) {
        std::function<void()> job;
        {
            std::unique_lock<std::mutex> ul(queueMutex_);
            queueCv_.wait(ul, [&]() {
                return !funcQueue_.empty() || !IsRunning.load(std::memory_order_relaxed);
            });
            if (!IsRunning.load(std::memory_order_relaxed) && funcQueue_.empty()) break;
            if (funcQueue_.empty()) continue;
            job = funcQueue_.front();
            funcQueue_.pop_front();
        }
        job();
    }
}

void NuAtlas::NodeJob::MaintenanceLoop()
{
    while (IsRunning.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lk(maintMutex_);
            maintCv_.wait_for(lk, maintInterval_, [this] {
                return maintWakeRequested_ || !IsRunning.load(std::memory_order_relaxed);
            });
            if (!IsRunning.load(std::memory_order_relaxed)) break;
            maintWakeRequested_ = false;
        }
        if (maintenanceFn_) {
            maintenanceFn_(NodeId);
        }
    }
}

void NuAtlas::NodeJob::Submit(const std::function<void()> &new_job)
{
    {
        std::unique_lock<std::mutex> ul(queueMutex_);
        funcQueue_.push_back(new_job);
    }
    queueCv_.notify_one();
}
