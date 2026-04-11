#pragma once
/*****************************************************************//**
 * \file   Logger.h
 * \brief  Logger for Furrballs.
 * 
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/
#include <iostream>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>
#include <atomic>

namespace NuAtlas {
    enum class LogLevel {
        Debug,
        Info,
        Warning,
        Error,
        Critical
    };

    class Logger {
    private:
        std::mutex logMutex;
        std::atomic<LogLevel> currentLogLevel;
        std::shared_ptr<std::ostream> logOutput;

        Logger() : currentLogLevel(LogLevel::Info), logOutput(std::make_shared<std::ostream>(std::cout.rdbuf())) {}

        Logger(const Logger&) = delete;
        Logger& operator=(const Logger&) = delete;

        std::string getCurrentTime();
        std::string logLevelToString(LogLevel level);

    public:
        static Logger& getInstance() {
            static Logger instance;
            return instance;
        }

        void setLogLevel(LogLevel level) {
            currentLogLevel.store(level, std::memory_order_relaxed);
        }

        void setLogOutput(std::shared_ptr<std::ostream> output) {
            std::lock_guard<std::mutex> lock(logMutex);
            logOutput = std::move(output);
        }

        void log(LogLevel level, const std::string& message) {
            if (level < currentLogLevel.load(std::memory_order_relaxed)) {
                return;
            }
            std::lock_guard<std::mutex> lock(logMutex);
            std::string logMessage = getCurrentTime() + " [" + logLevelToString(level) + "] " + message;
            (*logOutput) << logMessage << std::endl;
        }

        void debug(const std::string& message) { log(LogLevel::Debug, message); }
        void info(const std::string& message) { log(LogLevel::Info, message); }
        void warning(const std::string& message) { log(LogLevel::Warning, message); }
        void error(const std::string& message) { log(LogLevel::Error, message); }
        void critical(const std::string& message) { log(LogLevel::Critical, message); }
    };
}
