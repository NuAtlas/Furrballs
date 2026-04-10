#pragma once
/*****************************************************************//**
 * \file   Logger.h
 * \brief  Logger for Furrballs.
 * 
 * \author The Sphynx
 * \date   July 2024
 *********************************************************************/
#include <iostream>
#include <fstream>
#include <string>
#include <ctime>
#include <memory>
#include <mutex>

namespace NuAtlas{
    enum class LogLevel {
        Debug,
        Info,
        Warning,
        Error,
        Critical
    };
    /**
     * @brief Basic Logger.
     */
    class Logger {
    private:
        std::mutex logMutex;
        Logger() : currentLogLevel(LogLevel::Info), logOutput(&std::cout) {}

        Logger(const Logger&) = delete;
        Logger& operator=(const Logger&) = delete;
    public:
        static Logger& getInstance() {
            static Logger instance;
            return instance;
        }
        void setLogLevel(LogLevel level) {
            currentLogLevel = level;
        }

        void setLogOutput(std::ostream* output) {
            logOutput = output;
        }

        void log(LogLevel level, const std::string& message) {
            if (level >= currentLogLevel) {
                std::string logMessage = getCurrentTime() + " [" + logLevelToString(level) + "] " + message;
                std::lock_guard<std::mutex> lock(logMutex);
                (*logOutput) << logMessage << std::endl;
            }
        }

        void debug(const std::string& message) { log(LogLevel::Debug, message); }
        void info(const std::string& message) { log(LogLevel::Info, message); }
        void warning(const std::string& message) { log(LogLevel::Warning, message); }
        void error(const std::string& message) { log(LogLevel::Error, message); }
        void critical(const std::string& message) { log(LogLevel::Critical, message); }

    private:
        LogLevel currentLogLevel;
        std::ostream* logOutput;

        std::string getCurrentTime() {
            std::time_t now = std::time(nullptr);
            char buf[20];
#ifdef _WIN32
            struct tm timeInfo;
            if (localtime_s(&timeInfo, &now) == 0) {
                std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &timeInfo);
                return buf;
            }
#else
            struct tm timeInfo;
            if (localtime_r(&now, &timeInfo)) {
                std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &timeInfo);
                return buf;
            }
#endif
            return "Error getting time";
        }

        std::string logLevelToString(LogLevel level) {
            switch (level) {
            case LogLevel::Debug: return "Debug";
            case LogLevel::Info: return "Info";
            case LogLevel::Warning: return "Warning";
            case LogLevel::Error: return "Error";
            case LogLevel::Critical: return "Critical";
            default: return "Unknown";
            }
        }
    };
}
