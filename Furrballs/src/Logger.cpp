#include "Logger.h"

namespace NuAtlas {

    std::string Logger::getCurrentTime() {
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

    std::string Logger::logLevelToString(LogLevel level) {
        switch (level) {
        case LogLevel::Debug:   return "Debug";
        case LogLevel::Info:    return "Info";
        case LogLevel::Warning: return "Warning";
        case LogLevel::Error:   return "Error";
        case LogLevel::Critical:return "Critical";
        default:                return "Unknown";
        }
    }

}
