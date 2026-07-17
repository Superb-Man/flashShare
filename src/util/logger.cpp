#include "util/logger.h"

#include <cstdarg>
#include <cstdio>
#include <ctime>

namespace flashshare {

LogLevel Logger::level_ = LogLevel::INFO;

void Logger::set_level(LogLevel level) {
    level_ = level;
}

LogLevel Logger::get_level() {
    return level_;
}

void Logger::log(LogLevel lvl, const char* prefix, const char* fmt, va_list args) {
    if (static_cast<int>(lvl) < static_cast<int>(level_)) {
        return;
    }

    // Timestamp
    char timebuf[32];
    time_t now = time(nullptr);
    struct tm tm_info;
    localtime_r(&now, &tm_info);
    strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tm_info);

    fprintf(stderr, "[%s] %s ", timebuf, prefix);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");
}

void Logger::debug(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::DEBUG, "DEBUG", fmt, args);
    va_end(args);
}

void Logger::info(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::INFO, " INFO", fmt, args);
    va_end(args);
}

void Logger::warn(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::WARN, " WARN", fmt, args);
    va_end(args);
}

void Logger::error(const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log(LogLevel::ERROR, "ERROR", fmt, args);
    va_end(args);
}

}