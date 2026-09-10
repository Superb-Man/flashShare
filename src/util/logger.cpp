#include "util/logger.h"

#include <cstdarg>
#include <cstdio>
#include <ctime>

namespace flashshare {

LogLevel Logger::level_ = LogLevel::INFO;
FILE* Logger::log_file_ = nullptr;
std::mutex Logger::mutex_;

void Logger::set_level(LogLevel level) {
    level_ = level;
}

LogLevel Logger::get_level() {
    return level_;
}

bool Logger::set_log_file(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (log_file_) {
        fclose(log_file_);
        log_file_ = nullptr;
    }
    log_file_ = fopen(path.c_str(), "a");
    if (!log_file_) {
        fprintf(stderr, "Failed to open log file: %s\n", path.c_str());
        return false;
    }
    return true;
}

void Logger::close_log_file() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (log_file_) {
        fclose(log_file_);
        log_file_ = nullptr;
    }
}

void Logger::log(LogLevel lvl, const char* prefix, const char* fmt, va_list args) {
    if (static_cast<int>(lvl) < static_cast<int>(level_)) {
        return;
    }

    char timebuf[32];
    time_t now = time(nullptr);
    struct tm tm_info;
    localtime_r(&now, &tm_info);
    strftime(timebuf, sizeof(timebuf), "%H:%M:%S", &tm_info);

    std::lock_guard<std::mutex> lock(mutex_);

    va_list args_copy;
    va_copy(args_copy, args);

    fprintf(stderr, "[%s] %s ", timebuf, prefix);
    vfprintf(stderr, fmt, args);
    fprintf(stderr, "\n");

    if (log_file_) {
        fprintf(log_file_, "[%s] %s ", timebuf, prefix);
        vfprintf(log_file_, fmt, args_copy);
        fprintf(log_file_, "\n");
        fflush(log_file_);
    }

    va_end(args_copy);
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