#pragma once

#include <string>
#include <cstdio>

namespace flashshare {

enum class LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR
};

class Logger {
public:
    static void set_level(LogLevel level);
    static LogLevel get_level();

    static void debug(const char* fmt, ...);
    static void info(const char* fmt, ...);
    static void warn(const char* fmt, ...);
    static void error(const char* fmt, ...);

private:
    static LogLevel level_;
    static void log(LogLevel lvl, const char* prefix, const char* fmt, va_list args);
};

#define LOG_DEBUG(...) ::flashshare::Logger::debug(__VA_ARGS__)
#define LOG_INFO(...)  ::flashshare::Logger::info(__VA_ARGS__)
#define LOG_WARN(...)  ::flashshare::Logger::warn(__VA_ARGS__)
#define LOG_ERROR(...) ::flashshare::Logger::error(__VA_ARGS__)

} 
