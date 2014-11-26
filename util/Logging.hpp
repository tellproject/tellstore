#pragma once

#include <iostream>
#include <functional>
#include <vector>
#include <mutex>
#include <boost/format.hpp>
#include <crossbow/singleton.hpp>
#include <crossbow/string.hpp>

namespace tell {
namespace store {
namespace impl {

enum class LogLevel : unsigned char {
    TRACE = 0,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL
};

template<class... Args>
struct LogFormatter;

template<class Head, class... Tail>
struct LogFormatter<Head, Tail...> {
    LogFormatter<Tail...> base;
    void format(boost::format& f, Head h, Tail... tail) const {
        f % h;
        base.format(f, tail...);
    }
};

template<>
struct LogFormatter<> {
    void format(boost::format&) const {
        return;
    }
};

struct LoggerConfig {
    using DestructFunction = std::function<void()>;
    std::vector<DestructFunction> destructFunctions;
    LogLevel level;
    std::ostream* traceOut = &std::cout;
    std::ostream* debugOut = &std::cout;
    std::ostream* infoOut = &std::cout;
    std::ostream* warnOut = &std::clog;
    std::ostream* errorOut = &std::cerr;
    std::ostream* fatalOut = &std::cerr;
};

class LoggerT {
    std::mutex mTraceMutex;
    std::mutex mDebugMutex;
    std::mutex mInfoMutex;
    std::mutex mWarnMutex;
    std::mutex mErrorMutex;
    std::mutex mFatalMutex;
    template<class...Args>
    void log(
            LogLevel level,
            std::ostream& stream,
            std::mutex& mutex,
            const char* file,
            unsigned line,
            const char* function,
            const crossbow::string& str,
            Args... args)
    {
        if (config.level < level) return;
        boost::format formatter(str.c_str());
        LogFormatter<Args...> fmt;
        fmt.format(formatter, args...);
        std::lock_guard<std::mutex> _(mutex);
        stream << formatter.str();
        stream << "(in " << function << " at " << file << ':' << line << ')' << std::endl;
    }
public:
    LoggerConfig config;
    ~LoggerT();
    template<class...Args>
    void trace(const char* file, unsigned line, const char* function, const crossbow::string& str, Args... args) {
        log(LogLevel::TRACE, *(config.traceOut), mTraceMutex, file, line, function, str, args...);
    }
    template<class...Args>
    void debug(const char* file, unsigned line, const char* function, const crossbow::string& str, Args... args) {
        log(LogLevel::DEBUG, *(config.debugOut), mDebugMutex, file, line, function, str, args...);
    }
    template<class...Args>
    void info(const char* file, unsigned line, const char* function, const crossbow::string& str, Args... args) {
        log(LogLevel::INFO, *(config.infoOut), mInfoMutex, file, line, function, str, args...);
    }
    template<class...Args>
    void warn(const char* file, unsigned line, const char* function, const crossbow::string& str, Args... args) {
        log(LogLevel::WARN , *(config.warnOut), mWarnMutex, file, line, function, str, args...);
    }
    template<class...Args>
    void error(const char* file, unsigned line, const char* function, const crossbow::string& str, Args... args) {
        log(LogLevel::ERROR, *(config.errorOut), mInfoMutex, file, line, function, str, args...);
    }
    template<class...Args>
    void fatal(const char* file, unsigned line, const char* function, const crossbow::string& str, Args... args) {
        log(LogLevel::FATAL, *(config.fatalOut), mInfoMutex, file, line, function, str, args...);
    }
};

using Logger = crossbow::singleton<LoggerT>;

extern Logger logger;

#define LOG_TRACE(...) logger->trace(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOG_DEBUG(...) logger->debug(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOG_INFO(...) logger->info(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOG_WARN(...) logger->warn(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOG_ERROR(...) logger->error(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)
#define LOG_FATAL(...) logger->fatal(__FILE__, __LINE__, __FUNCTION__, __VA_ARGS__)

} // namespace tell
} // namespace store
} // namespace impl
