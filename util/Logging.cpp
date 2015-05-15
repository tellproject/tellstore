#include "Logging.hpp"

#include <unordered_map>

namespace tell {
namespace store {

namespace {

const std::unordered_map<crossbow::string, tell::store::LogLevel> gLogLevelNames = {
    std::make_pair(crossbow::string("TRACE"), tell::store::LogLevel::TRACE),
    std::make_pair(crossbow::string("DEBUG"), tell::store::LogLevel::DEBUG),
    std::make_pair(crossbow::string("INFO"), tell::store::LogLevel::INFO),
    std::make_pair(crossbow::string("WARN"), tell::store::LogLevel::WARN),
    std::make_pair(crossbow::string("ERROR"), tell::store::LogLevel::ERROR),
    std::make_pair(crossbow::string("FATAL"), tell::store::LogLevel::FATAL)
};

} // anonymous namespace

Logger logger;

LoggerT::~LoggerT() {
    for (auto& fun : config.destructFunctions) {
        fun();
    }
}

LogLevel logLevelFromString(const crossbow::string& s) {
    return gLogLevelNames.at(s);
}

} // namespace store
} // namespace tell
