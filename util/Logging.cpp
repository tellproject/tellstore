#include "Logging.hpp"

namespace tell {
namespace store {

Logger logger;

LoggerT::~LoggerT() {
    for (auto& fun : config.destructFunctions) {
        fun();
    }
}

} // namespace store
} // namespace tell
