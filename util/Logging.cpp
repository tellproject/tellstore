#include "Logging.hpp"

namespace tell {
namespace store {
namespace impl {

Logger logger;

LoggerT::~LoggerT() {
    for (auto& fun : config.destructFunctions) {
        fun();
    }
}

} // namespace tell
} // namespace store
} // namespace impl
