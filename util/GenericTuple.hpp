#pragma once
#include <unordered_map>
#include <boost/any.hpp>
#include <crossbow/string.hpp>


namespace tell {
namespace store {

using GenericTuple = std::unordered_map<crossbow::string, boost::any>;

} // namespace store
} // namespace tell

