/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */

#pragma once

#include "RowStorePage.hpp"
#include "RowStoreRecord.hpp"
#include "RowStoreScanProcessor.hpp"

namespace tell {
namespace store {

class PageManager;
class Record;

namespace deltamain {

class RowStoreContext {
public:
    using Scan = RowStoreScan;
    using Page = RowStoreMainPage;
    using PageModifier = RowStorePageModifier;

    using MainRecord = RowStoreRecord;
    using ConstMainRecord = ConstRowStoreRecord;

    static const char* implementationName() {
        return "Delta-Main Rewrite (Row Store)";
    }

    RowStoreContext(const PageManager& /* pageManager */, const Record& /* record */) {
    }
};

} // namespace deltamain
} // namespace store
} // namespace tell
