/**
 * Copyright (c) 2015 Eugene Lazin <4lazin@gmail.com>
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
 */

#include "queryprocessor.h"
#include "seriesparser.h"
#include "util.h"
#include "datetime.h"

#include <string>
#include <map>
#include <algorithm>
#include <regex>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <bits/unordered_set.h>

namespace Akumuli {

//                          //
//      Series Matcher      //
//                          //

static const SeriesMatcher::StringT EMPTY = std::make_pair(nullptr, 0);

SeriesMatcher::SeriesMatcher(uint64_t starting_id)
    : table(StringTools::create_table(0x1000))
    , series_id(starting_id)
{
    if (starting_id == 0u) {
        AKU_PANIC("Bad series ID");
    }
}

uint64_t SeriesMatcher::add(const char* begin, const char* end) {
    auto id = series_id++;
    StringT pstr = pool.add(begin, end, id);
    auto tup = std::make_tuple(std::get<0>(pstr), std::get<1>(pstr), id);
    table[pstr] = id;
    inv_table[id] = pstr;
    names.push_back(tup);
    return id;
}

void SeriesMatcher::_add(std::string series, uint64_t id) {
    if (series.empty()) {
        return;
    }
    const char* begin = &series[0];
    const char* end = begin + series.size();
    StringT pstr = pool.add(begin, end, id);
    table[pstr] = id;
    inv_table[id] = pstr;
}

uint64_t SeriesMatcher::match(const char* begin, const char* end) {

    int len = end - begin;
    StringT str = std::make_pair(begin, len);

    auto it = table.find(str);
    if (it == table.end()) {
        return 0ul;
    }
    return it->second;
}

SeriesMatcher::StringT SeriesMatcher::id2str(uint64_t tokenid) const {
    auto it = inv_table.find(tokenid);
    if (it == inv_table.end()) {
        return EMPTY;
    }
    return it->second;
}

void SeriesMatcher::pull_new_names(std::vector<SeriesMatcher::SeriesNameT> *buffer) {
    std::swap(names, *buffer);
}

} //namespace
