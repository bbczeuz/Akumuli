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

#pragma once
#include "akumuli_def.h"
//#include "queryprocessor_framework.h"
#include "stringpool.h"

#include <stdint.h>
#include <map>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <deque>
#include <memory>
#include <mutex>

namespace Akumuli {

/** Namespace class to store all parsing related things.
  */
struct SeriesParser
{
    /** Convert input string to normal form.
      * In normal form metric name is followed by the list of key
      * value pairs in alphabetical order. All keys should be unique and
      * separated from metric name and from each other by exactly one space.
      * @param begin points to the begining of the input string
      * @param end points to the to the end of the string
      * @param out_begin points to the begining of the output buffer (should be not less then input buffer)
      * @param out_end points to the end of the output buffer
      * @param keystr_begin points to the begining of the key string (string with key-value pairs)
      * @return AKU_SUCCESS if everything is OK, error code otherwise
      */
    static aku_Status to_normal_form(const char* begin, const char* end,
                              char* out_begin, char* out_end,
                              const char** keystr_begin, const char **keystr_end);

    typedef StringTools::StringT StringT;

    /** Remove redundant tags from input string. Leave only metric and tags from the list.
      */
    static std::tuple<aku_Status, StringT> filter_tags(StringT const& input, StringTools::SetT const& tags, char* out);
};

}

