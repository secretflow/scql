// Copyright 2025 Ant Group Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "engine/util/string_util.h"

#include <unordered_map>

#include "yacl/base/exception.h"

namespace scql::engine::util {

std::string MySQLDateFormatToArrowFormat(const std::string& mysql_format) {
  static const std::unordered_map<std::string, std::string> kMapping = {
      {"%Y", "%Y"},           // 4-digit year
      {"%y", "%y"},           // 2-digit year
      {"%m", "%m"},           // Month (01-12)
      {"%c", "%-m"},          // Month (1-12)
      {"%d", "%d"},           // Day (01-31)
      {"%e", "%-d"},          // Day (1-31)
      {"%H", "%H"},           // Hour (00-23)
      {"%k", "%-H"},          // Hour (0-23)
      {"%h", "%I"},           // Hour (01-12)
      {"%I", "%I"},           // Hour (01-12)
      {"%l", "%-I"},          // Hour (1-12)
      {"%i", "%M"},           // Minute (00-59)
      {"%S", "%S"},           // Second (00-59)
      {"%s", "%S"},           // Same as %S
      {"%f", "%f"},           // Microsecond
      {"%p", "%p"},           // AM/PM
      {"%T", "%H:%M:%S"},     // 24-hour time
      {"%r", "%I:%M:%S %p"},  // 12-hour time
      {"%b", "%b"},           // Abbreviated month name
      {"%M", "%B"},           // Full month name
      {"%a", "%a"},           // Abbreviated weekday name
      {"%W", "%A"},           // Full weekday name
  };

  std::string result;
  size_t i = 0;
  while (i < mysql_format.size()) {
    if (mysql_format[i] == '%') {
      YACL_ENFORCE(
          i + 1 < mysql_format.size(),
          "MySQLDateFormatToArrowFormat: invalid format string: trailing %%");

      // Special case: %%
      if (mysql_format[i + 1] == '%') {
        result += '%';
        i += 2;
        continue;
      }

      std::string specifier = mysql_format.substr(i, 2);
      auto it = kMapping.find(specifier);
      YACL_ENFORCE(it != kMapping.end(),
                   "MySQLDateFormatToArrowFormat: unsupported MySQL format "
                   "specifier: {}",
                   specifier);

      result += it->second;
      i += 2;
    } else {
      result += mysql_format[i];
      i++;
    }
  }

  return result;
}

}  // namespace scql::engine::util
