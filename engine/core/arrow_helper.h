// Copyright 2023 Ant Group Co., Ltd.
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

#pragma once

#include "yacl/base/exception.h"

#ifndef THROW_IF_ARROW_NOT_OK
#define THROW_IF_ARROW_NOT_OK(status)      \
  do {                                     \
    if (!status.ok()) {                    \
      YACL_THROW("{}", status.ToString()); \
    }                                      \
  } while (0)

#endif  // not defined(THROW_IF_ARROW_NOT_OK)

#ifndef ASSIGN_OR_THROW_ARROW_STATUS
#define ASSIGN_OR_THROW_ARROW_STATUS(lhs, rexpr)  \
  do {                                            \
    auto&& _tmp = (rexpr);                        \
    if (!_tmp.ok()) {                             \
      YACL_THROW("{}", _tmp.status().ToString()); \
    }                                             \
    lhs = std::move(_tmp).ValueUnsafe();          \
  } while (0)
#endif  // ASSIGN_OR_THROW_ARROW_STATUS
