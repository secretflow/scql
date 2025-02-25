// Copyright 2024 Ant Group Co., Ltd.
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

#include <functional>

#include "libspu/core/context.h"

namespace scql::engine::util {

using ScanFn = std::function<std::pair<spu::Value, spu::Value>(
    const spu::Value&, const spu::Value&, const spu::Value&,
    const spu::Value&)>;

// Reference: "Scape: Scalable Collaborative Analytics System on Private
// Database with Malicious Security", Fig. 13
spu::Value Scan(spu::SPUContext* ctx, const spu::Value& origin_value,
                const spu::Value& origin_group_mask, const ScanFn& scan_fn);

}  // namespace scql::engine::util