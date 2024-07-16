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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "msgpack.hpp"
#include "yacl/base/exception.h"

#include "engine/util/spu_io.h"

namespace scql::engine::util {

static constexpr size_t kMsgpackBatchSize = 1000 * 1000;
static constexpr char kCountSuffix[] = "count info";
static constexpr char kBatchSuffix[] = "batch";

template <typename T>
void SendMassiveMsgpack(std::shared_ptr<yacl::link::Context> lctx,
                        const std::string& tag, int64_t peer_rank,
                        const std::vector<T>& data) {
  // send total count and batch count
  size_t batch_count =
      (data.size() + kMsgpackBatchSize - 1) / kMsgpackBatchSize;
  msgpack::sbuffer count_buf;
  msgpack::pack(count_buf, std::make_pair(data.size(), batch_count));
  auto count_tag = fmt::format("{} {}", tag, kCountSuffix);

  lctx->Send(peer_rank,
             yacl::ByteContainerView(count_buf.data(), count_buf.size()),
             count_tag);

  for (size_t batch_idx = 0; batch_idx < batch_count; ++batch_idx) {
    auto start = batch_idx * kMsgpackBatchSize;
    auto end = std::min((batch_idx + 1) * kMsgpackBatchSize, data.size());
    auto sub_vec = std::vector<T>(data.begin() + start, data.begin() + end);
    msgpack::sbuffer buf;
    msgpack::pack(buf, sub_vec);
    auto sub_tag = fmt::format("{} {} {}", tag, kBatchSuffix, batch_idx);
    lctx->Send(peer_rank, yacl::ByteContainerView(buf.data(), buf.size()),
               sub_tag);
  }
}

template <typename T>
std::vector<T> RecvMassiveMsgpack(std::shared_ptr<yacl::link::Context> lctx,
                                  const std::string& tag, int64_t peer_rank) {
  auto count_tag = fmt::format("{} {}", tag, kCountSuffix);
  auto count_buf = lctx->Recv(peer_rank, count_tag);
  auto oh =
      msgpack::unpack(static_cast<char*>(count_buf.data()), count_buf.size());
  std::pair<size_t, size_t> count_info;
  oh.get().convert(count_info);

  const size_t& total_count = count_info.first;
  const size_t& batch_count = count_info.second;
  std::vector<T> ret;
  ret.reserve(total_count);
  for (size_t batch_idx = 0; batch_idx < batch_count; ++batch_idx) {
    auto sub_tag = fmt::format("{} {} {}", tag, kBatchSuffix, batch_idx);
    auto sub_buf = lctx->Recv(peer_rank, sub_tag);
    auto oh =
        msgpack::unpack(static_cast<char*>(sub_buf.data()), sub_buf.size());
    std::vector<T> sub_vec;
    oh.get().convert(sub_vec);
    ret.insert(ret.end(), sub_vec.begin(), sub_vec.end());
  }
  return ret;
}

}  // namespace scql::engine::util
