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

#include "engine/util/psi_detail_logger.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/strings/escaping.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "gflags/gflags.h"

#include "engine/core/tensor.h"
#include "engine/util/spu_io.h"

// [PSI][MaskSelf][row id: num] XXXXXXXXXX
#define LOG_FORMATTER "[{}][{}][row id: {}] {}"
#define PSI_OP "PSI"

DEFINE_uint64(detail_logger_sample_num, 0,
              "use to control sample number, 0 means print all, default 0.");

namespace scql::engine::util {

size_t GetItemsNumToWrite(size_t start_idx, size_t items_count) {
  size_t sample_num = FLAGS_detail_logger_sample_num;
  if (sample_num == 0) {
    return items_count;
  }
  if (sample_num <= start_idx) {
    return 0;
  }
  return std::min(items_count, sample_num - start_idx);
}

std::string InputStageToString(psi::ecdh::EcdhStage stage) {
  static std::map<psi::ecdh::EcdhStage, std::string> stageMap = {
      {psi::ecdh::EcdhStage::MaskSelf, "HashSelf"},
      {psi::ecdh::EcdhStage::MaskPeer, "RecvPeer"},
      {psi::ecdh::EcdhStage::RecvDualMaskedSelf, "RecvSelfEncrypt"}};

  return stageMap[stage];
}

std::string OutputStageToString(psi::ecdh::EcdhStage stage) {
  static std::map<psi::ecdh::EcdhStage, std::string> stageMap = {
      {psi::ecdh::EcdhStage::MaskSelf, "MaskSelf"},
      {psi::ecdh::EcdhStage::MaskPeer, "MaskPeer"}};

  return stageMap[stage];
}

void EcdhDetailLogger::Log(
    psi::ecdh::EcdhStage stage,
    const std::array<uint8_t, psi::kEccKeySize>& secret_key, size_t start_idx,
    const std::vector<std::string>& input,
    const std::vector<std::string>& output) {
  auto to_write_items_count = GetItemsNumToWrite(start_idx, input.size());
  if (to_write_items_count > 0) {
    std::stringstream ss;
    for (auto item : secret_key) {
      ss << std::hex << std::setw(2) << std::setfill('0')
         << static_cast<int>(item);
    }
    spd_logger_->info(LOG_FORMATTER, PSI_OP, "PrivateKey", 0, ss.str());
    for (size_t i = 0; i < to_write_items_count; i++) {
      spd_logger_->info(LOG_FORMATTER, PSI_OP, InputStageToString(stage),
                        i + start_idx, absl::BytesToHexString(input[i]));
    }
    if (!output.empty() && stage != psi::ecdh::EcdhStage::RecvDualMaskedSelf) {
      to_write_items_count = std::min(to_write_items_count, output.size());
      for (size_t i = 0; i < to_write_items_count; i++) {
        spd_logger_->info(LOG_FORMATTER, PSI_OP, OutputStageToString(stage),
                          i + start_idx, absl::BytesToHexString(output[i]));
      }
    }
  }
}

std::string ScalarToString(std::shared_ptr<arrow::Scalar>& scalar,
                           pb::PrimitiveDataType type) {
  if (!scalar->is_valid) {
    return "null";
  }
  // for string data, ToString returns [
  //   "string"
  // ]
  // we need remove the suffix "\n]" and prefix "[\n  "
  if (type == pb::PrimitiveDataType::STRING) {
    auto str = scalar->ToString();
    return str.substr(4, str.size() - 6);
  }
  return scalar->ToString();
}

void PsiDetailLogger::LogInput(const std::vector<TensorPtr>& inputs) {
  spd_logger_->info("Start, Loading data...");
  auto to_write_items_count = GetItemsNumToWrite(0, inputs[0]->Length());
  if (to_write_items_count > 0) {
    for (size_t i = 0; i < to_write_items_count; i++) {
      std::string to_write;
      for (size_t j = 0; j < inputs.size(); ++j) {
        auto scalar = inputs[j]->ToArrowChunkedArray()->GetScalar(i);
        if (j == 0) {
          to_write = ScalarToString(scalar.ValueOrDie(), inputs[j]->Type());
        } else {
          to_write = fmt::format(
              "{},{}", to_write,
              ScalarToString(scalar.ValueOrDie(), inputs[j]->Type()));
        }
      }
      spd_logger_->info(LOG_FORMATTER, PSI_OP, "Input", i, to_write);
    }
  }
}

void PsiDetailLogger::LogOutput(const TensorPtr& result,
                                const std::vector<TensorPtr>& inputs) {
  auto to_write_items_count = GetItemsNumToWrite(0, result->Length());
  if (to_write_items_count > 0) {
    // for in
    if (inputs.empty()) {
      for (size_t i = 0; i < to_write_items_count; i++) {
        auto scalar = result->ToArrowChunkedArray()->GetScalar(i);
        std::string to_write =
            ScalarToString(scalar.ValueOrDie(), result->Type());
        spd_logger_->info(LOG_FORMATTER, PSI_OP, "Output", i, to_write);
      }
    } else {
      // for join
      std::vector<std::shared_ptr<arrow::ChunkedArray>> outputs;
      for (const auto& input : inputs) {
        auto take_result = arrow::compute::CallFunction(
            "take",
            {input->ToArrowChunkedArray(), result->ToArrowChunkedArray()});
        outputs.emplace_back(take_result.ValueOrDie().chunked_array());
      }
      for (size_t i = 0; i < to_write_items_count; i++) {
        std::string to_write;
        for (size_t j = 0; j < inputs.size(); ++j) {
          auto scalar = outputs[j]->GetScalar(i);
          if (j == 0) {
            to_write = ScalarToString(scalar.ValueOrDie(), inputs[j]->Type());
          } else {
            to_write = fmt::format(
                "{},{}", to_write,
                ScalarToString(scalar.ValueOrDie(), inputs[j]->Type()));
          }
        }
        spd_logger_->info(LOG_FORMATTER, PSI_OP, "Output", i, to_write);
      }
    }
  }
  spd_logger_->info("End");
  spd_logger_->flush();
}

}  // namespace scql::engine::util
