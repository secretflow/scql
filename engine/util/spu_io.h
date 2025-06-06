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

#include <utility>

#include "arrow/api.h"
#include "libspu/device/io.h"
#include "libspu/kernel/hlo/basic_binary.h"
#include "libspu/kernel/hlo/geometrical.h"

#include "engine/core/tensor.h"
#include "engine/util/prefix_sum.h"

namespace scql::engine::util {

// expand value from bottom to top for each group, mark using '0' to mark
// the group end, e.g: inputs = {{0, 0, 1, 0, 2}}, mark = {1, 1, 0, 1, 0}
//            output = {{1, 1, 1, 2, 2}}
std::vector<spu::Value> ExpandGroupValueReversely(
    spu::SPUContext* sctx, const std::vector<spu::Value>& inputs,
    const spu::Value& mark);

spu::DataType GetWiderSpuType(const spu::DataType& t1, const spu::DataType& t2);

std::shared_ptr<arrow::Array> ConcatenateChunkedArray(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_arr);

class SpuVarNameEncoder {
 public:
  static std::string GetValueName(const std::string& name);
  static std::string GetValidityName(const std::string& name);
};

/// @brief helper class for infeeding tensor to SPU device
class SpuInfeedHelper {
 public:
  explicit SpuInfeedHelper(spu::device::ColocatedIo* cio) : cio_(cio) {}

  void InfeedTensorAsPublic(const std::string& name, const Tensor& tensor) {
    InfeedTensor(name, tensor, spu::VIS_PUBLIC);
  }

  void InfeedTensorAsSecret(const std::string& name, const Tensor& tensor) {
    InfeedTensor(name, tensor, spu::VIS_SECRET);
  }

  void Sync();

 private:
  /// @brief spu plaintext view of arrow array
  struct PtView {
    spu::PtBufferView value;

#ifdef SCQL_WITH_NULL
    spu::PtBufferView validity;  // null bitmap

    PtView(spu::PtBufferView val, spu::PtBufferView validity)
        : value(val), validity(validity) {}
#else
    explicit PtView(spu::PtBufferView val) : value(std::move(val)) {}
#endif  // SCQL_WITH_NULL

    PtView() = delete;
  };

  /// @brief return PtView will be valid for as long as
  /// @param[in] array is alive.
  PtView ConvertArrowArrayToPtView(const std::shared_ptr<arrow::Array>& array);

  void InfeedTensor(const std::string& name, const Tensor& tensor,
                    spu::Visibility vtype);

 private:
  spu::device::ColocatedIo* cio_;
  // hold a copy of array's shared_ptr to make sure spu::PtBufferView valid
  // until sync() is called.
  std::vector<std::shared_ptr<arrow::Array>> array_refs_;
};

/// @brief helper class for outfeeding tensors from SPU device
class SpuOutfeedHelper {
 public:
  SpuOutfeedHelper(spu::SPUContext* sctx,
                   const spu::device::SymbolTable* symbols)
      : sctx_(sctx), symbols_(symbols) {}

  TensorPtr DumpPublic(const std::string& name);

  /// Reveal a secret value from spu deivce to party `rank`.
  /// @returns nullptr to other parties.
  TensorPtr RevealTo(const std::string& name, size_t rank);

 private:
  spu::SPUContext* sctx_;
  const spu::device::SymbolTable* symbols_;
};

}  // namespace scql::engine::util