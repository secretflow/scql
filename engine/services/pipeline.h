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

#include "arrow/array.h"

#include "engine/core/tensor.h"
#include "engine/core/tensor_batch_reader.h"
#include "engine/core/tensor_constructor.h"
#include "engine/framework/session.h"

#include "api/subgraph.pb.h"

namespace scql::engine {

class TensorSlice;

class PipelineExecutor {
 public:
  PipelineExecutor(const pb::Pipeline& pipeline, Session* session);

  void UpdateTensorTable();

  void FetchOutputTensors();

  size_t GetBatchNum() const { return batch_num_; }

  void Finish();

 private:
  std::vector<TensorPtr> input_tensors_;
  std::vector<std::shared_ptr<TensorWriter>> output_writers_;
  std::vector<std::string> input_tensor_names_;
  std::vector<std::string> output_tensor_names_;
  std::vector<std::shared_ptr<TensorSlice>> tensor_readers_;
  Session* session_;
  bool first_batch_;
  bool batched_;
  size_t batch_num_;
};
}  // namespace scql::engine