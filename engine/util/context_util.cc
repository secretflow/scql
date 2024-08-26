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

#include "engine/util/context_util.h"

#include "engine/util/spu_io.h"
#include "engine/util/tensor_util.h"

namespace scql::engine::util {
TensorPtr GetPrivateOrPublicTensor(scql::engine::ExecContext* ctx,
                                   const pb::Tensor& t) {
  TensorPtr ret;
  if (util::IsTensorStatusMatched(t, pb::TENSORSTATUS_PUBLIC)) {
    // read public tensor from spu device symbol table
    auto spu_io = util::SpuOutfeedHelper(ctx->GetSession()->GetSpuContext(),
                                         ctx->GetSession()->GetDeviceSymbols());
    ret = spu_io.DumpPublic(t.name());

    if (t.elem_type() == pb::PrimitiveDataType::STRING) {
      ret = ctx->GetSession()->HashToString(*ret);
    }
  } else {
    ret = ctx->GetTensorTable()->GetTensor(t.name());
  }
  return ret;
}
}  // namespace scql::engine::util