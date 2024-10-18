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

#include <string>
#include <vector>

#include "engine/core/tensor.h"

namespace scql::engine::util {

/// utils for scql tensor protobuf representation

std::string GetStringValue(const pb::Tensor& t);

std::vector<std::string> GetStringValues(const pb::Tensor& t);

uint32_t GetScalarUint32(std::shared_ptr<Tensor> t);

void SetStringValues(pb::Tensor* t, const std::vector<std::string>& values);

int64_t GetInt64Value(const pb::Tensor& t);

void SetInt64Values(pb::Tensor* t, const std::vector<int64_t>& values);

bool GetBooleanValue(const pb::Tensor& t);

void SetBooleanValues(pb::Tensor* t, const std::vector<bool>& values);

pb::TensorStatus GetTensorStatus(const pb::Tensor& t);

/// @returns true if all tensors' status are equal to @param[in] expect_status
bool AreTensorsStatusMatched(const RepeatedPbTensor& tensors,
                             pb::TensorStatus expect_status);

/// @returns true if one of tensors' status is equal to @param[in] expect_status
bool OneOfTensorsStatusMatched(const RepeatedPbTensor& tensors,
                               pb::TensorStatus expect_status);
bool AreTensorsStatusMatchedOneOf(
    const RepeatedPbTensor& tensors,
    const std::vector<pb::TensorStatus>& status_set);
bool IsTensorStatusMatched(const pb::Tensor& t, pb::TensorStatus expect_status);

/// @returns true if all tensor' status are the same and be one of
/// @param[in] status_set.
///
/// Requirements:
/// @param[in] tensors size should >= 1
bool AreTensorsStatusEqualAndOneOf(const RepeatedPbTensor& tensors,
                                   std::vector<pb::TensorStatus> status_set);

void CopyValuesToProto(const std::shared_ptr<Tensor>& from_tensor,
                       pb::Tensor* to_proto);

std::shared_ptr<Tensor> ConvertDateTimeToInt64(
    const std::shared_ptr<arrow::ChunkedArray> from_chunked_arr);

std::shared_ptr<arrow::ChunkedArray> ConvertDateTimeToInt64(
    const std::shared_ptr<arrow::Array> from_arr);

void ConvertDateTimeAndCopyValuesToProto(
    const std::shared_ptr<Tensor>& from_tensor, pb::Tensor* to_proto);
}  // namespace scql::engine::util