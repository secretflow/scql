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

#include "engine/operator/all_ops_register.h"

#include <mutex>

#include "engine/operator/arithmetic.h"
#include "engine/operator/broadcast_to.h"
#include "engine/operator/bucket.h"
#include "engine/operator/case_when.h"
#include "engine/operator/cast.h"
#include "engine/operator/coalesce.h"
#include "engine/operator/compare.h"
#include "engine/operator/concat.h"
#include "engine/operator/constant.h"
#include "engine/operator/copy.h"
#include "engine/operator/dump_file.h"
#include "engine/operator/filter.h"
#include "engine/operator/filter_by_index.h"
#include "engine/operator/group.h"
#include "engine/operator/group_agg.h"
#include "engine/operator/group_he_sum.h"
#include "engine/operator/if.h"
#include "engine/operator/if_null.h"
#include "engine/operator/in.h"
#include "engine/operator/insert_table.h"
#include "engine/operator/is_null.h"
#include "engine/operator/join.h"
#include "engine/operator/limit.h"
#include "engine/operator/logical.h"
#include "engine/operator/make_private.h"
#include "engine/operator/make_public.h"
#include "engine/operator/make_share.h"
#include "engine/operator/oblivious_group_agg.h"
#include "engine/operator/oblivious_group_mark.h"
#include "engine/operator/publish.h"
#include "engine/operator/reduce.h"
#include "engine/operator/run_sql.h"
#include "engine/operator/shape.h"
#include "engine/operator/shuffle.h"
#include "engine/operator/sort.h"
#include "engine/operator/trigonometric.h"
#include "engine/operator/unique.h"
#include "engine/operator/window.h"

#ifndef ADD_OPERATOR_TO_REGISTRY
#define ADD_OPERATOR_TO_REGISTRY(op_type)                                \
  do {                                                                   \
    GetOpRegistry()->AddOperator(                                        \
        op_type::kOpType, []() { return std::make_unique<op_type>(); }); \
  } while (false)
#endif  // ADD_OPERATOR_TO_REGISTRY

namespace scql::engine::op {

namespace {

void RegisterAllOpsImpl() {
  ADD_OPERATOR_TO_REGISTRY(RunSQL);
  ADD_OPERATOR_TO_REGISTRY(Constant);
  ADD_OPERATOR_TO_REGISTRY(BroadcastTo);

  ADD_OPERATOR_TO_REGISTRY(Publish);
  ADD_OPERATOR_TO_REGISTRY(DumpFile);
  ADD_OPERATOR_TO_REGISTRY(InsertTable);

  ADD_OPERATOR_TO_REGISTRY(Join);
  ADD_OPERATOR_TO_REGISTRY(FilterByIndex);

  ADD_OPERATOR_TO_REGISTRY(In);
  ADD_OPERATOR_TO_REGISTRY(Filter);

  ADD_OPERATOR_TO_REGISTRY(MakeShare);
  ADD_OPERATOR_TO_REGISTRY(MakePrivate);
  ADD_OPERATOR_TO_REGISTRY(MakePublic);

  ADD_OPERATOR_TO_REGISTRY(Add);
  ADD_OPERATOR_TO_REGISTRY(Minus);
  ADD_OPERATOR_TO_REGISTRY(Mul);
  ADD_OPERATOR_TO_REGISTRY(Div);
  ADD_OPERATOR_TO_REGISTRY(IntDiv);
  ADD_OPERATOR_TO_REGISTRY(Mod);

  ADD_OPERATOR_TO_REGISTRY(Equal);
  ADD_OPERATOR_TO_REGISTRY(NotEqual);
  ADD_OPERATOR_TO_REGISTRY(Less);
  ADD_OPERATOR_TO_REGISTRY(LessEqual);
  ADD_OPERATOR_TO_REGISTRY(GreaterEqual);
  ADD_OPERATOR_TO_REGISTRY(Greater);

  ADD_OPERATOR_TO_REGISTRY(Not);
  ADD_OPERATOR_TO_REGISTRY(LogicalAnd);
  ADD_OPERATOR_TO_REGISTRY(LogicalOr);

  ADD_OPERATOR_TO_REGISTRY(Copy);

  // aggregation
  ADD_OPERATOR_TO_REGISTRY(ReduceSum);
  ADD_OPERATOR_TO_REGISTRY(ReduceCount);
  ADD_OPERATOR_TO_REGISTRY(ReduceAvg);
  ADD_OPERATOR_TO_REGISTRY(ReduceMin);
  ADD_OPERATOR_TO_REGISTRY(ReduceMax);

  ADD_OPERATOR_TO_REGISTRY(Shape);
  ADD_OPERATOR_TO_REGISTRY(Limit);
  ADD_OPERATOR_TO_REGISTRY(Unique);

  ADD_OPERATOR_TO_REGISTRY(Sort);
  ADD_OPERATOR_TO_REGISTRY(Shuffle);
  ADD_OPERATOR_TO_REGISTRY(CaseWhen);

  // private groupby
  ADD_OPERATOR_TO_REGISTRY(Group);
  ADD_OPERATOR_TO_REGISTRY(GroupFirstOf);
  ADD_OPERATOR_TO_REGISTRY(GroupCountDistinct);
  ADD_OPERATOR_TO_REGISTRY(GroupCount);
  ADD_OPERATOR_TO_REGISTRY(GroupSum);
  ADD_OPERATOR_TO_REGISTRY(GroupAvg);
  ADD_OPERATOR_TO_REGISTRY(GroupMin);
  ADD_OPERATOR_TO_REGISTRY(GroupMax);
  ADD_OPERATOR_TO_REGISTRY(GroupHeSum);

  // oblivious groupby
  ADD_OPERATOR_TO_REGISTRY(ObliviousGroupMark);
  ADD_OPERATOR_TO_REGISTRY(ObliviousGroupSum);
  ADD_OPERATOR_TO_REGISTRY(ObliviousGroupCount);
  ADD_OPERATOR_TO_REGISTRY(ObliviousGroupAvg);
  ADD_OPERATOR_TO_REGISTRY(ObliviousGroupMax);
  ADD_OPERATOR_TO_REGISTRY(ObliviousGroupMin);

  ADD_OPERATOR_TO_REGISTRY(Concat);
  ADD_OPERATOR_TO_REGISTRY(Cast);
  ADD_OPERATOR_TO_REGISTRY(If);
  ADD_OPERATOR_TO_REGISTRY(IfNull);
  ADD_OPERATOR_TO_REGISTRY(IsNull);
  ADD_OPERATOR_TO_REGISTRY(Coalesce);

  ADD_OPERATOR_TO_REGISTRY(Sine);
  ADD_OPERATOR_TO_REGISTRY(Cosine);
  ADD_OPERATOR_TO_REGISTRY(ACosine);

  ADD_OPERATOR_TO_REGISTRY(Bucket);

  ADD_OPERATOR_TO_REGISTRY(RowNumber);
}

}  // namespace

void RegisterAllOps() {
  static std::once_flag flag;
  std::call_once(flag, RegisterAllOpsImpl);
}

}  // namespace scql::engine::op