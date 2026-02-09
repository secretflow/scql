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

#include "engine/services/run_plan_core.h"

#include <chrono>

#include "engine/framework/exec.h"
#include "engine/framework/executor.h"
#include "engine/framework/session.h"
#include "engine/services/pipeline.h"
#include "engine/util/logging.h"
#include "engine/util/trace_categories.h"

#include "api/status_code.pb.h"

namespace scql::engine {

void RunPlanCore(const pb::RunExecutionPlanRequest& request, Session* session,
                 pb::RunExecutionPlanResponse* response) {
  TRACE_EVENT_DEFAULT_TRACK(RPCCALL_CATEGORY, "RunPlanCore");
  auto logger = ActiveLogger(session);
  const auto& graph = request.graph();
  const auto& policy = graph.policy();
  session->InitProgressStats(policy);
  if (policy.pipelines().size() > 1) {
    session->EnableStreamingBatched();
  }
  for (int pipe_index = 0; pipe_index < policy.pipelines().size();
       pipe_index++) {
    const auto& pipeline = policy.pipelines()[pipe_index];
    PipelineExecutor pipe_executor(pipeline, session);
    session->GetProgressStats()->SetBatchCntInPipeline(
        pipe_executor.GetBatchNum());
    TRACE_EVENT_DEFAULT_TRACK(
        OPERATOR_CATEGORY,
        ::perfetto::DynamicString(fmt::format("Run Pipeline: {}", pipe_index)),
        "total batch num", pipe_executor.GetBatchNum());
    for (size_t i = 0; i < pipe_executor.GetBatchNum(); i++) {
      TRACE_EVENT_DEFAULT_TRACK(
          OPERATOR_CATEGORY,
          ::perfetto::DynamicString(fmt::format("Run Batch: {}", i)));
      SPDLOG_LOGGER_INFO(logger,
                         "session({}) start to execute pipeline({}) batch({})",
                         session->Id(), pipe_index, i);
      pipe_executor.UpdateTensorTable();
      for (const auto& subdag : pipeline.subdags()) {
        for (const auto& job : subdag.jobs()) {
          const auto& node_ids = job.node_ids();
          for (const auto& node_id : node_ids) {
            const auto& iter = graph.nodes().find(node_id);
            YACL_ENFORCE(iter != graph.nodes().cend(),
                         "no node for node_id={} in node_ids", node_id);
            const auto& node = iter->second;
            TRACE_EVENT_DEFAULT_TRACK(OPERATOR_CATEGORY,
                                      ::perfetto::DynamicString(fmt::format(
                                          "Run Node: node({}) op({})",
                                          node.node_name(), node.op_type())));
            SPDLOG_LOGGER_INFO(logger,
                               "session({}) start to execute node({}) op({}) "
                               "pipeline({}) batch({})",
                               session->Id(), node.node_name(), node.op_type(),
                               pipe_index, i);
            auto start = std::chrono::system_clock::now();
            session->GetProgressStats()->SetCurrentNodeInfo(start,
                                                            node.node_name());

            YACL_ENFORCE(session->GetState() == SessionState::RUNNING,
                         "session status not equal to running");
            ExecContext context(node, session);
            Executor executor;
            executor.RunExecNode(&context);

            auto end = std::chrono::system_clock::now();
            SPDLOG_LOGGER_INFO(
                logger,
                "session({}) finished executing node({}), op({}), cost({})ms",
                session->Id(), node.node_name(), node.op_type(),
                std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                      start)
                    .count());
            // TODO(xiaoyuan): fix progress in streaming mode later
            session->GetProgressStats()->IncExecutedNodes();
            if (!session->GetStreamingOptions().batched) {
              YACL_ENFORCE(session->GetProgressStats()->GetExecutedNodes() <=
                               session->GetProgressStats()->GetNodesCnt(),
                           "executed nodes: {}, total nodes count: {}",
                           session->GetProgressStats()->GetExecutedNodes(),
                           session->GetProgressStats()->GetNodesCnt());
            }
            if (node.op_type() == "Publish") {
              auto results = session->GetPublishResults();
              for (const auto& result : results) {
                pb::Tensor* out_column = response->add_out_columns();
                out_column->CopyFrom(*result);
              }
            } else if (node.op_type() == "DumpFile" ||
                       node.op_type() == "InsertTable") {
              auto affected_rows = session->GetAffectedRows();
              response->set_num_rows_affected(affected_rows);
            }
            COMMUNICATION_COUNTER(session->GetLinkStats());
          }
        }
        if (subdag.need_call_barrier_after_jobs()) {
          yacl::link::Barrier(session->GetLink(), session->Id());
        }
      }
      pipe_executor.FetchOutputTensors();
      session->GetProgressStats()->IncExecutedBatch();
      SPDLOG_LOGGER_INFO(
          logger, "session({}) finished executing pipeline({}) batch({})",
          session->Id(), pipe_index, i);
    }
    pipe_executor.Finish();
    session->GetProgressStats()->IncExecutedPipeline();
  }
  SPDLOG_LOGGER_INFO(logger, "session({}) run plan policy succ", session->Id());
  response->mutable_status()->set_code(::scql::pb::Code::OK);
}

}  // namespace scql::engine
