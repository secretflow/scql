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

#include "engine/link/mux_receiver_service.h"

#include "brpc/closure_guard.h"
#include "spdlog/spdlog.h"

namespace scql::engine {

void MuxReceiverServiceImpl::Push(::google::protobuf::RpcController* cntl,
                                  const link::pb::MuxPushRequest* request,
                                  link::pb::MuxPushResponse* response,
                                  ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  try {
    // get listener from listener_manager_.
    const std::string& link_id = request->link_id();
    const auto& msg = request->msg();
    const size_t sender_rank = msg.sender_rank();
    const auto& listener = listener_manager_->GetListener(link_id);
    if (!listener) {
      response->set_error_code(link::pb::ErrorCode::LINKID_NOT_FOUND);
      response->set_error_msg(
          fmt::format("no exist Listener for link_id={}", link_id));
      return;
    }
    listener->OnRequest(sender_rank, request, response);
    return;
  } catch (const std::exception& e) {
    response->set_error_code(link::pb::ErrorCode::UNEXPECTED_ERROR);
    response->set_error_msg(fmt::format("dispatch error, link_id={}, error={}",
                                        request->link_id(), e.what()));
    return;
  }
}

}  // namespace scql::engine
