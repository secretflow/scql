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

#include "engine/link/listener.h"

#include "engine/link/mux_receiver.pb.h"

namespace scql::engine {

// Multiplexing Receiver Service
class MuxReceiverServiceImpl : public link::pb::MuxReceiverService {
 public:
  explicit MuxReceiverServiceImpl(ListenerManager* listener_manager)
      : listener_manager_(listener_manager) {}

  void Push(::google::protobuf::RpcController* cntl,
            const link::pb::MuxPushRequest* request,
            link::pb::MuxPushResponse* response,
            ::google::protobuf::Closure* done) override;

 private:
  ListenerManager* listener_manager_;
};

}  // namespace scql::engine