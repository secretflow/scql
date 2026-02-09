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

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <memory>

#include "gflags/gflags.h"
#include "google/protobuf/util/json_util.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

#include "engine/datasource/datasource_adaptor_mgr.h"
#include "engine/datasource/embed_router.h"
#include "engine/datasource/router.h"
#include "engine/framework/party_info.h"
#include "engine/framework/session.h"
#include "engine/operator/all_ops_register.h"
#include "engine/services/run_plan_core.h"

#include "api/core.pb.h"
#include "api/engine.pb.h"
#include "api/interpreter.pb.h"
#include "api/status_code.pb.h"
#include "api/subgraph.pb.h"

namespace py = pybind11;
// using namespace scql::engine;
namespace scql_pb = ::scql::pb;

// Python Engine Runner
class PythonEngineRunner {
 public:
  PythonEngineRunner(const std::string& party_code,
                     const std::string& embed_router_conf) {
    // Initialize gflags with our specific setting (only once)
    static bool gflags_initialized = false;
    if (!gflags_initialized) {
      char argv0[] = "scql_engine";
      char argv1[] = "--enable_restricted_read_path=false";
      char* argv_array[] = {argv0, argv1};
      char** argv = argv_array;
      int argc = sizeof(argv_array) / sizeof(argv_array[0]);

      gflags::ParseCommandLineFlags(&argc, &argv, true);
      gflags_initialized = true;
    }

    // Initialize operator registration
    scql::engine::op::RegisterAllOps();

    party_code_ = party_code;
    ds_router_ = scql::engine::EmbedRouter::FromJsonStr(embed_router_conf);
    ds_mgr_ = std::make_unique<scql::engine::DatasourceAdaptorMgr>();
    // TODO: add more options, such as timezone, allowed spu protocols, etc.
  }

  scql_pb::RunExecutionPlanResponse RunPlan(
      std::string_view serialized_compile_plan,
      std::shared_ptr<yacl::link::Context> link_context) {
    scql_pb::RunExecutionPlanResponse response;
    if (!link_context) {
      response.mutable_status()->set_code(scql_pb::Code::INVALID_ARGUMENT);
      response.mutable_status()->set_message("link_context is null");
      return response;
    }

    // Redirect C++ logs to file
    std::string log_file = "scql_engine.log";
    auto file_sink =
        std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file, true);
    auto engine_logger =
        std::make_shared<spdlog::logger>("engine_logger", file_sink);
    spdlog::set_default_logger(engine_logger);

    scql_pb::CompiledPlan compiled_plan;
    {
      // Parse proto from serialized_compile_plan in json format
      google::protobuf::util::JsonParseOptions options;
      options.ignore_unknown_fields = true;
      auto status = google::protobuf::util::JsonStringToMessage(
          std::string(serialized_compile_plan), &compiled_plan, options);
      if (!status.ok()) {
        response.mutable_status()->set_code(scql_pb::Code::INVALID_ARGUMENT);
        response.mutable_status()->set_message("Failed to parse JSON plan: " +
                                               std::string(status.message()));
        return response;
      }
    }

    try {
      auto request = ConvertPlanToRequest(compiled_plan);
      auto session = CreateSessionFromContext(compiled_plan, link_context);
      session->SetState(scql::engine::SessionState::RUNNING);
      scql::engine::RunPlanCore(request, session.get(), &response);
      session->GetLink()->WaitLinkTaskFinish();
    } catch (const std::exception& e) {
      // Convert exception information to error status in response
      response.mutable_status()->set_code(scql_pb::Code::UNKNOWN_ENGINE_ERROR);
      response.mutable_status()->set_message("Engine execution failed: " +
                                             std::string(e.what()));
    }

    return response;
  }

 private:
  // Directly construct Session using provided Context and basic parameters
  std::unique_ptr<scql::engine::Session> CreateSessionFromContext(
      const scql_pb::CompiledPlan& plan,
      std::shared_ptr<yacl::link::Context> link_context) {
    scql::engine::PartyInfo party_info(party_code_, plan.parties());
    return std::make_unique<scql::engine::Session>(
        party_info, plan.spu_runtime_conf(), ds_router_.get(), ds_mgr_.get(),
        link_context);
  }

  scql_pb::RunExecutionPlanRequest ConvertPlanToRequest(
      const scql_pb::CompiledPlan& compile_plan) {
    scql_pb::RunExecutionPlanRequest request;
    auto iter = compile_plan.sub_graphs().find(party_code_);
    if (iter == compile_plan.sub_graphs().end()) {
      throw std::runtime_error("no subgraph found for party: " + party_code_);
    }
    request.mutable_graph()->CopyFrom(iter->second);
    return request;
  }

 private:
  std::string party_code_;
  std::unique_ptr<scql::engine::Router> ds_router_;
  std::unique_ptr<scql::engine::DatasourceAdaptorMgr> ds_mgr_;
};

// Pybind11 module definition
PYBIND11_MODULE(scql_engine, m) {
  m.doc() = "SCQL Engine Python Bindings";

  // Bind core classes
  py::class_<PythonEngineRunner>(m, "Engine")
      .def(py::init<const std::string&, const std::string&>(),
           "Create SCQL Engine instance with party code and embed_router_conf "
           "string",
           py::arg("party_code"), py::arg("embed_router_conf"))
      .def("run_plan", &PythonEngineRunner::RunPlan,
           "Run execution plan with request and link context",
           py::arg("request"), py::arg("link_context"),
           py::call_guard<py::gil_scoped_release>(),
           py::return_value_policy::copy);

  // Fully bind RunExecutionPlanResponse, Python uses original response object
  // directly
  py::class_<scql_pb::RunExecutionPlanResponse>(m, "RunExecutionPlanResponse")
      .def(py::init<>(), "Create empty response")
      .def("status", &scql_pb::RunExecutionPlanResponse::status,
           "Get execution status", py::return_value_policy::reference)
      .def(
          "out_columns",
          [](const scql_pb::RunExecutionPlanResponse& self) {
            // Return list of output columns
            std::vector<scql_pb::Tensor> columns;
            for (const auto& col : self.out_columns()) {
              columns.push_back(col);
            }
            return columns;
          },
          "Get output columns")
      .def("num_rows_affected",
           &scql_pb::RunExecutionPlanResponse::num_rows_affected,
           "Get number of rows affected")
      .def(
          "is_success",
          [](const scql_pb::RunExecutionPlanResponse& self) {
            return self.status().code() == scql_pb::Code::OK;
          },
          "Check if execution was successful")
      .def(
          "get_error_message",
          [](const scql_pb::RunExecutionPlanResponse& self) {
            if (self.status().code() == scql_pb::Code::OK) {
              return std::string("");
            }
            return self.status().message();
          },
          "Get error message if execution failed")
      .def("__str__",
           [](const scql_pb::RunExecutionPlanResponse& self) {
             return self.DebugString();
           })
      .def("__repr__", [](const scql_pb::RunExecutionPlanResponse& self) {
        return self.DebugString();
      });

  // Bind Status class
  py::class_<scql_pb::Status>(m, "Status")
      .def(py::init<>())
      .def("code", &scql_pb::Status::code)
      .def("message", &scql_pb::Status::message)
      .def("set_code",
           [](scql_pb::Status& self, int code) {
             self.set_code(static_cast<scql_pb::Code>(code));
           })
      .def("set_message", [](scql_pb::Status& self, const std::string& msg) {
        self.set_message(msg);
      });

  // Bind PrimitiveDataType enum
  py::enum_<scql_pb::PrimitiveDataType>(m, "PrimitiveDataType")
      .value("UNDEFINED",
             scql_pb::PrimitiveDataType::PrimitiveDataType_UNDEFINED)
      .value("INT8", scql_pb::PrimitiveDataType::INT8)
      .value("INT16", scql_pb::PrimitiveDataType::INT16)
      .value("INT32", scql_pb::PrimitiveDataType::INT32)
      .value("INT64", scql_pb::PrimitiveDataType::INT64)
      .value("FLOAT32", scql_pb::PrimitiveDataType::FLOAT32)
      .value("FLOAT64", scql_pb::PrimitiveDataType::FLOAT64)
      .value("BOOL", scql_pb::PrimitiveDataType::BOOL)
      .value("STRING", scql_pb::PrimitiveDataType::STRING)
      .value("DATETIME", scql_pb::PrimitiveDataType::DATETIME)
      .value("TIMESTAMP", scql_pb::PrimitiveDataType::TIMESTAMP);

  // Bind TensorShape class
  py::class_<scql_pb::TensorShape>(m, "TensorShape")
      .def(py::init<>())
      .def("dim_size", &scql_pb::TensorShape::dim_size)
      .def(
          "dim",
          [](const scql_pb::TensorShape& self, int index) {
            return self.dim(index);
          },
          py::return_value_policy::reference);

  // Bind TensorShape::Dimension class
  py::class_<scql_pb::TensorShape_Dimension>(m, "TensorShape_Dimension")
      .def("dim_value", &scql_pb::TensorShape_Dimension::dim_value)
      .def("dim_param", &scql_pb::TensorShape_Dimension::dim_param)
      .def("has_dim_value", &scql_pb::TensorShape_Dimension::has_dim_value)
      .def("has_dim_param", &scql_pb::TensorShape_Dimension::has_dim_param);

  // Bind Tensor class for output results - full version supports accessing
  // element data
  py::class_<scql_pb::Tensor>(m, "Tensor")
      .def(py::init<>())
      .def("name", &scql_pb::Tensor::name)
      .def(
          "elem_type",
          [](const scql_pb::Tensor& self) {
            return static_cast<int>(self.elem_type());
          },
          "Get element type as integer")
      .def("shape", &scql_pb::Tensor::shape, py::return_value_policy::reference)
      .def(
          "int32_data",
          [](const scql_pb::Tensor& self) {
            return py::array_t<int32_t>(self.int32_data().size(),
                                        self.int32_data().data());
          },
          "Get int32 data as numpy array")
      .def(
          "int64_data",
          [](const scql_pb::Tensor& self) {
            return py::array_t<int64_t>(self.int64_data().size(),
                                        self.int64_data().data());
          },
          "Get int64 data as numpy array")
      .def(
          "float_data",
          [](const scql_pb::Tensor& self) {
            return py::array_t<float>(self.float_data().size(),
                                      self.float_data().data());
          },
          "Get float32 data as numpy array")
      .def(
          "double_data",
          [](const scql_pb::Tensor& self) {
            return py::array_t<double>(self.double_data().size(),
                                       self.double_data().data());
          },
          "Get float64 data as numpy array")
      .def(
          "bool_data",
          [](const scql_pb::Tensor& self) {
            return py::array_t<bool>(self.bool_data().size(),
                                     self.bool_data().data());
          },
          "Get bool data as numpy array")
      .def(
          "string_data",
          [](const scql_pb::Tensor& self) {
            std::vector<std::string> result;
            result.reserve(self.string_data().size());
            for (const auto& str : self.string_data()) {
              result.push_back(str);
            }
            return result;
          },
          "Get string data as list")
      .def(
          "data_validity",
          [](const scql_pb::Tensor& self) {
            return py::array_t<bool>(self.data_validity().size(),
                                     self.data_validity().data());
          },
          "Get data validity mask as numpy array")
      .def("__str__",
           [](const scql_pb::Tensor& self) { return self.DebugString(); })
      .def("__repr__",
           [](const scql_pb::Tensor& self) { return self.DebugString(); });
}
