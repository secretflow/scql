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

package constant

// HTTP service paths
const (
	HealthPath               = "/health"
	DoQueryPath              = "/intra/query"
	SubmitQueryPath          = "/intra/query/submit"
	FetchResultPath          = "/intra/query/fetch"
	CancelQueryPath          = "/intra/query/cancel"
	ExplainQueryPath         = "/intra/query/explain"
	CreateProjectPath        = "/intra/project/create"
	ListProjectsPath         = "/intra/project/list"
	InviteMemberPath         = "/intra/member/invite"
	ListInvitationsPath      = "/intra/invitation/list"
	ProcessInvitationPath    = "/intra/invitation/process"
	CreateTablePath          = "/intra/table/create"
	ListTablesPath           = "/intra/table/list"
	DropTablePath            = "/intra/table/drop"
	GrantCCLPath             = "/intra/ccl/grant"
	RevokeCCLPath            = "/intra/ccl/revoke"
	ShowCCLPath              = "/intra/ccl/show"
	EngineCallbackPath       = "/intra/cb/engine"
	CheckAndUpdateStatusPath = "/intra/status/check_and_update"

	InviteToProjectPath = "/inter/project/invite"
	ReplyInvitationPath = "/inter/invitation/reply"
	SyncInfoPath        = "/inter/info/sync"
	AskInfoPath         = "/inter/info/ask"
	DistributeQueryPath = "/inter/query/distribute"
	CancelQueryJobPath  = "/inter/query/cancel"
	ExchangeJobInfoPath = "/inter/job/exchange"
)
