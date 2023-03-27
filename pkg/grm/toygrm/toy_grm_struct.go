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

package toygrm

type column struct {
	ColumnName string `json:"column_name"`
	ColumnType string `json:"column_type"`
}

type schema struct {
	RefDbName    string   `json:"ref_db_name"`
	RefTableName string   `json:"ref_table_name"`
	Columns      []column `json:"columns"`
}

type tableSchema struct {
	Tid    string `json:"tid"`
	Schema schema `json:"schema"`
}

type ownership struct {
	Tids  []string `json:"tids"`
	Token string   `json:"token"`
}

type table struct {
	ReadToken    []string      `json:"read_token"`
	Ownerships   []ownership   `json:"ownerships"`
	TableSchemas []tableSchema `json:"table_schema"`
}

type enginesInfo struct {
	Party      string   `json:"party"`
	Url        []string `json:"url"`
	Credential []string `json:"credential"`
}

type engine struct {
	ReadToken   []string      `json:"read_token"`
	EnginesInfo []enginesInfo `json:"engines_info"`
}

type GrmSource struct {
	Engine engine `json:"engine"`
	Table  table  `json:"table"`
}
