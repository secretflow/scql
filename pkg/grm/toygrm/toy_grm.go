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

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"golang.org/x/exp/slices"

	"github.com/secretflow/scql/pkg/grm"
)

var (
	_ grm.Grm = &ToyGrmInfo{}
)

type TableWithToken struct {
	tableSchemas map[string]*grm.TableSchema
	// map[db_name][]string
	readToken       []string
	tableOwnerToken map[string]string
}

type EngineWithToken struct {
	enginesInfo map[string]*grm.EngineInfo
	readToken   []string
}

type ToyGrmInfo struct {
	schema *TableWithToken
	engine *EngineWithToken
}

// get table meta controlled by read token
// request party not used in toygrm
func (g *ToyGrmInfo) GetTableMeta(tid, requestParty, token string) (*grm.TableSchema, error) {
	if exist := slices.Contains(g.schema.readToken, token); !exist {
		return nil, fmt.Errorf("failed to get %s: no auth", tid)
	}
	if table, exist := g.schema.tableSchemas[tid]; exist {
		return table, nil
	}
	return nil, fmt.Errorf("failed to get %s: table not found", tid)
}

func (g *ToyGrmInfo) GetEngines(partyCodes []string, token string) ([]*grm.EngineInfo, error) {
	if exist := slices.Contains(g.engine.readToken, token); !exist {
		return nil, fmt.Errorf("failed to get engines: no auth")
	}
	var result []*grm.EngineInfo
	for i, p := range partyCodes {
		if engineInfo, exist := g.engine.enginesInfo[p]; exist {
			result = append(result, engineInfo)
			continue
		}
		return nil, fmt.Errorf("%s not found", partyCodes[i])
	}
	return result, nil
}

// if token is in list of the owners of table, return true
func (g *ToyGrmInfo) VerifyTableOwnership(tid, token string) (bool, error) {
	if g.schema.tableOwnerToken[tid] == token {
		return true, nil
	}
	return false, nil
}

func New(path string) (res *ToyGrmInfo, err error) {
	jsonFile, err := os.Open(path)
	if err != nil {
		return
	}
	defer jsonFile.Close()
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return
	}
	grmSource := &GrmSource{}
	err = json.Unmarshal(byteValue, grmSource)
	if err != nil {
		return
	}

	res = &ToyGrmInfo{
		schema: &TableWithToken{
			tableSchemas:    make(map[string]*grm.TableSchema),
			readToken:       make([]string, 0),
			tableOwnerToken: make(map[string]string),
		},
		engine: &EngineWithToken{
			enginesInfo: make(map[string]*grm.EngineInfo),
			readToken:   make([]string, 0),
		},
	}

	res.schema.readToken = grmSource.Table.ReadToken
	for _, ownership := range grmSource.Table.Ownerships {
		for _, tid := range ownership.Tids {
			res.schema.tableOwnerToken[tid] = ownership.Token
		}
	}
	for _, tableInfo := range grmSource.Table.TableSchemas {
		schemaDesc := []*grm.ColumnDesc{}
		for _, columnSchema := range tableInfo.Schema.Columns {
			schemaDesc = append(schemaDesc, &grm.ColumnDesc{
				Name: columnSchema.ColumnName,
				Type: columnSchema.ColumnType,
			})
		}
		res.schema.tableSchemas[tableInfo.Tid] = &grm.TableSchema{
			DbName:    tableInfo.Schema.RefDbName,
			TableName: tableInfo.Schema.RefTableName,
			Columns:   schemaDesc,
		}

	}

	res.engine.readToken = grmSource.Engine.ReadToken
	for _, engineInfo := range grmSource.Engine.EnginesInfo {
		if len(engineInfo.Url) != len(engineInfo.Credential) {
			return nil, fmt.Errorf("engine url and credential size mismatch")
		}
		res.engine.enginesInfo[engineInfo.Party] = &grm.EngineInfo{
			Endpoints:  engineInfo.Url,
			Credential: engineInfo.Credential,
		}
	}

	return
}
