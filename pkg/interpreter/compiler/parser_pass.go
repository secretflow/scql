// Copyright 2026 Ant Group Co., Ltd.
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

package compiler

import (
	"fmt"
	"sort"
	"strings"

	"github.com/secretflow/scql/pkg/constant"
	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/parser/ast"
	"github.com/secretflow/scql/pkg/parser/model"
	"github.com/secretflow/scql/pkg/planner/core"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/sessionctx/variable"
	"github.com/secretflow/scql/pkg/types"
)

// ParserPass handles SQL parsing and initial setup
type ParserPass struct{}

// NewParserPass creates a new parser pass
func NewParserPass() *ParserPass {
	return &ParserPass{}
}

// Name returns the pass name
func (p *ParserPass) Name() string {
	return "ParserPass"
}

// Run builds InfoSchema, parses SQL, and processes variables
func (p *ParserPass) Run(c *CompileContext) error {
	// 1. Build InfoSchema from catalog
	is, err := BuildInfoSchemaFromCatalogProto(c.Request.GetCatalog())
	if err != nil {
		return fmt.Errorf("failed to build info schema: %v", err)
	}
	c.InfoSchema = is

	// 2. Parse SQL
	parser := parser.New()
	stmts, _, err := parser.Parse(c.Request.GetQuery(), "", "")
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %v", err)
	}
	if len(stmts) == 0 {
		return fmt.Errorf("no statement found in SQL")
	}
	if len(stmts) > 1 {
		return fmt.Errorf("only support one query one time, but got %d queries", len(stmts))
	}

	// 3. Process variables and placeholders
	node, err := processVariablesAndPlaceholders(stmts[0], c.Request.GetVariables(), c.Request.GetPlaceholders())
	if err != nil {
		return fmt.Errorf("failed to process variables and placeholders: %v", err)
	}

	// Type assertion to convert ast.Node to ast.StmtNode
	stmt, ok := node.(ast.StmtNode)
	if !ok {
		return fmt.Errorf("processed node is not a statement")
	}

	// Store result in context
	c.AST = stmt

	return nil
}

func createDatumFromVariable(dataType string, variable *pb.Variable) (*types.Datum, error) {
	if variable == nil {
		return nil, fmt.Errorf("variable is nil")
	}
	// TODO: @xiaoyuan support bool type
	switch {
	case constant.StringTypeAlias[dataType]:
		d := types.NewStringDatum(variable.StringData)
		return &d, nil
	case constant.IntegerTypeAlias[dataType]:
		d := types.NewIntDatum(variable.Int64Data)
		return &d, nil
	case constant.FloatTypeAlias[dataType], constant.DoubleTypeAlias[dataType]:
		d := types.NewFloat32Datum(variable.FloatData)
		return &d, nil
	default:
		return nil, fmt.Errorf("unknown type in schema: %s", dataType)
	}
}

// processVariablesAndPlaceholders processes variables and placeholders in SQL statement
func processVariablesAndPlaceholders(stmt ast.Node, variables []*pb.Variable, placeholders *pb.Placeholders) (ast.Node, error) {
	if variables == nil {
		return stmt, nil
	}

	if len(variables) != len(placeholders.Placeholders) {
		return nil, fmt.Errorf("processVariablesAndPlaceholders: variables {%+v} in request are not related to placeholders {%+v}", variables, placeholders.Placeholders)
	}

	preparedParams := variable.PreparedParams{}
	placeholderMap := make(map[string]*pb.Placeholder, len(placeholders.Placeholders))
	for _, v := range placeholders.Placeholders {
		placeholderMap[v.Name] = v
	}
	if len(placeholderMap) != len(placeholders.Placeholders) {
		return nil, fmt.Errorf("processVariablesAndPlaceholders: duplicate placeholder name in placeholders {%+v}", placeholders.Placeholders)
	}

	for _, v := range variables {
		if placeholder, ok := placeholderMap[v.Name]; ok {
			d, err := createDatumFromVariable(placeholder.DataType, v)
			if err != nil {
				return nil, err
			}
			preparedParams[v.Name] = d
		} else {
			return nil, fmt.Errorf("processVariablesAndPlaceholders: missing variable for placeholder %s", v.Name)
		}
	}

	paramVisitor := core.NewPreparedParamsVisitor(&preparedParams)
	processedStmt, ok := stmt.Accept(paramVisitor)
	if !ok || paramVisitor.Error() != nil {
		return nil, fmt.Errorf("failed to accept prepared params visitor: %s", paramVisitor.Error())
	}

	return processedStmt, nil
}

// BuildInfoSchemaFromCatalogProto builds an InfoSchema from catalog proto
func BuildInfoSchemaFromCatalogProto(catalog *pb.Catalog) (infoschema.InfoSchema, error) {
	tblInfoMap := make(map[string][]*model.TableInfo)
	for i, tblEntry := range catalog.GetTables() {
		dbTable, err := core.NewDbTableFromString(tblEntry.GetTableName())
		if err != nil {
			return nil, err
		}
		tblInfo := &model.TableInfo{
			ID:          int64(i),
			TableId:     fmt.Sprint(i),
			Name:        model.NewCIStr(dbTable.GetTableName()),
			Columns:     []*model.ColumnInfo{},
			Indices:     []*model.IndexInfo{},
			ForeignKeys: []*model.FKInfo{},
			State:       model.StatePublic,
			PKIsHandle:  false,
		}

		if tblEntry.Owner != nil {
			tblInfo.PartyCode = tblEntry.Owner.Code
		}

		if tblEntry.GetIsView() {
			tblInfo.View = &model.ViewInfo{
				Algorithm:  model.AlgorithmMerge,
				SelectStmt: tblEntry.SelectString,
			}
		}
		// sort columns by ordinal position
		sort.Slice(tblEntry.Columns, func(i, j int) bool {
			return tblEntry.Columns[i].OrdinalPosition < tblEntry.Columns[j].OrdinalPosition
		})

		for idx, col := range tblEntry.GetColumns() {
			colTp := strings.ToLower(col.GetType())
			defaultVal, err := infoschema.TypeDefaultValue(colTp)
			if err != nil {
				return nil, err
			}
			fieldTp, err := infoschema.TypeConversion(colTp)
			if err != nil {
				return nil, err
			}
			colInfo := &model.ColumnInfo{
				ID:                 int64(idx),
				Name:               model.NewCIStr(col.GetName()),
				Offset:             idx,
				OriginDefaultValue: defaultVal,
				DefaultValue:       defaultVal,
				DefaultValueBit:    []byte{},
				Dependences:        map[string]struct{}{},
				FieldType:          fieldTp,
				State:              model.StatePublic,
			}
			tblInfo.Columns = append(tblInfo.Columns, colInfo)
		}
		tblInfoMap[dbTable.GetDbName()] = append(tblInfoMap[dbTable.GetDbName()], tblInfo)
	}
	return infoschema.MockInfoSchema(tblInfoMap), nil
}
