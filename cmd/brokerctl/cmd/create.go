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

package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

var (
	projectConf string
	columns     string
	refTable    string
	dbType      string
	query       string

	createCmd = &cobra.Command{
		Use:   "create {project|table <name>|job}",
		Short: "Create a new project/table/job",
		Args:  cobra.MatchAll(cobra.MinimumNArgs(1), cobra.MaximumNArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "project":
				return createProject()
			case "table":
				if len(args) != 2 {
					return fmt.Errorf("missing <name> for creating table")
				}
				return createTable(args[1])
			case "job":
				return createJob()
			default:
				return fmt.Errorf("not support create %v", args[0])
			}
		},
	}
)

func init() {
	createCmd.Flags().StringVar(&projectConf, "project-conf", `{"protocol":"SEMI2K", "field": "FM64"}`, "spu conf for project in json format")
	createCmd.Flags().StringVar(&columns, "columns", "", "columns for table, format: 'column_name column_type [, name type]', e.g: 'ID string, age int'")
	createCmd.Flags().StringVar(&refTable, "ref-table", "", "the physical table name corresponding to the new table, e.g: 'test_table'")
	createCmd.Flags().StringVar(&dbType, "db-type", "mysql", "the database type to which the table belongs, e.g: 'mysql'")
	createCmd.Flags().StringVar(&query, "query", "", "the sql query for create job, e.g: 'select count(*) from ta'")
}

func createProject() error {
	newID, err := brokerCommand.CreateProject(projectID, projectConf)
	if err != nil {
		return err
	}
	fmt.Println("create project succeeded")
	if projectID == "" {
		fmt.Printf("project id: %s\n", newID)
	}
	return nil
}

func createTable(name string) error {
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	}
	if refTable == "" {
		return fmt.Errorf("flags ref-table must not be empty")
	}
	var columnDescs []*pb.CreateTableRequest_ColumnDesc
	if columns == "" {
		return fmt.Errorf("flags columns must not be empty")
	} else {
		cols := strings.Split(columns, ",")
		if len(cols) == 0 {
			return fmt.Errorf("flags columns format illegal")
		}
		for _, col := range cols {
			var items []string
			for _, item := range strings.Split(col, " ") {
				if item != "" {
					items = append(items, item)
				}
			}
			if len(items) != 2 {
				return fmt.Errorf("flags columns format illegal: the format of item{%s} is not 'column_name column_type'", col)
			}
			columnDescs = append(columnDescs, &pb.CreateTableRequest_ColumnDesc{
				Name:  items[0],
				Dtype: items[1],
			})
		}
	}
	err := brokerCommand.CreateTable(projectID, name, dbType, refTable, columnDescs)
	if err != nil {
		return err
	}
	fmt.Println("create table succeeded")
	return nil
}

func createJob() error {
	if projectID == "" {
		return fmt.Errorf("flags project-id must not be empty")
	}
	if query == "" {
		return fmt.Errorf("flags query must not be empty")
	}
	jobID, err := brokerCommand.CreateJob(projectID, query)
	if err != nil {
		return err
	}

	pollingCmd := fmt.Sprintf("%s get result --job-id=%s --host=%s", os.Args[0], jobID, host)
	fmt.Printf("create job succeeded, you could poll results via following cmd:\n\t%s\n", pollingCmd)

	return nil
}
