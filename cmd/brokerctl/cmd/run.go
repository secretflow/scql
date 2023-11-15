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
	"log"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/util/tableview"
)

var (
	runCmd = &cobra.Command{
		Use:   "run <sql query>",
		Short: "Run a specific sql query job",
		Args:  cobra.MatchAll(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			if projectID == "" {
				return fmt.Errorf("flags project-id must not be empty")
			}
			return runQuery(args[0])
		},
	}
)

func runQuery(query string) error {
	response, err := brokerCommand.DoQuery(projectID, query)
	if err != nil {
		return fmt.Errorf("run query: %v", err)
	}
	fmt.Println("run query succeeded")
	printQueryResult(response)
	return nil
}

func printQueryResult(response *pb.QueryResponse) {
	for _, warn := range response.Warnings {
		fmt.Printf("Warning : %v\n", warn.Reason)
	}
	if len(response.OutColumns) > 0 {
		fmt.Fprintf(os.Stdout, "[fetch]\n")
		// table view
		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoWrapText(false)
		table.SetAutoFormatHeaders(false)
		if err := tableview.ConvertToTable(response.OutColumns, table); err != nil {
			log.Fatalf("[fetch]convertToTable with err:%v\n", err)
		}
		fmt.Printf("%v rows in set: (%vs)\n", table.NumLines(), response.GetCostTimeS())
		table.Render()
		return
	} else if response.AffectedRows > 0 {
		fmt.Printf("[fetch] %v rows affected\n", response.AffectedRows)
		return
	}
}
