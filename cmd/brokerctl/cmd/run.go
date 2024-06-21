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

func init() {
	runCmd.Flags().StringVar(&jobConf, "job-conf", `{}`, `job conf in json format, e.g.: '{"session_expire_seconds": 86400}'
	{
		"session_expire_seconds": 86400, // Duration in seconds after which a session expires. If set to 0, falls back to project default setting
		"time_zone": "+02:00", // If not set, would use the default timezone
		"link_recv_timeout_sec": 30, // Duration in seconds after which a link receive operation times out
		"link_throttle_window_size": 100, // Size of the throttle window for link operations
		"link_chunked_send_parallel_size": 10, // Number of parallel chunks for chunked link sends
		"unbalance_psi_ratio_threshold": 80, // Threshold ratio for unbalanced PSI
		"unbalance_psi_larger_party_rows_count_threshold": 1000, // Threshold rows count for unbalanced PSI in larger party
		"psi_curve_type": 1, // Type of curve used in PSI calculations
		"http_max_payload_size": 1048576 // Maximum payload size for HTTP requests
	  }
	`)
}

func runQuery(query string) error {
	response, err := brokerCommand.DoQuery(projectID, query, &pb.DebugOptions{EnablePsiDetailLog: enablePsiDetailLog}, jobConf)
	if err != nil {
		return fmt.Errorf("run query: %w", err)
	}
	fmt.Println("run query succeeded")
	printQueryResult(response.GetResult())
	return nil
}

func printQueryResult(result *pb.QueryResult) {
	for _, warn := range result.GetWarnings() {
		fmt.Printf("Warning : %v\n", warn.Reason)
	}
	if len(result.GetOutColumns()) > 0 {
		fmt.Fprintf(os.Stdout, "[fetch]\n")
		// table view
		table := tablewriter.NewWriter(os.Stdout)
		table.SetAutoWrapText(false)
		table.SetAutoFormatHeaders(false)
		if err := tableview.ConvertToTable(result.GetOutColumns(), table); err != nil {
			log.Fatalf("[fetch]convertToTable with err:%v\n", err)
		}
		fmt.Printf("%v rows in set: (%vs)\n", table.NumLines(), result.GetCostTimeS())
		table.Render()
		return
	} else if result.GetAffectedRows() > 0 {
		fmt.Printf("[fetch] %v rows affected\n", result.GetAffectedRows())
		return
	}
}
