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

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/influxdata/go-prompt"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/secretflow/scql/pkg/proto-gen/scql"
	"github.com/secretflow/scql/pkg/scdb/client"
	"github.com/secretflow/scql/pkg/util/tableview"
)

var (
	// for all cmd
	host                       string
	userName                   string
	passwd                     string
	usersConfFileName          string
	sync                       bool
	pollingTimes               int
	pollingTimeIntervalSeconds int

	// for source
	sourceFile string

	version = "scql version"
)

type UserCredential struct {
	UserName string
	Password string
}

var (
	curDBName   string
	curUser     UserCredential
	userConfMap map[string]UserCredential
)

var rootCmd = &cobra.Command{
	Use:       "scdbclient",
	Short:     "terminal client for scdb",
	Long:      "A terminal client to work with scdb",
	ValidArgs: []string{"prompt", "source", "help"},
	Args:      cobra.OnlyValidArgs,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return setupUserConfig()
	},
	Version: version,
}

var sourceCmd = &cobra.Command{
	Use:   "source",
	Short: "execute SCQL statements in a script file",
	Run: func(cmd *cobra.Command, args []string) {
		runSourceMode()
	},
}

var promptCmd = &cobra.Command{
	Use:   "prompt",
	Short: "command prompt lets you issue query to scdb server in interactive mode",
	Run: func(cmd *cobra.Command, args []string) {
		runPromptMode()
	},
}

func initFlags() {
	rootCmd.PersistentFlags().StringVar(&host, "host", "http://localhost:8080", "scdb server host")
	rootCmd.PersistentFlags().StringVar(&userName, "userName", "", "user name for scdb user")
	rootCmd.PersistentFlags().StringVar(&passwd, "passwd", "", "user passwd for scdb user")
	rootCmd.PersistentFlags().StringVar(&usersConfFileName, "usersConfFileName", "cmd/scdbclient/users.json", "user conf file name")

	rootCmd.PersistentFlags().BoolVar(&sync, "sync", false, "query in sync or async mode")
	rootCmd.PersistentFlags().IntVar(&pollingTimes, "pollingTimes", 0, "polling times, if pollingTimes <= 0, polling until the result is obtained and exit")
	rootCmd.PersistentFlags().IntVar(&pollingTimeIntervalSeconds, "pollingTimeIntervalSeconds", 1, "polling times interval in seconds")

	sourceCmd.PersistentFlags().StringVar(&sourceFile, "sourceFile", "", "scql script file")

	rootCmd.AddCommand(sourceCmd)
	rootCmd.AddCommand(promptCmd)
}

func setupUserConfig() error {
	if usersConfFileName != "" {
		jsonFile, err := os.Open(usersConfFileName)
		if err != nil {
			log.Fatalf("read input fail: %v", usersConfFileName)
		}
		defer func() {
			err = jsonFile.Close()
		}()
		byteValue, err := io.ReadAll(jsonFile)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(byteValue, &userConfMap); err != nil {
			return err
		}
	} else if userName != "" {
		userConfMap[userName] = UserCredential{
			UserName: userName,
			Password: passwd,
		}
	}
	if userName != "" {
		if err := switchUser(userName); err != nil {
			log.Fatalf("fail to switch to user %s err: %v", userName, err)
		}
	}
	return nil
}

func switchUser(userName string) error {
	if _, exist := userConfMap[userName]; !exist {
		return fmt.Errorf("user %s doesn't exist", userName)
	}
	curUser = userConfMap[userName]
	return nil
}

func executor(sql string) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimSuffix(sql, ";")
	lowerSQL := strings.ToLower(sql)
	if sql == "" || strings.HasPrefix(sql, "#") || strings.HasPrefix(sql, "-") || strings.HasPrefix(sql, "/") {
		return
	}
	if sql == "quit" || sql == "exit" {
		fmt.Fprintln(os.Stdout, "Bye ^_^")
		os.Exit(0)
	}
	if strings.HasPrefix(lowerSQL, "switch") {
		items := strings.Split(lowerSQL, " ")
		if len(items) != 2 {
			fmt.Fprintf(os.Stderr, "expect `switch $user`, but got: %v", lowerSQL)
			return
		}
		userName := items[1]
		if err := switchUser(userName); err != nil {
			fmt.Fprintf(os.Stderr, "switch failed %v\n", err)
		}
		return
	}
	if strings.HasPrefix(lowerSQL, "use") {
		items := strings.Split(lowerSQL, " ")
		if len(items) != 2 {
			fmt.Fprintf(os.Stderr, "expect `use $dbName`, but got: %v", lowerSQL)
			return
		}
		curDBName = items[1]
		return
	}
	runSql(curDBName, sql, &curUser, sync)
}

func completer(d prompt.Document) []prompt.Suggest {
	return []prompt.Suggest{}
}

func livePrefix() (string, bool) {
	if len(curDBName) > 0 {
		return fmt.Sprintf("[%s]%s> ", curDBName, curUser.UserName), true
	}
	return fmt.Sprintf("%s> ", curUser.UserName), true
}

func runSourceMode() {
	if sourceFile == "" {
		log.Fatalf("miss sourceFile parameters")
	}
	fi, err := os.Open(sourceFile)
	if err != nil {
		log.Fatalf("read input fail: %v", sourceFile)
	}
	defer fi.Close()
	bs := bufio.NewScanner(fi)
	for {
		if !bs.Scan() {
			break
		}
		sql := bs.Text()
		log.Println(sql)
		sql = strings.TrimSpace(sql)
		sql = strings.TrimSuffix(sql, ";")
		lowerSQL := strings.ToLower(sql)
		if sql == "" || strings.HasPrefix(sql, "#") || strings.HasPrefix(sql, "-") || strings.HasPrefix(sql, "/") {
			continue
		}
		if strings.HasPrefix(lowerSQL, "switch") {
			items := strings.Split(lowerSQL, " ")
			if len(items) != 2 {
				log.Fatalf("expect `switch $user`, but got: %v", lowerSQL)
			}
			userName := items[1]
			if err := switchUser(userName); err != nil {
				log.Fatalf("switch failed: %v\n", err)
			}
			continue
		}
		if err := runSql("", sql, &curUser, sync); err != nil {
			log.Fatalf("run sql err: %v", err)
		}
	}
}

func runPromptMode() {
	p := prompt.New(executor, completer,
		prompt.OptionPrefix("> "),
		prompt.OptionLivePrefix(livePrefix),
		prompt.OptionPrefixTextColor(prompt.Yellow),
	)

	//p.Run() will not support multiple-line input,
	//Refer to: https://github.com/c-bata/go-prompt/issues/25
	for {
		inputStr := p.Input()
		for _, sql := range strings.Split(inputStr, ";") {
			executor(sql)
		}
	}
}

func newUserCredential(user *UserCredential) *scql.SCDBCredential {
	return &scql.SCDBCredential{
		User: &scql.User{
			AccountSystemType: scql.User_NATIVE_USER,
			User: &scql.User_NativeUser_{
				NativeUser: &scql.User_NativeUser{
					Name:     user.UserName,
					Password: user.Password,
				},
			},
		},
	}
}

func runSql(dbName, sql string, userAuth *UserCredential, sync bool) (err error) {
	user := newUserCredential(userAuth)
	startTime := time.Now()

	httpClient := &http.Client{Timeout: 1800 * time.Second}

	stub := client.NewClient(host, httpClient, pollingTimes, time.Duration(pollingTimeIntervalSeconds)*time.Second)
	if len(dbName) > 0 {
		stub.SetDBName(dbName)
	}

	var result *scql.SCDBQueryResultResponse
	if sync {
		result, err = stub.SubmitAndGet(user, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[submit and get]err: %v\n", err)
			return err
		}
	} else {
		submitResponse, err := stub.Submit(user, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[submit]err: %v\n", err)
			return err
		}
		if submitResponse.ScdbSessionId == "" {
			fmt.Fprintf(os.Stderr, "[submit]errorCode: %v, msg: %v\n", submitResponse.Status.Code, submitResponse.Status.Message)
			return fmt.Errorf("[submit]errorCode: %v, msg: %v", submitResponse.Status.Code, submitResponse.Status.Message)
		}
		result, err = fetchResult(user, submitResponse.ScdbSessionId, pollingTimes, pollingTimeIntervalSeconds, stub)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[fetch]err: %v\n", err)
			return err
		}
	}

	if result.GetStatus().GetCode() == int32(scql.Code_OK) {
		for _, warn := range result.Warnings {
			fmt.Printf("Warning : %v\n", warn.Reason)
		}
		if len(result.OutColumns) > 0 {
			fmt.Fprintf(os.Stdout, "[fetch]\n")
			// table view
			table := tablewriter.NewWriter(os.Stdout)
			table.SetAutoWrapText(false)
			table.SetAutoFormatHeaders(false)
			if err := tableview.ConvertToTable(result.OutColumns, table); err != nil {
				log.Fatalf("[fetch]convertToTable with err:%v\n", err)
			}
			fmt.Printf("%v rows in set: (%v)\n", table.NumLines(), time.Since(startTime))
			table.Render()
			return nil
		} else if result.AffectedRows > 0 {
			fmt.Printf("[fetch] %v rows affected\n", result.AffectedRows)
			return nil
		}
		fmt.Fprintf(os.Stdout, "[fetch] OK for DDL/DCL\n")
	} else {
		fmt.Fprintf(os.Stdout, "[fetch]err: Code: %v, message:%v\n", result.GetStatus().GetCode(), result.GetStatus().GetMessage())
	}

	return nil
}

func fetchResult(user *scql.SCDBCredential, sessionId string, maxFetchNum int, fetchInterval int, c *client.Client) (*scql.SCDBQueryResultResponse, error) {
	count := 0
	for {

		if maxFetchNum > 0 && count > maxFetchNum {
			return nil, fmt.Errorf("exceed max number of fetch number %v", maxFetchNum)
		}
		count += 1

		<-time.After(time.Duration(pollingTimeIntervalSeconds) * time.Second)

		response, err := c.FetchOnce(user, sessionId)
		if err != nil {
			return nil, err
		}
		if response.GetStatus().GetCode() == int32(scql.Code_NOT_READY) {
			continue
		}
		return response, nil
	}
}

func main() {
	initFlags()
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalf("Failed to execute cobra.Command: %v", err)
	}
}
