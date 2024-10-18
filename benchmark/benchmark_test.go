// Copyright 2024 Ant Group Co., Ltd.
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

package benchmark_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/secretflow/scql/cmd/regtest"
	"github.com/secretflow/scql/cmd/regtest/p2p"
	"github.com/secretflow/scql/pkg/scdb/config"
	"github.com/secretflow/scql/pkg/util/mock"
)

var (
	testConf       *p2p.TestConfig
	projectName    string
	testDataSource regtest.TestDataSource
	spuProtocol    string
	containerNames []string
	outputDir      string
)

type QueryInfo struct {
	Issuer string `json:"issuer"`
	Query  string `json:"query"`
}

type QueryInfos struct {
	Queries []QueryInfo `json:"queries"`
}

func TestMain(m *testing.M) {
	confFile := flag.String("conf", "", "/path/to/conf")
	spuP := flag.String("spu_protocol", "SEMI2K", "spu protocol")
	containerNameStr := flag.String("container_names", "", "container names")
	outputDirStr := flag.String("output_dir", "/tmp", "output dir")
	flag.Parse()
	spuProtocol = *spuP
	outputDir = *outputDirStr
	var err error
	testConf, err = p2p.ReadConf(*confFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	projectName = fmt.Sprintf("bench_%s", testConf.SpuProtocol)
	if len(testConf.BrokerAddrs) == 0 {
		fmt.Println("Skipping testing due to empty BrokerAddrs")
		os.Exit(1)
	}
	containerNames = []string{}
	if *containerNameStr == "" {
		fmt.Println("Skipping testing due to empty containerNameStr")
		os.Exit(1)
	}
	containerNames = strings.Split(*containerNameStr, ",")

	mysqlConf := &config.StorageConf{
		ConnStr:         testConf.MySQLConnStr,
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 120,
		ConnMaxLifetime: 3000,
	}
	start := time.Now()
	maxRetries := 8
	retryDelay := 8 * time.Second
	if err := testDataSource.ConnDB(mysqlConf, maxRetries, retryDelay); err != nil {
		fmt.Printf("connect MySQL(%s) failed\n", testConf.MySQLConnStr)
		panic(err)
	}
	ConnTime := time.Since(start)
	if ConnTime >= retryDelay {
		fmt.Println("Participant may be in initialization, start to validate all participants")
		if err = p2p.ValidateAllParticipants(); err != nil {
			fmt.Println("Validate all participants failed")
			panic(err)
		}
	}
	os.Exit(m.Run())
}

type statsData struct {
	memUsage uint64
	cpuUsage float64
	time     time.Time
}

func WriteToCsv(data []statsData, fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	record := make([]string, len(data))
	for i, d := range data {
		record[i] = fmt.Sprintf("%d,%f,%d\n", d.time.Unix(), d.cpuUsage, d.memUsage)
	}
	writer.Write(record)
}

func GetDockerStats(containerName string, interval time.Duration, closeCh chan bool, waitCh chan bool, dir string) {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}
	prevSysCpuNs := uint64(0)
	prevTotalCpuNs := uint64(0)
	stats, err := cli.ContainerStats(ctx, containerName, true)
	if err != nil {
		panic(err)
	}
	var containerStats container.StatsResponse
	defer stats.Body.Close()
	decoder := json.NewDecoder(stats.Body)

	// create file
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, os.FileMode(0777))
	}
	file, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s.csv", containerName)))
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"timestamp", "cpu_usage", "mem_usage", "running_time_s"})
	startTime := time.Now()
	for {
		select {
		default:
			err = decoder.Decode(&containerStats)
			if err != nil {
				log.Fatalf("Error decoding container stats: %v", err)
			}
			data := statsData{}
			data.time = time.Now()
			data.memUsage = containerStats.MemoryStats.Usage
			data.cpuUsage = float64(containerStats.CPUStats.CPUUsage.TotalUsage-prevTotalCpuNs) /
				float64(containerStats.CPUStats.SystemUsage-prevSysCpuNs) * 100.0 * float64(runtime.NumCPU())
			prevTotalCpuNs = containerStats.CPUStats.CPUUsage.TotalUsage
			prevSysCpuNs = containerStats.CPUStats.SystemUsage
			writer.Write([]string{fmt.Sprintf("%d", data.time.Unix()), fmt.Sprintf("%.2f", data.cpuUsage), fmt.Sprintf("%d", data.memUsage), fmt.Sprintf("%d", data.time.Unix()-startTime.Unix())})
		case <-closeCh:
			waitCh <- true
			return
		}
	}
}

func BenchmarkRunQuery(b *testing.B) {
	r := require.New(b)
	r.NoError(p2p.ClearData(&testDataSource))
	content, err := os.ReadFile("testdata/query.json")
	r.NoError(err)
	queries := &QueryInfos{}
	err = yaml.Unmarshal(content, queries)
	r.NoError(err)
	r.NoError(p2p.GetUrlList(testConf))
	curDir, err := os.Getwd()
	r.NoError(err)
	mock.MockDBPath = filepath.Join(curDir, "testdata/db.json")
	mockTables, err := mock.MockAllTables()
	r.NoError(err)
	regtest.FillTableToPartyCodeMap(mockTables)
	cclList, err := mock.MockAllCCL()
	r.NoError(err)
	if !testConf.SkipCreateTableCCL {
		r.NoError(p2p.CreateProjectTableAndCcl(testConf.ProjectConf, cclList, testConf.SkipCreateTableCCL))
	}
	closeCh := make(chan bool, 1)
	outputCh := make(chan bool, len(containerNames))
	for _, name := range containerNames {
		go GetDockerStats(name, 10*time.Second, closeCh, outputCh, outputDir)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		fmt.Println("run query")
		for _, query := range queries.Queries {
			_, err = p2p.RunSql(query.Issuer, query.Query, "{}", p2p.NewFetchConf(100000, time.Second))
			r.NoError(err)
		}
	}
	close(closeCh)
	//wait for write csv
	for range containerNames {
		<-outputCh
	}
}
