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

package translator

import (
	"flag"
	"fmt"
	"os"
	"testing"

	. "github.com/pingcap/check"

	"github.com/secretflow/scql/pkg/infoschema"
	"github.com/secretflow/scql/pkg/interpreter/graph"
	"github.com/secretflow/scql/pkg/parser"
	"github.com/secretflow/scql/pkg/sessionctx"
	"github.com/secretflow/scql/pkg/util/mock"
)

var _ = Suite(&testTranslatorSuite{})

var recordTestOutput bool

func init() {
	flag.BoolVar(&recordTestOutput, "recordTestOutput", false, "used for output dot for translator test and ignore dot check")
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testTranslatorSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context

	engineInfo *graph.EnginesInfo

	issuerParty string

	recordFile *os.File
}

func (s *testTranslatorSuite) SetUpSuite(c *C) {
	mockTables, err := mock.MockAllTables()
	c.Assert(err, IsNil)
	s.is = infoschema.MockInfoSchema(mockTables)
	s.ctx = mock.MockContext()
	s.Parser = parser.New()
	mockEngines, err := mock.MockEngines()
	c.Assert(err, IsNil)
	s.engineInfo, err = ConvertMockEnginesToEnginesInfo(mockEngines)
	c.Assert(err, IsNil)

	s.issuerParty = "alice"

	if recordTestOutput {
		var err error
		s.recordFile, err = os.CreateTemp("", "recordFile_*.txt")
		c.Assert(err, IsNil)
		fmt.Println("record file: ", s.recordFile.Name())
	}
}

func (s *testTranslatorSuite) TearDownSuite(c *C) {
	if recordTestOutput {
		s.recordFile.Close()
	}
}

type testConf struct {
	groupThreshold int
	batched        bool
}

type sPair struct {
	sql           string
	dotGraph      string
	briefPipeline string
	conf          testConf
}
