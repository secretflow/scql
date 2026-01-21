// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified by Ant Group in 2023

//go:build !codes
// +build !codes

package testutil

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
)

// record is a flag used for generate test result.
var record bool
var SkipOutJson bool

// IsRecording returns true if the -record flag is set
func IsRecording() bool {
	return record
}

func init() {
	flag.BoolVar(&record, "record", false, "to generate test result")
	flag.BoolVar(&SkipOutJson, "SkipOutJson", false, "put in and out data in one json file (*_in.json)")
}

type testCases struct {
	Name       string           `json:"name"`
	Cases      *json.RawMessage `json:"cases"` // For delayed parse.
	decodedOut interface{}      // For generate output.
}

// TestData stores all the data of a test suite.
type TestData struct {
	input          []testCases
	output         []testCases
	filePathPrefix string
	funcMap        map[string]int
	skipOutJson    bool  // Store whether this test suite should skip out json
}

// LoadTestSuiteData loads test suite data from file.
func LoadTestSuiteData(dir, suiteName string) (res TestData, err error) {
	res.filePathPrefix = filepath.Join(dir, suiteName)
	res.input, err = loadTestSuiteCases(fmt.Sprintf("%s_in.json", res.filePathPrefix))
	if err != nil {
		return res, err
	}
	
	// Store the current SkipOutJson state for this test suite
	res.skipOutJson = SkipOutJson
	
	if record {
		res.output = make([]testCases, len(res.input))
		for i := range res.input {
			res.output[i].Name = res.input[i].Name
		}
	} else {
		if !res.skipOutJson {
			outFilePath := fmt.Sprintf("%s_out.json", res.filePathPrefix)
			res.output, err = loadTestSuiteCases(outFilePath)
			if err != nil {
				// If the out file doesn't exist, automatically use SkipOutJson mode for this test suite
				if os.IsNotExist(err) {
					res.skipOutJson = true
				} else {
					return res, err
				}
			} else {
				if len(res.input) != len(res.output) {
					return res, errors.New(fmt.Sprintf("Number of test input cases %d does not match test output cases %d", len(res.input), len(res.output)))
				}
			}
		}
	}
	
	res.funcMap = make(map[string]int, len(res.input))
	for i, test := range res.input {
		res.funcMap[test.Name] = i
		if !res.skipOutJson && test.Name != res.output[i].Name {
			return res, errors.New(fmt.Sprintf("Input name of the %d-case %s does not match output %s", i, test.Name, res.output[i].Name))
		}
	}
	
	return res, nil
}

func loadTestSuiteCases(filePath string) (res []testCases, err error) {
	jsonFile, err := os.Open(filePath)
	if err != nil {
		return res, err
	}
	defer func() {
		if err1 := jsonFile.Close(); err == nil && err1 != nil {
			err = err1
		}
	}()
	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return res, err
	}
	// Remove comments(keep a // b), since they are not allowed in json
	re := regexp.MustCompile("(?s)  //.*?\n")
	err = json.Unmarshal(re.ReplaceAll(byteValue, nil), &res)
	return res, err
}

// GetTestCases gets the test cases for a test function.
func (t *TestData) GetTestCases(c *check.C, in interface{}, out interface{}) {
	// Extract caller's name.
	pc, _, _, ok := runtime.Caller(1)
	c.Assert(ok, check.IsTrue)
	details := runtime.FuncForPC(pc)
	funcNameIdx := strings.LastIndex(details.Name(), ".")
	funcName := details.Name()[funcNameIdx+1:]

	casesIdx, ok := t.funcMap[funcName]
	c.Assert(ok, check.IsTrue, check.Commentf("Must get test %s", funcName))
	err := json.Unmarshal(*t.input[casesIdx].Cases, in)
	c.Assert(err, check.IsNil)
	if !record {
		err = json.Unmarshal(*t.output[casesIdx].Cases, out)
		c.Assert(err, check.IsNil)
	} else {
		// Init for generate output file.
		inputLen := reflect.ValueOf(in).Elem().Len()
		v := reflect.ValueOf(out).Elem()
		if v.Kind() == reflect.Slice {
			v.Set(reflect.MakeSlice(v.Type(), inputLen, inputLen))
		}
	}
	t.output[casesIdx].decodedOut = out
}

// GetTestCasesWithoutOut gets the test cases for a test function without separate output.
// This is used for tests where the test data includes both inputs and expected outputs
// in the same structure (e.g., when using SkipOutJson mode).
func (t *TestData) GetTestCasesWithoutOut(c *check.C, in interface{}) {
	// Extract caller's name.
	pc, _, _, ok := runtime.Caller(1)
	c.Assert(ok, check.IsTrue)
	details := runtime.FuncForPC(pc)
	funcNameIdx := strings.LastIndex(details.Name(), ".")
	funcName := details.Name()[funcNameIdx+1:]
	casesIdx, ok := t.funcMap[funcName]
	c.Assert(ok, check.IsTrue, check.Commentf("Must get test %s", funcName))
	err := json.Unmarshal(*t.input[casesIdx].Cases, in)
	c.Assert(err, check.IsNil)
	
	// When in record mode, store the input as the decodedOut so it can be written back
	if record {
		t.output[casesIdx].decodedOut = in
	}
}

// OnRecord execute the function to update result.
func (t *TestData) OnRecord(updateFunc func()) {
	if record {
		updateFunc()
	}
}

// GenerateOutputIfNeeded generate the output file.
func (t *TestData) GenerateOutputIfNeeded() error {
	if !record {
		return nil
	}

	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	
	// Determine which data to write based on this test suite's skipOutJson flag
	var dataToWrite []testCases
	var outputFile string
	
	if t.skipOutJson {
		// When skipOutJson is true, write to *_in.json
		dataToWrite = make([]testCases, len(t.input))
		for i := range t.input {
			dataToWrite[i].Name = t.input[i].Name
			// Encode the decoded output data
			err := enc.Encode(t.output[i].decodedOut)
			if err != nil {
				return err
			}
			res := make([]byte, len(buf.Bytes()))
			copy(res, buf.Bytes())
			buf.Reset()
			rm := json.RawMessage(res)
			dataToWrite[i].Cases = &rm
		}
		outputFile = fmt.Sprintf("%s_in.json", t.filePathPrefix)
	} else {
		// When skipOutJson is false, write to *_out.json (original behavior)
		for i, test := range t.output {
			err := enc.Encode(test.decodedOut)
			if err != nil {
				return err
			}
			res := make([]byte, len(buf.Bytes()))
			copy(res, buf.Bytes())
			buf.Reset()
			rm := json.RawMessage(res)
			t.output[i].Cases = &rm
		}
		dataToWrite = t.output
		outputFile = fmt.Sprintf("%s_out.json", t.filePathPrefix)
	}
	
	err := enc.Encode(dataToWrite)
	if err != nil {
		return err
	}
	file, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer func() {
		if err1 := file.Close(); err == nil && err1 != nil {
			err = err1
		}
	}()
	_, err = file.Write(buf.Bytes())
	return err
}
