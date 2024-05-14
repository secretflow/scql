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

package application

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"sort"
	"strings"
	"sync"

	"golang.org/x/exp/slices"

	"github.com/sirupsen/logrus"

	"github.com/secretflow/scql/pkg/broker/storage"
	"github.com/secretflow/scql/pkg/parser/model"
	pb "github.com/secretflow/scql/pkg/proto-gen/scql"
)

type Checksum struct {
	TableSchema []byte
	CCL         []byte
}

func NewChecksumFromProto(checksumProto *pb.Checksum) Checksum {
	return Checksum{TableSchema: checksumProto.TableSchema, CCL: checksumProto.Ccl}
}

func (c *Checksum) CompareWith(checksum Checksum) pb.ChecksumCompareResult {
	if !slices.Equal(c.CCL, checksum.CCL) && !slices.Equal(c.TableSchema, checksum.TableSchema) {
		return pb.ChecksumCompareResult_TABLE_CCL_NOT_EQUAL
	}
	if !slices.Equal(c.CCL, checksum.CCL) {
		return pb.ChecksumCompareResult_CCL_NOT_EQUAL
	}
	if !slices.Equal(c.TableSchema, checksum.TableSchema) {
		return pb.ChecksumCompareResult_TABLE_SCHEMA_NOT_EQUAL
	}
	return pb.ChecksumCompareResult_EQUAL
}

func (c *Checksum) String() string {
	result := ""
	if len(c.TableSchema) > 0 {
		result += fmt.Sprintf("table schema: %x;", c.TableSchema)
	}
	if len(c.CCL) > 0 {
		result += fmt.Sprintf("ccl: %x", c.CCL)
	}
	return result
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (c *Checksum) TruncateString() string {
	result := ""
	checksumTruncateNum := 10
	if len(c.TableSchema) > 0 {
		result += fmt.Sprintf("table schema: %+v;", c.TableSchema[:min(len(c.TableSchema), checksumTruncateNum)])
	}
	if len(c.CCL) > 0 {
		result += fmt.Sprintf("ccl: %+v", c.CCL[:min(len(c.TableSchema), checksumTruncateNum)])
	}
	return result
}

type ChecksumStorage struct {
	// maps from party code to checksum
	// localChecksums keeps checksum calculated by local storage
	// remoteChecksums keeps checksums from their owner party(not include self code)
	localChecksums  sync.Map
	remoteChecksums sync.Map
}

func (s *ChecksumStorage) GetLocal(partyCode string) (Checksum, error) {
	value, ok := s.localChecksums.Load(partyCode)
	if !ok {
		return Checksum{}, fmt.Errorf("failed to find local checksum for party %s", partyCode)
	}
	checksum, ok := value.(Checksum)
	if !ok {
		return Checksum{}, fmt.Errorf("failed to parse local checksum from %+v", value)
	}
	logrus.Infof("get local checksum %s for party %s", checksum.TruncateString(), partyCode)
	return checksum, nil
}

func (s *ChecksumStorage) GetRemote(partyCode string) (Checksum, error) {
	value, ok := s.remoteChecksums.Load(partyCode)
	if !ok {
		return Checksum{}, fmt.Errorf("failed to find remote checksum for party %s", partyCode)
	}
	checksum, ok := value.(Checksum)
	if !ok {
		return Checksum{}, fmt.Errorf("failed to parse remote checksum from %+v", value)
	}
	logrus.Infof("get remote checksum %s for party %s", checksum.TruncateString(), partyCode)
	return checksum, nil
}

func (s *ChecksumStorage) SaveLocal(partyCode string, sum Checksum) error {
	if len(sum.TableSchema) == 0 {
		return fmt.Errorf("table schema in checksum is empty")
	}
	if len(sum.CCL) == 0 {
		return fmt.Errorf("ccl in checksum is empty")
	}
	s.localChecksums.Store(partyCode, sum)
	logrus.Infof("save local checksum %s for party %s", sum.TruncateString(), partyCode)
	return nil
}

func (s *ChecksumStorage) SaveRemote(partyCode string, pbChecksum *pb.Checksum) error {
	if pbChecksum == nil {
		return fmt.Errorf("checksum in pb is nil")
	}
	sum := NewChecksumFromProto(pbChecksum)
	if len(sum.TableSchema) == 0 {
		return fmt.Errorf("table schema in checksum is empty")
	}
	if len(sum.CCL) == 0 {
		return fmt.Errorf("ccl in checksum is empty")
	}
	s.remoteChecksums.Store(partyCode, sum)
	logrus.Infof("save remote checksum %s for party %s", sum.TruncateString(), partyCode)
	return nil
}

func (s *ChecksumStorage) CompareChecksumFor(partyCode string) (pb.ChecksumCompareResult, error) {
	localChecksum, err := s.GetLocal(partyCode)
	if err != nil {
		return 0, err
	}
	remoteChecksum, err := s.GetRemote(partyCode)
	if err != nil {
		return 0, err
	}
	reqChecksumCompareRes := localChecksum.CompareWith(remoteChecksum)
	if reqChecksumCompareRes != pb.ChecksumCompareResult_EQUAL {
		logrus.Warningf("compare result of checksum is %s, self checksum: %+v, remote checksum: %+v", reqChecksumCompareRes.String(), localChecksum, remoteChecksum)
	}
	return reqChecksumCompareRes, nil
}

type ChecksumHasher struct {
	tableSchemaCrypt hash.Hash
	cclCrypt         hash.Hash
}

func NewChecksumHasher() *ChecksumHasher {
	return &ChecksumHasher{
		tableSchemaCrypt: sha256.New(),
		cclCrypt:         sha256.New(),
	}
}

func (c *ChecksumHasher) InfeedTable(tableName string, columnInfos []*model.ColumnInfo) {
	c.tableSchemaCrypt.Write([]byte(tableName))

	sort.Slice(columnInfos, func(i, j int) bool {
		return columnInfos[i].Name.String() < columnInfos[j].Name.String()
	})
	for _, col := range columnInfos {
		c.tableSchemaCrypt.Write([]byte(col.Name.String()))
		c.tableSchemaCrypt.Write([]byte(col.GetTypeDesc()))
	}
}

func (c *ChecksumHasher) InfeedCCLs(ccls []*pb.SecurityConfig_ColumnControl) {
	sort.Slice(ccls, func(i, j int) bool {
		return strings.Join([]string{ccls[i].TableName, ccls[i].ColumnName, ccls[i].PartyCode}, " ") <
			strings.Join([]string{ccls[j].TableName, ccls[j].ColumnName, ccls[j].PartyCode}, " ")
	})
	for _, ccl := range ccls {
		c.cclCrypt.Write([]byte(ccl.TableName))
		c.cclCrypt.Write([]byte(ccl.ColumnName))
		c.cclCrypt.Write([]byte(ccl.Visibility.String()))
	}
}

func (c *ChecksumHasher) Finalize() Checksum {
	return Checksum{
		TableSchema: c.tableSchemaCrypt.Sum(nil),
		CCL:         c.cclCrypt.Sum(nil),
	}
}

// TODO(jingshi): DRY, use ChecksumHasher.InfeedTable instead
func GetTableChecksum(meta storage.TableMeta) string {
	crypt := sha256.New()
	// only care about table name
	crypt.Write([]byte(meta.Table.TableName))

	sort.Slice(meta.Columns, func(i, j int) bool {
		return meta.Columns[i].ColumnName < meta.Columns[j].ColumnName
	})
	for _, col := range meta.Columns {
		crypt.Write([]byte(col.ColumnName))
		crypt.Write([]byte(col.DType))
	}

	return fmt.Sprintf("%x", crypt.Sum(nil))
}

func GetCCLsChecksum(ccls []storage.ColumnPriv) string {
	hasher := NewChecksumHasher()
	hasher.InfeedCCLs(storage.ColumnPrivs2ColumnControls(ccls))
	checksum := hasher.Finalize()
	return fmt.Sprintf("%x", checksum.CCL)
}
