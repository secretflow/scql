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

package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/planner/util"
)

func TestCollectIntoParties(t *testing.T) {
	r := require.New(t)
	{
		sql := "select * from dt1.test"
		parties, err := util.CollectIntoParties(sql)
		r.NoError(err)
		r.Equal(0, len(parties))
	}

	{
		sql := "select test.id from dt1.test into outfile party_code 'bob' '/tmp/file.csv' party_code 'carol' '/tmp/file.csv'"
		parties, err := util.CollectIntoParties(sql)
		r.NoError(err)
		r.Equal(2, len(parties))
	}

	{
		sql := "select test.id from dt1.test into outfile '/tmp/file.csv'"
		parties, err := util.CollectIntoParties(sql)
		r.NoError(err)
		r.Equal(0, len(parties))
	}

}
