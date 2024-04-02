package scheduler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/secretflow/scql/pkg/broker/config"
)

func TestNaiveScheduler(t *testing.T) {
	r := require.New(t)
	// test pod marshal unmarshal
	engine := residentEngine{
		Uri: config.EngineUri{
			ForPeer: "for_peer",
			ForSelf: "for_self",
		},
		JobID: "job_id",
	}
	jobInfo, err := engine.MarshalToText()
	r.NoError(err)

	scheduler := &naiveScheduler{}
	newEngine, err := scheduler.ParseEngineInstance(jobInfo)
	r.NoError(err)
	r.Equal(&engine, newEngine, fmt.Sprintf("origin: %+v, result: %+v", engine, newEngine))

}
