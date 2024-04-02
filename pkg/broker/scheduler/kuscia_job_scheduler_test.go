package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKusciaScheduler(t *testing.T) {
	r := require.New(t)
	// test pod marshal unmarshal
	pod := kusciaEnginePod{
		JobID:             "job_id",
		TaskID:            "task_id",
		Endpoint:          "endpoint",
		KeepAliveForDebug: true,
	}
	jobInfo, err := pod.MarshalToText()
	r.NoError(err)

	scheduler := &kusciaJobScheduler{}
	newPod, err := scheduler.ParseEngineInstance(jobInfo)
	r.NoError(err)
	r.Equal(&pod, newPod)

}
