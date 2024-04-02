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

package prom

import (
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
)

const MetricsPath = "/metrics"

const ResponseStatusKey = "response_code"

var (
	defaultDuration = []float64{3 * 60, 30 * 60, 2 * 60 * 60, 12 * 60 * 60, 24 * 60 * 60}
	once            sync.Once
	monitor         *Monitor
)

type Monitor struct {
	RequestTotal    *prometheus.CounterVec
	RequestDuration *prometheus.HistogramVec
}

func GetMonitor() *Monitor {
	once.Do(initMonitor)
	return monitor
}

func (m *Monitor) Middleware(ctx *gin.Context) {
	if ctx.Request.URL.Path == MetricsPath {
		ctx.Next()
		return
	}
	startTime := time.Now()

	// execute normal process.
	ctx.Next()

	// after request
	r := ctx.Request
	code := strconv.Itoa(ctx.Writer.Status())
	status := ctx.GetString(ResponseStatusKey)

	// set request total
	m.RequestTotal.WithLabelValues(ctx.FullPath(), r.Method, code, status).Inc()

	// set request duration
	latency := time.Since(startTime)
	m.RequestDuration.WithLabelValues(ctx.FullPath(), r.Method, code, status).Observe(latency.Seconds())

}

func initMonitor() {
	monitor = &Monitor{}

	monitor.RequestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_request_total",
			Help: "all the server received request num.",
		},
		[]string{"uri", "method", "code", "status"})
	prometheus.MustRegister(monitor.RequestTotal)

	monitor.RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "the time server took to handle the request.",
			Buckets: defaultDuration,
		},
		[]string{"uri", "method", "code", "status"})
	prometheus.MustRegister(monitor.RequestDuration)
}
