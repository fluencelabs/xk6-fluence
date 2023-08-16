package fluence

import (
	"errors"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type fluenceMetrics struct {
	PeerConnectionCount *metrics.Metric
}

func registerMetrics(vu modules.VU) (fluenceMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	fluenceMetrics := fluenceMetrics{}

	if fluenceMetrics.PeerConnectionCount, err = registry.NewMetric(
		"fluence_peer_connection_count", metrics.Counter); err != nil {
		return fluenceMetrics, errors.Unwrap(err)
	}

	return fluenceMetrics, nil
}
