package fluence

import (
	"errors"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type fluenceMetrics struct {
	PeerConnectionCount *metrics.Metric
	ParticleCount       *metrics.Metric
}

func registerMetrics(vu modules.VU) (fluenceMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	instance := fluenceMetrics{}

	if instance.PeerConnectionCount, err = registry.NewMetric(
		"fluence_peer_connection_count", metrics.Counter); err != nil {
		return instance, errors.Unwrap(err)
	}

	if instance.ParticleCount, err = registry.NewMetric(
		"fluence_particle_count", metrics.Counter); err != nil {
		return instance, errors.Unwrap(err)
	}

	return instance, nil
}
