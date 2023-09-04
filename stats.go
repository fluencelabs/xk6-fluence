package fluence

import (
	"errors"

	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type fluenceMetrics struct {
	PeerConnectionCount   *metrics.Metric
	ParticleSendCount     *metrics.Metric
	ParticleReceiveCount  *metrics.Metric
	ParticleFailedCount   *metrics.Metric
	ParticleExecutionTime *metrics.Metric
}

func registerMetrics(vu modules.VU) (fluenceMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	instance := fluenceMetrics{}

	if instance.PeerConnectionCount, err = registry.NewMetric(
		"fluence_peer_connection_count", metrics.Counter); err != nil {
		return instance, errors.Unwrap(err)
	}

	if instance.ParticleSendCount, err = registry.NewMetric(
		"fluence_particle_send_count", metrics.Counter); err != nil {
		return instance, errors.Unwrap(err)
	}

	if instance.ParticleReceiveCount, err = registry.NewMetric(
		"fluence_particle_receive_count", metrics.Counter); err != nil {
		return instance, errors.Unwrap(err)
	}

	if instance.ParticleFailedCount, err = registry.NewMetric(
		"fluence_particle_failed_count", metrics.Counter); err != nil {
		return instance, errors.Unwrap(err)
	}

	if instance.ParticleExecutionTime, err = registry.NewMetric(
		"fluence_particle_execution_time", metrics.Trend, metrics.Time); err != nil {
		return instance, errors.Unwrap(err)
	}

	return instance, nil
}
