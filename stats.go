package fluence

import (
	"context"
	b64 "encoding/base64"
	"errors"
	"fmt"
	prometheus "github.com/prometheus/client_golang/api"
	prometheusv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"go.k6.io/k6/lib"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

var log = logging.Logger("fluence_metrics")

type fluenceMetrics struct {
	PeerConnectionCount   *metrics.Metric
	ParticleSendCount     *metrics.Metric
	ParticleReceiveCount  *metrics.Metric
	ParticleFailedCount   *metrics.Metric
	ParticleExecutionTime *metrics.Metric
	registry              *metrics.Registry
}

func registerMetrics(vu modules.VU) (fluenceMetrics, error) {
	var err error
	registry := vu.InitEnv().Registry
	instance := fluenceMetrics{}

	instance.registry = registry
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

type authRoundTripper struct {
	params PrometheusParams
	next   http.RoundTripper
}

type PrometheusParams struct {
	OrgId    string    `json:"org_id"`
	Address  string    `json:"address"`
	Username string    `json:"username"`
	Password string    `json:"password"`
	Env      string    `json:"env"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
}

func (a *authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", a.params.OrgId)
	encoded := b64.StdEncoding.EncodeToString([]byte(a.params.Username + ":" + a.params.Password))
	req.Header.Set("Authorization", "Basic "+encoded)
	return a.next.RoundTrip(req)
}

func (m *fluenceMetrics) InjectPrometheusMetrics(state *lib.State, params PrometheusParams) error {
	log.Debug("Inject metrics from: ", params.Address)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) //TODO: make constant for timeout
	defer cancel()

	config := prometheus.Config{
		Address: params.Address,
		RoundTripper: &authRoundTripper{
			params: params,
			next:   prometheus.DefaultRoundTripper,
		},
	}

	client, err := prometheus.NewClient(config)

	if err != nil {
		log.Error("Could not connect to prometheus: ", err)
		return err
	}

	api := prometheusv1.NewAPI(client)

	r := prometheusv1.Range{Start: params.Start, End: params.End, Step: time.Minute}

	ctm := state.Tags.GetCurrentValues()

	var samples []metrics.Sample
	sendParticlePerSecondSamples, err := m.fetchSendParticlesPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch send particles per second data: ", err)
	}
	samples = append(samples, sendParticlePerSecondSamples...)

	receiveParticlePerSecondSamples, err := m.fetchReceiveParticlesPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch receive particles per second data: ", err)
	}
	samples = append(samples, receiveParticlePerSecondSamples...)

	connectionCountSamples, err := m.fetchConnectionCount(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch counnection count: ", err)
	}
	samples = append(samples, connectionCountSamples...)

	interperatationPerSecond, err := m.fetchInterperatationPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch iterpretation per second: ", err)
	}
	samples = append(samples, interperatationPerSecond...)

	serviceCallPerSecond, err := m.fetchServiceCallPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch service call per second: ", err)
	}
	samples = append(samples, serviceCallPerSecond...)

	loadAvarage1, err := m.fetchLoadAverage1(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch load avarage 1m: ", err)
	}
	samples = append(samples, loadAvarage1...)

	loadAvarage5, err := m.fetchLoadAverage5(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch load avarage 5m: ", err)
	}
	samples = append(samples, loadAvarage5...)

	loadAvarage15, err := m.fetchLoadAverage15(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch load avarage 15m: ", err)
	}
	samples = append(samples, loadAvarage15...)

	cpuLoad, err := m.fetchCpuTime(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch cpu load: ", err)
	}
	samples = append(samples, cpuLoad...)

	memoryUsage, err := m.fetchMemoryUsage(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch memory usage: ", err)
	}
	samples = append(samples, memoryUsage...)

	networkBytesReceivedPerSecond, err := m.fetchNetworkBytesReceivedPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch netword received per second: ", err)
	}
	samples = append(samples, networkBytesReceivedPerSecond...)

	networkBytesSendPerSecond, err := m.fetchNetworkBytesSendPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch netword send per second: ", err)
	}
	samples = append(samples, networkBytesSendPerSecond...)

	networkBytesReceivedTotal, err := m.fetchNetworkBytesReceivedTotal(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch netword received total: ", err)
	}
	samples = append(samples, networkBytesReceivedTotal...)

	networkBytesSendTotal, err := m.fetchNetworkBytesSendTotal(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch netword send total: ", err)
	}
	samples = append(samples, networkBytesSendTotal...)

	fsUsageTotal, err := m.fetchFsUsageBytesTotal(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch fs usage total: ", err)
	}
	samples = append(samples, fsUsageTotal...)

	fsUsagePerSecond, err := m.fetchFsUsageBytesPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch fs usage per second: ", err)
	}
	samples = append(samples, fsUsagePerSecond...)

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Samples(samples))

	err = m.injectInterpretationTime(ctx, api, r, ctm, state, params.Env)
	if err != nil {
		log.Warn("Could not inject interpretation time: ", err)
	}

	err = m.injectServiceCallTime(ctx, api, r, ctm, state, params.Env)
	if err != nil {
		log.Warn("Could not inject service call time: ", err)
	}

	return nil
}

func (m *fluenceMetrics) fetchSendParticlesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("sum by (env,instance) (rate(connectivity_particle_send_success_total{env=\"%s\"}[1m]))", env), "fluence_peer_%s_particle_per_second_receive", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchReceiveParticlesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("sum by (env,instance) (rate(connection_pool_received_particles_total{env=\"%s\"}[1m]))", env), "fluence_peer_%s_particle_per_second_receive", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchConnectionCount(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("connection_pool_connected_peers{env=\"%s\"}", env), "fluence_peer_%s_connection_count", ctx, api, r, tagsMeta, metrics.Gauge, metrics.Default)
}

func (m *fluenceMetrics) fetchInterperatationPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("sum by (instance) (rate(particle_executor_interpretation_time_sec_count{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_interpretation_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchServiceCallPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("sum by (instance) (rate(particle_executor_service_call_time_sec_count{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_service_call_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchLoadAverage1(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("avg by(instance) (node_load1{env=\"%s\"})", env), "fluence_peer_%s_load_average_1m", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchLoadAverage5(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("avg by(instance) (node_load5{env=\"%s\"})", env), "fluence_peer_%s_load_average_5m", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchLoadAverage15(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("avg by(instance) (node_load15{env=\"%s\"})", env), "fluence_peer_%s_load_average_15m", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchCpuTime(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("sum by (name,instance) (rate(container_cpu_user_seconds_total{env=~\"%s\",image!=\"\", name=~\"nox.*\"}[1m]) * 100)", env), "fluence_peer_%s_cpu_load", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchMemoryUsage(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 15 * time.Millisecond
	return m.fetchQueryData(fmt.Sprintf("container_memory_usage_bytes{env=~\"%s\",image!=\"\", name=~\"nox.*\"}", env), "fluence_peer_%s_memory_usage", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesReceivedPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (rate(container_network_receive_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_network_bytes_received_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesReceivedTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_network_receive_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_network_bytes_received_total", ctx, api, r, tagsMeta, metrics.Counter, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesSendPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (rate(container_network_transmit_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_network_bytes_send_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesSendTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_network_transmit_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_network_bytes_send_total", ctx, api, r, tagsMeta, metrics.Counter, metrics.Data)
}

func (m *fluenceMetrics) fetchFsUsageBytesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (rate(container_fs_usage_bytes{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_fs_bytes_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchFsUsageBytesTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 1 * time.Minute
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_fs_usage_bytes{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_fs_bytes_total", ctx, api, r, tagsMeta, metrics.Counter, metrics.Data)
}

func (m *fluenceMetrics) injectInterpretationTime(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, state *lib.State, env string) error {
	r.Step = 15 * time.Millisecond
	return m.injectQueryHistoToTrend(fmt.Sprintf("sum by(instance, le) (increase(particle_executor_interpretation_time_sec_bucket{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_interpretation_time", ctx, api, r, tagsMeta, state)
}

func (m *fluenceMetrics) injectServiceCallTime(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, state *lib.State, env string) error {
	r.Step = 15 * time.Millisecond
	return m.injectQueryHistoToTrend(fmt.Sprintf("sum by(instance, le) (increase(particle_executor_service_call_time_sec_bucket{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_service_call_time", ctx, api, r, tagsMeta, state)
}

type pointData struct {
	le    float64
	value int64
}

func (m *fluenceMetrics) injectQueryHistoToTrend(query string, metricName string, ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, state *lib.State) error {
	r.Step = time.Minute
	result, warnings, err := api.QueryRange(ctx, query, r, prometheusv1.WithTimeout(5*time.Second)) //TODO: make constant for timeout and env as parameter

	if err != nil {
		return err
	}

	if len(warnings) > 0 {
		log.Warn("Warnings: ", warnings)
	}

	if matrix, ok := result.(model.Matrix); ok {
		var instanceGroup = make(map[string]map[time.Time][]pointData)

		for _, sampleStream := range matrix {
			leStr := string(sampleStream.Metric["le"])
			if leStr == "+Inf" || leStr == "-Inf" {
				continue
			}
			le, err := strconv.ParseFloat(leStr, 64)
			if err != nil {
				return err
			}
			instance := string(sampleStream.Metric["instance"])
			instance = strings.ReplaceAll(instance, "-", "_")

			instanceGroupdData, ok := instanceGroup[instance]
			if !ok {
				instanceGroupdData = make(map[time.Time][]pointData)
				instanceGroup[instance] = instanceGroupdData
			}

			for _, samplePair := range sampleStream.Values {
				unixTime := time.UnixMilli(int64(samplePair.Timestamp))
				point := pointData{
					le:    le,
					value: int64(samplePair.Value),
				}
				timeGroupData, ok := instanceGroupdData[unixTime]
				if ok {
					i := sort.Search(len(timeGroupData), func(i int) bool { return timeGroupData[i].le >= point.le })
					if i == len(timeGroupData) {
						timeGroupData = append(timeGroupData, point)
					} else {
						timeGroupData = append(timeGroupData[:i+1], timeGroupData[i:]...)
						timeGroupData[i] = point
					}

					instanceGroupdData[unixTime] = timeGroupData
				} else {
					instanceGroupdData[unixTime] = []pointData{point}
				}
			}
		}

		var samples []metrics.Sample
		for instance, instanceGroupData := range instanceGroup {
			metric, err := m.registry.NewMetric(fmt.Sprintf(metricName, instance), metrics.Trend, metrics.Time)
			if err != nil {
				return err
			}
			for t, timeGroupData := range instanceGroupData {
				var previousValue int64 = 0
				for _, entry := range timeGroupData {
					size := entry.value - previousValue
					previousValue = entry.value
					localSamples := make([]metrics.Sample, size)
					for i := range localSamples {
						localSamples[i] = metrics.Sample{
							Time: t,
							TimeSeries: metrics.TimeSeries{
								Metric: metric,
								Tags:   tagsMeta.Tags,
							},
							Value:    entry.le,
							Metadata: tagsMeta.Metadata,
						}
					}
					samples = append(samples, localSamples...)
				}
			}
		}
		metrics.PushIfNotDone(ctx, state.Samples, metrics.Samples(samples))
	} else {
		log.Warn("Query did not return a valid matrix.")
	}

	return nil
}

func (m *fluenceMetrics) fetchQueryData(query string, metricName string, ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, metricType metrics.MetricType, valueType metrics.ValueType) ([]metrics.Sample, error) {
	result, warnings, err := api.QueryRange(ctx, query, r, prometheusv1.WithTimeout(5*time.Second)) //TODO: make constant for timeout and env as parameter

	if err != nil {
		return nil, err
	}

	if len(warnings) > 0 {
		log.Warn("Warnings: ", warnings)
	}
	var samples []metrics.Sample

	if matrix, ok := result.(model.Matrix); ok {
		for _, sampleStream := range matrix {
			instance := string(sampleStream.Metric["instance"])
			instance = strings.ReplaceAll(instance, "-", "_")
			metric, err := m.registry.NewMetric(fmt.Sprintf(metricName, instance), metricType, valueType)
			if err != nil {
				return nil, err
			}
			samplesStep := make([]metrics.Sample, len(sampleStream.Values))
			for i, samplePair := range sampleStream.Values {
				entry := samplePair
				unixTime := time.UnixMilli(int64(entry.Timestamp))
				samplesStep[i] = metrics.Sample{
					Time: unixTime,
					TimeSeries: metrics.TimeSeries{
						Metric: metric,
						Tags:   tagsMeta.Tags,
					},
					Value:    float64(entry.Value),
					Metadata: tagsMeta.Metadata,
				}
			}
			samples = append(samples, samplesStep...)
		}
	} else {
		log.Warn("Query did not return a valid matrix.")
	}

	return samples, nil
}
