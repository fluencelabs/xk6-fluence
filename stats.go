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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) //TODO: make constant for timeout
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

	log.Info("Waiting 0 rates...")
	m.waitZeroRates(ctx, api, params.Env)
	log.Info("Waiting 0 rates finished")

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

	cpuLoadUser, err := m.fetchCpuTimeUser(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch cpu load user: ", err)
	}
	samples = append(samples, cpuLoadUser...)

	cpuLoadSystem, err := m.fetchCpuTimeSystem(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch cpu load system: ", err)
	}
	samples = append(samples, cpuLoadSystem...)

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

	fsWriteBytesTotal, err := m.fetchFsWriteBytesTotal(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch fs write bytes total: ", err)
	}
	samples = append(samples, fsWriteBytesTotal...)

	fsWriteBytesPerSecond, err := m.fetchFsWriteBytesPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch fs write bytes per second: ", err)
	}
	samples = append(samples, fsWriteBytesPerSecond...)

	fsReadBytesTotal, err := m.fetchFsReadBytesTotal(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch fs read bytes total: ", err)
	}
	samples = append(samples, fsReadBytesTotal...)

	fsReadBytesPerSecond, err := m.fetchFsReadBytesPerSecond(ctx, api, r, ctm, params.Env)
	if err != nil {
		log.Warn("Could not fetch fs read bytes per second: ", err)
	}
	samples = append(samples, fsReadBytesPerSecond...)

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

func (m *fluenceMetrics) waitZeroRates(ctx context.Context, api prometheusv1.API, env string) {
	finishCh := make(chan string, 1)
	go func() {
	outer:
		for {
			query := fmt.Sprintf("sum by (instance) (rate(connection_pool_received_particles_total{env=\"%s\"}[30s]))", env)
			rates, err := m.fetchLatest(query, ctx, api)
			log.Info("Rates: ", rates)
			if err != nil {
				log.Warn("Could not fetch received particles rates: ", err)
				time.Sleep(5 * time.Second)
				continue
			}
			for _, rate := range rates {
				if rate.value != 0 {
					time.Sleep(5 * time.Second)
					continue outer
				}
			}
			query = fmt.Sprintf("sum by (instance) (rate(connectivity_particle_send_success_total{env=\"%s\"}[30s]))", env)
			rates, err = m.fetchLatest(query, ctx, api)
			log.Info("Rates: ", rates)
			if err != nil {
				log.Warn("Could not fetch send particles rates: ", err)
				time.Sleep(5 * time.Second)
				continue
			}
			for _, rate := range rates {
				if rate.value != 0 {
					time.Sleep(5 * time.Second)
					continue outer
				}
			}
			finishCh <- "done"
			break
		}
	}()

	select {
	case _ = <-finishCh:
		{

		}
	case <-time.After(1 * time.Minute):
		log.Warn("Rate waiting timeout")
	}
}

type rate struct {
	instance string
	value    float64
}

func (m *fluenceMetrics) fetchSendParticlesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (env,instance) (rate(connectivity_particle_send_success_total{env=\"%s\"}[1m]))", env), "fluence_peer_%s_particle_per_second_receive", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchReceiveParticlesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (env,instance) (rate(connection_pool_received_particles_total{env=\"%s\"}[1m]))", env), "fluence_peer_%s_particle_per_second_receive", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchConnectionCount(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("connection_pool_connected_peers{env=\"%s\"}", env), "fluence_peer_%s_connection_count", ctx, api, r, tagsMeta, metrics.Gauge, metrics.Default)
}

func (m *fluenceMetrics) fetchInterperatationPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (instance) (rate(particle_executor_interpretation_time_sec_count{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_interpretation_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchServiceCallPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (instance) (rate(particle_executor_service_call_time_sec_count{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_service_call_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchLoadAverage1(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("avg by(instance) (node_load1{env=\"%s\"})", env), "fluence_peer_%s_load_average_1m", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchLoadAverage5(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("avg by(instance) (node_load5{env=\"%s\"})", env), "fluence_peer_%s_load_average_5m", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchLoadAverage15(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("avg by(instance) (node_load15{env=\"%s\"})", env), "fluence_peer_%s_load_average_15m", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchCpuTime(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (name,instance) (rate(container_cpu_usage_seconds_total{env=~\"%s\",image!=\"\", name=~\"nox.*\"}[1m]) * 100)", env), "fluence_peer_%s_cpu_load", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchCpuTimeUser(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (name,instance) (rate(container_cpu_user_seconds_total{env=~\"%s\",image!=\"\", name=~\"nox.*\"}[1m]) * 100)", env), "fluence_peer_%s_cpu_user_load", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchCpuTimeSystem(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by (name,instance) (rate(container_cpu_system_seconds_total{env=~\"%s\",image!=\"\", name=~\"nox.*\"}[1m]) * 100)", env), "fluence_peer_%s_cpu_system_load", ctx, api, r, tagsMeta, metrics.Trend, metrics.Default)
}

func (m *fluenceMetrics) fetchMemoryUsage(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("container_memory_usage_bytes{env=~\"%s\",image!=\"\", name=~\"nox.*\"}", env), "fluence_peer_%s_memory_usage", ctx, api, r, tagsMeta, metrics.Gauge, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesReceivedPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by(instance) (rate(container_network_receive_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_network_bytes_received_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesReceivedTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	duration := r.End.Sub(r.Start)
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_network_receive_bytes_total{env=~\"%s\", name=~\"nox.*\"}[%dms]))", env, duration.Milliseconds()), "fluence_peer_%s_network_bytes_received_total", ctx, api, tagsMeta, metrics.Gauge, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesSendPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by(instance) (rate(container_network_transmit_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_network_bytes_send_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchNetworkBytesSendTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	duration := r.End.Sub(r.Start)
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_network_transmit_bytes_total{env=~\"%s\", name=~\"nox.*\"}[%dms]))", env, duration.Milliseconds()), "fluence_peer_%s_network_bytes_send_total", ctx, api, tagsMeta, metrics.Gauge, metrics.Data)
}

func (m *fluenceMetrics) fetchFsWriteBytesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by(instance) (rate(container_fs_writes_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_fs_write_bytes_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchFsWriteBytesTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	duration := r.End.Sub(r.Start)
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_fs_writes_bytes_total{env=~\"%s\", name=~\"nox.*\"}[%dms]))", env, duration.Milliseconds()), "fluence_peer_%s_fs_write_bytes", ctx, api, tagsMeta, metrics.Gauge, metrics.Data)
}

func (m *fluenceMetrics) fetchFsReadBytesPerSecond(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	r.Step = 150 * time.Millisecond
	return m.fetchQueryRangeData(fmt.Sprintf("sum by(instance) (rate(container_fs_reads_bytes_total{env=~\"%s\", name=~\"nox.*\"}[1m]))", env), "fluence_peer_%s_fs_read_bytes_per_second", ctx, api, r, tagsMeta, metrics.Trend, metrics.Data)
}

func (m *fluenceMetrics) fetchFsReadBytesTotal(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, env string) ([]metrics.Sample, error) {
	duration := r.End.Sub(r.Start)
	return m.fetchQueryData(fmt.Sprintf("sum by(instance) (increase(container_fs_reads_bytes_total{env=~\"%s\", name=~\"nox.*\"}[%dms]))", env, duration.Milliseconds()), "fluence_peer_%s_fs_read_bytes", ctx, api, tagsMeta, metrics.Gauge, metrics.Data)
}

func (m *fluenceMetrics) injectInterpretationTime(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, state *lib.State, env string) error {
	r.Step = 150 * time.Millisecond
	return m.injectQueryHistoToTrend(fmt.Sprintf("sum by(instance, le) (increase(particle_executor_interpretation_time_sec_bucket{env=~\"%s\"}[1m]))", env), "fluence_peer_%s_interpretation_time", ctx, api, r, tagsMeta, state)
}

func (m *fluenceMetrics) injectServiceCallTime(ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, state *lib.State, env string) error {
	r.Step = 150 * time.Millisecond
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

func (m *fluenceMetrics) fetchQueryRangeData(query string, metricName string, ctx context.Context, api prometheusv1.API, r prometheusv1.Range, tagsMeta metrics.TagsAndMeta, metricType metrics.MetricType, valueType metrics.ValueType) ([]metrics.Sample, error) {
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

func (m *fluenceMetrics) fetchQueryData(query string, metricName string, ctx context.Context, api prometheusv1.API, tagsMeta metrics.TagsAndMeta, metricType metrics.MetricType, valueType metrics.ValueType) ([]metrics.Sample, error) {
	now := time.Now()
	result, warnings, err := api.Query(ctx, query, now, prometheusv1.WithTimeout(5*time.Second)) //TODO: make constant for timeout and env as parameter
	if err != nil {
		return nil, err
	}

	if len(warnings) > 0 {
		log.Warn("Warnings: ", warnings)
	}
	var samples []metrics.Sample

	if vector, ok := result.(model.Vector); ok {
		for _, entry := range vector {
			instance := string(entry.Metric["instance"])
			instance = strings.ReplaceAll(instance, "-", "_")
			unixTime := time.UnixMilli(int64(entry.Timestamp))
			metric, err := m.registry.NewMetric(fmt.Sprintf(metricName, instance), metricType, valueType)
			if err != nil {
				return nil, err
			}
			samples = append(samples, metrics.Sample{
				Time: unixTime,
				TimeSeries: metrics.TimeSeries{
					Metric: metric,
					Tags:   tagsMeta.Tags,
				},
				Value:    float64(entry.Value),
				Metadata: tagsMeta.Metadata,
			})
		}
	} else {
		log.Warn("Query did not return a valid vector.")
	}

	return samples, nil
}

func (m *fluenceMetrics) fetchLatest(query string, ctx context.Context, api prometheusv1.API) ([]rate, error) {
	now := time.Now()
	result, warnings, err := api.Query(ctx, query, now, prometheusv1.WithTimeout(5*time.Second)) //TODO: make constant for timeout and env as parameter
	if err != nil {
		return nil, err
	}

	if len(warnings) > 0 {
		log.Warn("Warnings: ", warnings)
	}
	var rates []rate

	if vector, ok := result.(model.Vector); ok {
		for _, sample := range vector {
			instance := string(sample.Metric["instance"])
			rates = append(rates, rate{
				instance: instance,
				value:    float64(sample.Value),
			})
		}
	} else {
		log.Warn("Query did not return a valid vector.")
	}

	return rates, nil
}
