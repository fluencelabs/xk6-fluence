package fluence

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dop251/goja"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peerstore"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"go.k6.io/k6/js/promises"
	"io"
	"strconv"
	"sync"
	"time"

	"go.k6.io/k6/metrics"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
)

type Particle struct {
	Action     string    `json:"action"`
	ID         string    `json:"id"`
	InitPeerId peer.ID   `json:"init_peer_id"`
	Timestamp  timestamp `json:"timestamp"`
	Ttl        int64     `json:"ttl"`
	Script     string    `json:"script"`
	Signature  []int     `json:"signature"`
	Data       []byte    `json:"data"`
}

type Builder struct {
	f          *Fluence
	relay      string
	remoteAddr peer.AddrInfo
}

type Connection struct {
	f            *Fluence
	ctx          context.Context
	finalizer    context.CancelFunc
	peerInstance host.Host
	PeerId       peer.ID
	relay        string
	remoteAddr   peer.AddrInfo
	callbacks    sync.Map
	privKey      crypto.PrivKey
}

func (f *Fluence) Builder(relay string) (*Builder, error) {
	remoteAddr, err := peer.AddrInfoFromString(relay)
	if err != nil {
		return nil, WrongRelayAddress
	}

	builder := Builder{}
	builder.f = f
	builder.relay = relay
	builder.remoteAddr = *remoteAddr

	return &builder, nil
}

func (b *Builder) Connect() (*Connection, error) {
	log.Debug("Connect: ", b.relay)
	ctx, cancel := context.WithCancel(context.Background())
	scalingLimits := rcmgr.DefaultLimits

	// Add limits around included libp2p protocols
	libp2p.SetDefaultServiceLimits(&scalingLimits)

	// Turn the scaling limits into a concrete set of limits using `.AutoScale`. This
	// scales the limits proportional to your system memory.
	scaledDefaultLimits := scalingLimits.AutoScale()
	// Tweak certain settings
	cfg := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			// Allow unlimited streams
			Conns:           rcmgr.Unlimited,
			ConnsInbound:    rcmgr.Unlimited,
			ConnsOutbound:   rcmgr.Unlimited,
			Streams:         rcmgr.Unlimited,
			StreamsInbound:  rcmgr.Unlimited,
			StreamsOutbound: rcmgr.Unlimited,
		},
		// Everything else is default. The exact values will come from `scaledDefaultLimits` above.
	}

	// Create our limits by using our cfg and replacing the default values with values from `scaledDefaultLimits`
	limits := cfg.Build(scaledDefaultLimits)

	// The resource manager expects a limiter, se we create one from our limits.
	limiter := rcmgr.NewFixedLimiter(limits)

	rm, err := rcmgr.NewResourceManager(limiter, rcmgr.WithMetricsDisabled())

	if err != nil {
		log.Error("Could not create resource manager: ", err)
		cancel()
		return nil, ConnectionFailed
	}

	prvKey, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		log.Error("Could not create private key: ", err)
		cancel()
		return nil, ConnectionFailed
	}

	cm, err := connmgr.NewConnManager(
		10,
		4000,
		connmgr.WithGracePeriod(1*time.Minute),
		connmgr.WithSilencePeriod(1*time.Minute),
	)

	if err != nil {
		log.Error("Could not create connection manager: ", err)
		cancel()
		return nil, ConnectionFailed
	}

	peerInstance, err := libp2p.New(
		libp2p.NoListenAddrs,
		libp2p.Identity(prvKey),
		libp2p.ProtocolVersion("fluence/particle/2.0.0"),
		libp2p.ResourceManager(rm),
		libp2p.ConnectionManager(cm),
		// Usually EnableRelay() is not required as it is enabled by default
		// but NoListenAddrs overrides this, so we're adding it in explictly again.
		libp2p.EnableRelay(),
	)
	if err != nil {
		log.Error("Could not create peer instance: ", err)
		cancel()
		return nil, ConnectionFailed
	}

	finalizer := func() {
		log.Debug("Connection finalizer called")
		err = peerInstance.Close()
		if err != nil {
			log.Warn("Could not close peer instance: ", err)
			return
		}
		cancel()
	}

	con := Connection{}
	con.f = b.f
	con.ctx = ctx
	con.finalizer = finalizer
	con.PeerId = peerInstance.ID()
	con.relay = b.relay
	con.remoteAddr = b.remoteAddr
	con.peerInstance = peerInstance
	con.callbacks = sync.Map{}
	con.privKey = prvKey

	peerInstance.SetStreamHandler("/fluence/particle/2.0.0", func(stream network.Stream) {
		defer func() {
			err := stream.Close()
			if err != nil {
				log.Warn("Could not close inbound stream", err)
				return
			}
		}()
		log.Debug("Message arrived")
		err := stream.SetReadDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			log.Error("Could not set read deadline", err)
			return
		}
		reader := bufio.NewReader(stream)
		particle, err := readParticle(reader)
		if err != nil {
			log.Error("Could not read particle from stream", err)
			return
		}
		log.Debugf("Particle %s arrived", particle.ID)
		callback, found := con.callbacks.Load(particle.ID)
		if found {
			callback := callback.(chan *Particle)
			callback <- particle
		}

	})

	if err := peerInstance.Connect(ctx, b.remoteAddr); err != nil {
		log.Error("Could not connect to remote addr: ", err)
		cancel()
		return nil, ConnectionFailed
	}

	peerInstance.Peerstore().AddAddrs(b.remoteAddr.ID, b.remoteAddr.Addrs, peerstore.PermanentAddrTTL)

	return &con, nil
}
func (c *Connection) Close() {
	c.finalizer()
}

func (c *Connection) UnsafeSend(data []byte) error {
	stream, err := c.peerInstance.NewStream(c.ctx, c.remoteAddr.ID, "/fluence/particle/2.0.0")
	if err != nil {
		log.Error("Could not create stream: ", err)
		return ConnectionFailed
	}
	defer func(stream network.Stream) {
		err := stream.Close()
		if err != nil {
			log.Warn("Could not close stream: ", err)
		}
	}(stream)

	readWriter := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

	_, err = readWriter.Writer.Write(data)
	if err != nil {
		log.Error("Could not write particle to the stream: ", err)
		return SendFailed
	}

	err = readWriter.Writer.Flush()
	if err != nil {
		log.Error("Could not write particle to the stream: ", err)
		return SendFailed
	}

	err = writeMetrics(c, []MetricValue{
		{
			metric: c.f.metrics.ParticleSendCount,
			value:  float64(1),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func writeData(writer *bufio.Writer, data []byte) error {
	_, err := writer.Write(data)
	if err != nil {
		log.Error("Could not write particle to the stream: ", err)
		return SendFailed
	}

	err = writer.Flush()
	if err != nil {
		log.Error("Could not write particle to the stream: ", err)
		return SendFailed
	}
	return nil
}

func (c *Connection) Send(script string, timeout time.Duration) error {
	particle, err := makeParticle(c.privKey, script, c.PeerId, timeout)
	if err != nil {
		return err
	}
	log.Debug("Sending particle: ", particle.ID)

	serialisedParticle, err := json.Marshal(particle)
	if err != nil {
		return err
	}

	particleBytes := encodeByteArray(serialisedParticle)

	return c.UnsafeSend(particleBytes)
}

func (c *Connection) AsyncExecute(script string, timeout time.Duration) *goja.Promise {
	promise, resolve, reject := promises.New(c.f.vu)

	go func() {
		particle, err := makeParticle(c.privKey, script, c.PeerId, timeout)
		if err != nil {
			reject(err)
			return
		}
		log.Debug("Execute particle: ", particle.ID)
		start := time.Now()

		callback, err := func() (chan *Particle, error) {
			stream, err := c.peerInstance.NewStream(c.ctx, c.remoteAddr.ID, "/fluence/particle/2.0.0")
			if err != nil {
				return nil, err
			}
			defer func(stream network.Stream) {
				err := stream.Close()
				if err != nil {
					log.Warn("Could not close stream: ", err)
				}
			}(stream)

			callback := make(chan *Particle, 1)
			c.callbacks.Store(particle.ID, callback)
			err = writeParticle(bufio.NewWriter(stream), *particle)
			if err != nil {
				return nil, err
			}
			err = writeMetrics(c, []MetricValue{
				{
					metric: c.f.metrics.ParticleSendCount,
					value:  float64(1),
				},
			})

			return callback, nil
		}()

		if err != nil {
			log.Error("Could not send particle: ", err)
			reject(err)
			return
		}

		defer func() {
			callback, loaded := c.callbacks.LoadAndDelete(particle.ID)
			if loaded {
				callback := callback.(chan *Particle)
				close(callback)
			}
		}()

		select {
		case response := <-callback:
			now := time.Now()
			duration := now.Sub(start)

			err = writeMetrics(c, []MetricValue{
				{
					metric: c.f.metrics.ParticleReceiveCount,
					value:  float64(1),
				},
				{
					metric: c.f.metrics.ParticleExecutionTime,
					value:  metrics.D(duration),
				},
			})
			if err != nil {
				reject(err)
				return
			}
			resolve(response)
			return
		case <-time.After(timeout):
			reject(errors.New("particle timeout"))
			return
		}

	}()

	return promise
}

func (c *Connection) Execute(script string, timeout time.Duration) (*Particle, error) {
	particle, err := makeParticle(c.privKey, script, c.PeerId, timeout)
	if err != nil {
		return nil, err
	}
	log.Debug("Execute particle: ", particle.ID)

	start := time.Now()
	callback, err := func() (chan *Particle, error) {
		stream, err := c.peerInstance.NewStream(c.ctx, c.remoteAddr.ID, "/fluence/particle/2.0.0")
		if err != nil {
			return nil, err
		}
		defer func(stream network.Stream) {
			err := stream.Close()
			if err != nil {
				log.Warn("Could not close stream: ", err)
			}
		}(stream)

		callback := make(chan *Particle, 1)
		c.callbacks.Store(particle.ID, callback)
		err = writeParticle(bufio.NewWriter(stream), *particle)
		if err != nil {
			return nil, err
		}
		err = writeMetrics(c, []MetricValue{
			{
				metric: c.f.metrics.ParticleSendCount,
				value:  float64(1),
			},
		})

		return callback, nil
	}()

	if err != nil {
		log.Error("Could not send particle: ", err)
		return nil, ExecuteFailed
	}

	defer func() {
		callback, loaded := c.callbacks.LoadAndDelete(particle.ID)
		if loaded {
			callback := callback.(chan *Particle)
			close(callback)
		}
	}()

	select {
	case response := <-callback:
		now := time.Now()
		duration := now.Sub(start)

		err = writeMetrics(c, []MetricValue{
			{
				metric: c.f.metrics.ParticleReceiveCount,
				value:  float64(1),
			},
			{
				metric: c.f.metrics.ParticleExecutionTime,
				value:  metrics.D(duration),
			},
		})
		if err != nil {
			return nil, err
		}

		return response, nil
	case <-time.After(timeout):
		return nil, errors.New("particle timeout")
	}
}

func (f *Fluence) SendParticle(relay, script string, timeout time.Duration) error {
	builder, err := f.Builder(relay)
	if err != nil {
		return err
	}
	connection, err := builder.Connect()
	if err != nil {
		return err
	}
	defer connection.Close()
	err = connection.Send(script, timeout)
	if err != nil {
		return err
	}
	return nil
}

func (f *Fluence) ExecuteParticle(relay, script string, timeout time.Duration) (*Particle, error) {
	builder, err := f.Builder(relay)
	if err != nil {
		return nil, err
	}
	connection, err := builder.Connect()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	particle, err := connection.Execute(script, timeout)
	if err != nil {
		return nil, err
	}
	return particle, nil
}

type timestamp time.Time

func (ct timestamp) MarshalJSON() ([]byte, error) {
	t := time.Time(ct)
	unixTimestamp := t.UnixMilli()
	return []byte(fmt.Sprintf("%d", unixTimestamp)), nil
}

func (ct *timestamp) UnmarshalJSON(data []byte) error {
	// Parse the JSON data into an integer representing Unix milliseconds
	unixMillis, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return err
	}

	// Convert Unix milliseconds to a time.Time value
	t := time.Unix(0, unixMillis*int64(time.Millisecond))

	// Set the timestamp value to the converted time.Time
	*ct = timestamp(t)

	return nil
}

func makeParticle(privKey crypto.PrivKey, script string, peerId peer.ID, timeout time.Duration) (*Particle, error) {
	particle := Particle{}
	particle.Action = "Particle"
	particle.ID = uuid.New().String()
	particle.InitPeerId = peerId
	particle.Timestamp = timestamp(time.Now())
	particle.Ttl = timeout.Milliseconds()
	particle.Script = script
	particle.Data = []byte{}

	data, err := serialiseSignatireData(particle)
	if err != nil {
		return nil, err
	}
	signature, err := privKey.Sign(data)
	if err != nil {
		return nil, err
	}

	signatureInt := make([]int, len(signature))
	for i, b := range signature {
		signatureInt[i] = int(b)
	}

	particle.Signature = signatureInt

	return &particle, err
}

func serialiseSignatireData(particle Particle) ([]byte, error) {
	bytes := []byte(particle.ID)

	timestampBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestampBytes, uint64(time.Time(particle.Timestamp).UnixMilli()))
	bytes = append(bytes, timestampBytes...)

	ttlBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(ttlBytes, uint32(particle.Ttl))
	bytes = append(bytes, ttlBytes...)

	bytes = append(bytes, []byte(particle.Script)...)

	return bytes, nil
}

func encodeByteArray(data []byte) []byte {
	result := varint.ToUvarint(uint64(len(data)))
	result = append(result, data...)
	return result
}

func writeParticle(writer *bufio.Writer, particle Particle) error {
	log.Debugf("Writing particle with id %s", particle.ID)
	serialisedParticle, err := json.Marshal(particle)

	if err != nil {
		return err
	}

	particleBytes := encodeByteArray(serialisedParticle)

	return writeData(writer, particleBytes)
}

func readParticle(reader *bufio.Reader) (*Particle, error) {
	log.Debug("Reading particle from the stream")
	particleLen, err := varint.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	particleBytes := make([]byte, particleLen)

	_, err = io.ReadFull(reader, particleBytes)

	if err != nil {
		return nil, err
	}

	particle := Particle{}
	err = json.Unmarshal(particleBytes, &particle)
	if err != nil {
		return nil, err
	}
	return &particle, nil
}

type MetricValue struct {
	metric *metrics.Metric
	value  float64
}

func writeMetrics(c *Connection, data []MetricValue) error {
	state := c.f.vu.State()
	if state == nil {
		return MetricsSubmissionFailed
	}
	ctm := state.Tags.GetCurrentValues()
	now := time.Now()
	sampleTags := ctm.Tags.With("relay", c.relay)

	samples := make([]metrics.Sample, len(data))
	for i := range data {
		entry := data[i]
		samples[i] = metrics.Sample{
			Time: now,
			TimeSeries: metrics.TimeSeries{
				Metric: entry.metric,
				Tags:   sampleTags,
			},
			Value:    entry.value,
			Metadata: ctm.Metadata,
		}
	}

	metrics.PushIfNotDone(c.ctx, state.Samples, metrics.ConnectedSamples{
		Samples: samples,
		Tags:    sampleTags,
		Time:    now,
	})
	return nil
}

func (f *Fluence) InjectPrometheusMetrics(params PrometheusParams) error {
	state := f.vu.State()
	if state == nil {
		return MetricsSubmissionFailed
	}
	return f.metrics.InjectPrometheusMetrics(state, params)
}
