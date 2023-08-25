package fluence

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/patrickmn/go-cache"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"go.k6.io/k6/metrics"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
)

var log = logging.Logger("fluence")

var (
	ConnectionCache *cache.Cache
	mu              sync.Mutex
)

type Particle struct {
	Action     string    `json:"action"`
	ID         uuid.UUID `json:"id"`
	InitPeerId peer.ID   `json:"init_peer_id"`
	Timestamp  timestamp `json:"timestamp"`
	Ttl        uint32    `json:"ttl"`
	Script     string    `json:"script"`
	Signature  []int     `json:"signature"`
	Data       []byte    `json:"data"`
}

type Builder struct {
	f          *Fluence
	relay      string
	remoteAddr peer.AddrInfo
}

type CachedBuilder struct {
	b         *Builder
	cacheType CacheType
}

type Connection struct {
	f            *Fluence
	ctx          context.Context
	finalizer    context.CancelFunc
	peerInstance host.Host
	PeerId       peer.ID
	relay        string
	remoteAddr   peer.AddrInfo
}

type CachedConnection struct {
	connection *Connection
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

type CacheType int8

const (
	PerVU  CacheType = 0
	Global CacheType = 1
)

func (b *Builder) CacheBy(cacheType CacheType) *CachedBuilder {
	builder := CachedBuilder{}
	builder.b = b
	builder.cacheType = cacheType
	return &builder
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
			// Allow unlimited outbound streams
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

	prvKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		log.Error("Could not create private key: ", err)
		cancel()
		return nil, ConnectionFailed
	}

	cm, err := connmgr.NewConnManager(
		10,
		4000,
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

	if err := peerInstance.Connect(ctx, b.remoteAddr); err != nil {
		log.Error("Could not connect to remote addr: ", err)
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

	state := b.f.vu.State()
	ctm := b.f.vu.State().Tags.GetCurrentValues()
	now := time.Now()
	sampleTags := ctm.Tags.With("relay", b.relay)

	if state == nil {
		return nil, ConnectionFailed
	}

	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: b.f.metrics.PeerConnectionCount,
					Tags:   sampleTags,
				},
				Value:    float64(1),
				Metadata: ctm.Metadata,
			},
		},
		Tags: sampleTags,
		Time: now,
	})

	return &con, nil
}

func (c *CachedConnection) Send(script string) error {
	err := c.connection.Send(script)
	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) Send(script string) error {
	log.Debug("Sending particle")
	particle := Particle{}
	particle.Action = "Particle"
	particle.ID = uuid.New()
	particle.InitPeerId = c.PeerId
	particle.Timestamp = timestamp(time.Now())
	particle.Ttl = 60000
	particle.Script = script
	particle.Signature = []int{}
	particle.Data = []byte{}

	stream, err := c.peerInstance.NewStream(network.WithUseTransient(c.ctx, "fluence/particle/2.0.0"), c.remoteAddr.ID, "/fluence/particle/2.0.0")
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

	writer := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

	serialisedParticle, err := json.Marshal(particle)

	if err != nil {
		log.Error("Failed to serialize particle: ", err)
		return SendFailed
	}

	_, err = writer.Write(varint.ToUvarint(uint64(len(serialisedParticle))))
	if err != nil {
		log.Error("Could not write len: ", err)
		return SendFailed
	}

	_, err = writer.Write(serialisedParticle)
	if err != nil {
		log.Error("Could not write message: ", err)
		return SendFailed
	}
	err = writer.Flush()
	if err != nil {
		log.Error("Could not flush message: ", err)
		return SendFailed
	}

	state := c.f.vu.State()
	ctm := c.f.vu.State().Tags.GetCurrentValues()
	now := time.Now()
	sampleTags := ctm.Tags.With("relay", c.relay)

	if state == nil {
		return ConnectionFailed
	}

	metrics.PushIfNotDone(c.ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: c.f.metrics.ParticleCount,
					Tags:   sampleTags,
				},
				Value:    float64(1),
				Metadata: ctm.Metadata,
			},
		},
		Tags: sampleTags,
		Time: now,
	})

	return nil
}

type create func() (interface{}, error)

func getOrSet(key string, fn create) (interface{}, error) {
	mu.Lock()
	defer mu.Unlock()
	if value, found := ConnectionCache.Get(key); found {
		return value, nil
	} else {
		value, err := fn()
		if err != nil {
			return nil, err
		}
		ConnectionCache.Set(key, value, cache.NoExpiration)
		return value, nil
	}
}

func (cb *CachedBuilder) Connect() (*CachedConnection, error) {
	key := ""
	switch cb.cacheType {
	case PerVU:
		id := cb.b.f.vu.State().VUID
		key = fmt.Sprintf("%d_%s", id, cb.b.relay)
	case Global:
		key = cb.b.relay
	}
	connect := func() (interface{}, error) {
		underlyingConnection, err := cb.b.Connect()
		if err != nil {
			return nil, err
		}
		connection := CachedConnection{}
		connection.connection = underlyingConnection
		return &connection, nil
	}
	connection, err := getOrSet(key, connect)
	if err != nil {
		return nil, err
	}
	return connection.(*CachedConnection), nil
}

func (c *CachedConnection) Close() {

}

func (c *Connection) Close() {
	c.finalizer()
}

func (f *Fluence) SendParticle(relay, script string) error {
	builder, err := f.Builder(relay)
	if err != nil {
		return err
	}
	connection, err := builder.Connect()
	if err != nil {
		return err
	}
	defer connection.Close()
	err = connection.Send(script)
	if err != nil {
		return err
	}
	return nil
}

type timestamp time.Time

func (ct timestamp) MarshalJSON() ([]byte, error) {
	t := time.Time(ct)
	unixTimestamp := t.UnixMilli()
	return []byte(fmt.Sprintf("%d", unixTimestamp)), nil
}
