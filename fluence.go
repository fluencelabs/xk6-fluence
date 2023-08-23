package fluence

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/patrickmn/go-cache"
	"io"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.k6.io/k6/metrics"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
)

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
	f          *Fluence
	ctx        context.Context
	finalizer  context.CancelFunc
	host       host.Host
	PeerId     peer.ID
	relay      string
	remoteAddr peer.AddrInfo
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
	ctx, cancel := context.WithCancel(context.Background())
	peerInstance, err := libp2p.New(
		libp2p.NoListenAddrs,
		// Usually EnableRelay() is not required as it is enabled by default
		// but NoListenAddrs overrides this, so we're adding it in explictly again.
		libp2p.EnableRelay(),
	)
	if err != nil {
		logger.Error("Could not create peerInstance.", err)
		cancel()
		return nil, ConnectionFailed
	}

	if err := peerInstance.Connect(ctx, b.remoteAddr); err != nil {
		logger.Error("Could not connect to remote addr.", err)
		cancel()
		return nil, ConnectionFailed
	}

	peerInstance.SetStreamHandler("/fluence/particle/2.0.0", func(s network.Stream) {
		f := func() {
			_, err := io.ReadAll(s)
			if err != nil {
				return
			}
		}
		f()
	})

	finalizer := func() {
		err = peerInstance.Close()
		if err != nil {
			log.Warn("Could not close peerInstance")
			return
		}
		cancel()
	}

	con := Connection{}
	con.f = b.f
	con.ctx = ctx
	con.finalizer = finalizer
	con.host = peerInstance
	con.PeerId = peerInstance.ID()
	con.relay = b.relay
	con.remoteAddr = b.remoteAddr

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
	particle := Particle{}
	particle.Action = "Particle"
	particle.ID = uuid.New()
	particle.InitPeerId = c.PeerId
	particle.Timestamp = timestamp(time.Now())
	particle.Ttl = 3600
	particle.Script = script
	particle.Signature = []int{}
	particle.Data = []byte{}

	stream, err := c.host.NewStream(network.WithUseTransient(c.ctx, "fluence/particle/2.0.0"), c.remoteAddr.ID, "/fluence/particle/2.0.0")
	if err != nil {
		logger.Error("Could not create stream.", err)
		return ConnectionFailed
	}
	defer func(stream network.Stream) {
		err := stream.Close()
		if err != nil {
			log.Warn("Could not close stream", err)
		}
	}(stream)
	writer := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

	serialisedParticle, err := json.Marshal(particle)

	if err != nil {
		log.Error("Failed to serialize particle.", err)
		return SendFailed
	}

	_, err = writer.Write(varint.ToUvarint(uint64(len(serialisedParticle))))
	if err != nil {
		logger.Error("Could not write len.", err)
		return SendFailed
	}

	_, err = writer.Write(serialisedParticle)
	if err != nil {
		log.Error("Could not write message.", err)
		return SendFailed
	}
	err = writer.Flush()
	if err != nil {
		log.Error("Could not flush message.", err)
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

func (cb *CachedBuilder) Connect() (*CachedConnection, error) {
	key := ""
	switch cb.cacheType {
	case PerVU:
		id := cb.b.f.vu.State().VUID
		key = fmt.Sprintf("%d_%s", id, cb.b.relay)
	case Global:
		key = cb.b.relay
	}
	mu.Lock()
	defer mu.Unlock()
	if value, found := ConnectionCache.Get(key); found {
		return value.(*CachedConnection), nil
	} else {
		underlyingConnection, err := cb.b.Connect()
		if err != nil {
			return nil, err
		}
		connection := CachedConnection{}
		connection.connection = underlyingConnection
		ConnectionCache.Set(key, &connection, cache.NoExpiration)
		return &connection, nil
	}
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
