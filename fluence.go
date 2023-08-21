package fluence

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	"go.k6.io/k6/metrics"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
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

type Connection struct {
	f         *Fluence
	finalizer context.CancelFunc
	writer    *bufio.Writer
	PeerId    peer.ID
	relay     string
}

func (f *Fluence) Connect(relay string) (*Connection, error) {
	ctx, cancel := context.WithCancel(context.Background())
	remoteAddr, err := peer.AddrInfoFromString(relay)
	if err != nil {
		cancel()
		return nil, WrongRelayAddress
	}

	host, err := libp2p.New(
		libp2p.NoListenAddrs,
		// Usually EnableRelay() is not required as it is enabled by default
		// but NoListenAddrs overrides this, so we're adding it in explictly again.
		libp2p.EnableRelay(),
	)
	if err != nil {
		logger.Error("Could not create peer.", err)
		cancel()
		return nil, ConnectionFailed
	}

	if err := host.Connect(ctx, *remoteAddr); err != nil {
		logger.Error("Could not connect to remote addr.", err)
		cancel()
		return nil, ConnectionFailed
	}

	stream, err := host.NewStream(network.WithUseTransient(ctx, "fluence/particle/2.0.0"), remoteAddr.ID, "/fluence/particle/2.0.0")
	writer := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	if err != nil {
		logger.Error("Could not create writer.", err)
		cancel()
		return nil, ConnectionFailed
	}
	host.SetStreamHandler("/fluence/particle/2.0.0", func(s network.Stream) {
		_, err := io.ReadAll(s)
		if err != nil {
			return
		}
	})

	finalizer := func() {
		err := writer.Flush()
		if err != nil {
			log.Warn("Could not flush writer")
			return
		}
		err = stream.Close()
		if err != nil {
			log.Warn("Could not close stream")
			return
		}
		err = host.Close()
		if err != nil {
			log.Warn("Could not close peer")
			return
		}
		cancel()
	}

	con := Connection{}
	con.f = f
	con.finalizer = finalizer
	con.writer = writer.Writer
	con.PeerId = host.ID()
	con.relay = relay

	state := f.vu.State()
	ctm := f.vu.State().Tags.GetCurrentValues()
	now := time.Now()
	sampleTags := ctm.Tags.With("relay", relay)

	if state == nil {
		return nil, ConnectionFailed
	}

	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
		Samples: []metrics.Sample{
			{
				Time: now,
				TimeSeries: metrics.TimeSeries{
					Metric: f.metrics.PeerConnectionCount,
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

func (c *Connection) Send(script string) error {
	ctx := context.Background()
	particle := Particle{}
	particle.Action = "Particle"
	particle.ID = uuid.New()
	particle.InitPeerId = c.PeerId
	particle.Timestamp = timestamp(time.Now())
	particle.Ttl = 3600
	particle.Script = script
	particle.Signature = []int{}
	particle.Data = []byte{}

	serialisedParticle, err := json.Marshal(particle)

	if err != nil {
		log.Error("Failed to serialize particle.", err)
		return SendFailed
	}

	_, err = c.writer.Write(varint.ToUvarint(uint64(len(serialisedParticle))))
	if err != nil {
		logger.Error("Could not write len.", err)
		return SendFailed
	}

	_, err = c.writer.Write(serialisedParticle)
	if err != nil {
		log.Error("Could not write message.", err)
		return SendFailed
	}
	err = c.writer.Flush()
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

	metrics.PushIfNotDone(ctx, state.Samples, metrics.ConnectedSamples{
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

func (c *Connection) Close() {
	c.finalizer()
}

func (f *Fluence) SendParticle(relay, script string) error {
	connection, err := f.Connect(relay)
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
