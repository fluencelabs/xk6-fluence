package fluence

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-varint"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/fluence", new(Fluence))
}

type Fluence struct {
}

type Timestamp time.Time

func (ct Timestamp) MarshalJSON() ([]byte, error) {
	t := time.Time(ct)
	unixTimestamp := t.UnixMilli()
	return []byte(fmt.Sprintf("%d", unixTimestamp)), nil
}

type Particle struct {
	Action     string    `json:"action"`
	ID         uuid.UUID `json:"id"`
	InitPeerId peer.ID   `json:"init_peer_id"`
	Timestamp  Timestamp `json:"timestamp"`
	Ttl        uint32    `json:"ttl"`
	Script     string    `json:"script"`
	Signature  []int     `json:"signature"`
	Data       []byte    `json:"data"`
}

func (f *Fluence) SendParticle(relay, data string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	remoteAddr, err := peer.AddrInfoFromString(relay)
	if err != nil {
		log.Printf("Wrong relay address: %v %v", relay, err)
		return false
	}

	host, err := libp2p.New(
		libp2p.NoListenAddrs,
		// Usually EnableRelay() is not required as it is enabled by default
		// but NoListenAddrs overrides this, so we're adding it in explictly again.
		libp2p.EnableRelay(),
	)
	if err != nil {
		log.Printf("Failed to create peer: %v", err)
		return false
	}
	host.SetStreamHandler("/fluence/particle/2.0.0", func(s network.Stream) {
		// log.Printf("Message from peer")

		// End the example
		s.Close()
	})

	particle := Particle{}
	particle.Action = "Particle"
	particle.ID = uuid.New()
	particle.InitPeerId = host.ID()
	particle.Timestamp = Timestamp(time.Now())
	particle.Ttl = 3600
	particle.Script = data
	particle.Signature = []int{}
	particle.Data = []byte{}

	json, err := json.Marshal(particle)

	if err != nil {
		fmt.Printf("Failed to serialize particle", err)
		return false
	}

	if err := host.Connect(ctx, *remoteAddr); err != nil {
		log.Printf("Failed to connect host and relay: %v", err)
		return false
	}
	stream, err := host.NewStream(network.WithUseTransient(ctx, "fluence/particle/2.0.0"), remoteAddr.ID, "/fluence/particle/2.0.0")
	if err != nil {
		log.Printf("Could not open stream: %v", err)
		return false
	}
	defer stream.Close()

	_, err = stream.Write(varint.ToUvarint(uint64(len(json))))
	if err != nil {
		log.Printf("Could not send message: %v", err)
		return false
	}

	_, err = stream.Write(json)
	if err != nil {
		log.Printf("Could not send message: %v", err)
		return false
	}

	stream.Read(make([]byte, 1))
	return true
}
