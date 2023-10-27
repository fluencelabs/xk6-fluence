package fluence

import (
	"crypto/ed25519"
	"encoding/base64"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSignature(t *testing.T) {
	privKeyBytes, err := base64.StdEncoding.DecodeString("7h48PQ/f1rS9TxacmgODxbD42Il9B3KC117jvOPppPE=")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	edPrvKey := ed25519.NewKeyFromSeed(privKeyBytes)

	prvKey, err := crypto.UnmarshalEd25519PrivateKey([]byte(edPrvKey))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	peerId, err := peer.IDFromPublicKey(prvKey.GetPublic())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	particle := Particle{}
	particle.Action = "Particle"
	particle.ID = "2883f959-e9e7-4843-8c37-205d393ca372"
	particle.InitPeerId = peerId
	particle.Timestamp = timestamp(time.UnixMilli(1696934545662))
	particle.Ttl = 7000
	particle.Script = "abc"
	particle.Data = []byte{}

	res, err := serialiseSignatireData(particle)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	signature, err := prvKey.Sign(res)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	expectedResult, err := base64.StdEncoding.DecodeString("Mjg4M2Y5NTktZTllNy00ODQzLThjMzctMjA1ZDM5M2NhMzcy/kguGYsBAABYGwAAYWJj")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	expectedSignature, err := base64.StdEncoding.DecodeString("KceXDnOfqe0dOnAxiDsyWBIvUq6WHoT0ge+VMHXOZsjZvCNH7/10oufdlYfcPomfv28On6E87ZhDcHGBZcb7Bw==")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
		t.Fail()
	}

	assert.Equal(t, res, expectedResult)
	assert.Equal(t, signature, expectedSignature)
}
