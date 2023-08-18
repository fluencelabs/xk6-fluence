package fluence

import "fmt"

type errCode uint32

const (
	// non specific.
	wrongRelayAddress errCode = 1000
	connectionFailed  errCode = 1001
	sendFailed        errCode = 1002
)

var (
	WrongRelayAddress = NewXk6Error(wrongRelayAddress, "Wrong multi-address format", nil)
	ConnectionFailed  = NewXk6Error(connectionFailed, "Failed to connect to the remote peer", nil)
	SendFailed        = NewXk6Error(sendFailed, "Could not send particle", nil)
)

type Xk6Error struct {
	Code          errCode
	Message       string
	OriginalError error
}

func NewXk6Error(code errCode, msg string, originalErr error) *Xk6Error {
	return &Xk6Error{Code: code, Message: msg, OriginalError: originalErr}
}

func (e Xk6Error) Error() string {
	if e.OriginalError == nil {
		return e.Message
	}
	return fmt.Sprintf(e.Message+", OriginalError: %w", e.OriginalError)
}

func (e Xk6Error) Unwrap() error {
	return e.OriginalError
}
