// Package nats pkg/transport/nats/errors.go
package nats

import (
	"fmt"

	"github.com/netsampler/goflow2/v3/transport"
)

type TransportError struct {
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("nats transport: %s", e.Err.Error())
}

func (e *TransportError) Unwrap() []error {
	return []error{transport.ErrTransport, e.Err}
}
