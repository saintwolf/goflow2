package nats

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/netsampler/goflow2/v3/transport"
)

// NatsDriver implements the GoFlow2 transport interface for NATS Core.
type NatsDriver struct {
	natsURL     string
	subject     string
	// Stream name is not used in NATS Core
	tlsCertFile string
	tlsKeyFile  string
	tlsCAFile   string
	tlsInsecure bool
	nc          *nats.Conn
}

// Prepare sets up command-line flags for the NATS transport.
func (d *NatsDriver) Prepare() error {
	flag.StringVar(&d.natsURL, "transport.nats.url", "nats://localhost:4222", "NATS server URL")
	// Removed transport.nats.stream flag as it does not apply to Core NATS
	flag.StringVar(&d.subject, "transport.nats.subject", "goflow2.messages", "NATS subject for publishing messages")
	flag.StringVar(&d.tlsCertFile, "transport.nats.tls.cert", "", "NATS client certificate file")
	flag.StringVar(&d.tlsKeyFile, "transport.nats.tls.key", "", "NATS client key file")
	flag.StringVar(&d.tlsCAFile, "transport.nats.tls.ca", "", "NATS CA certificate file")
	flag.BoolVar(&d.tlsInsecure, "transport.nats.tls.insecure", false, "Skip TLS verification for NATS")

	return nil
}

// Init initializes the NATS connection.
func (d *NatsDriver) Init() error {
	// 1. Determine if we should use TLS
	// We use TLS if the URL scheme is 'tls://' OR if certificates are provided.
	useTLS := strings.HasPrefix(d.natsURL, "tls://") || d.tlsCertFile != "" || d.tlsKeyFile != ""

	// 2. Validate Certificates (Only if we are actually using TLS)
	if useTLS && (d.tlsCertFile != "" || d.tlsKeyFile != "") {
		if d.tlsCertFile == "" || d.tlsKeyFile == "" {
			return &TransportError{Err: fmt.Errorf("both tls.cert and tls.key are required for mTLS")}
		}
	}

	// 3. Configure Connection Options
	opts := []nats.Option{
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Printf("NATS error: %v", err)
		}),
		nats.ConnectHandler(func(nc *nats.Conn) {
			log.Printf("Connected to NATS: %s", nc.ConnectedUrl())
		}),
	}

	// Only add Secure() options if TLS is actually required
	if useTLS {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: d.tlsInsecure,
		}

		if d.tlsCertFile != "" && d.tlsKeyFile != "" {
			opts = append(opts, nats.ClientCert(d.tlsCertFile, d.tlsKeyFile))
		}

		if d.tlsCAFile != "" {
			opts = append(opts, nats.RootCAs(d.tlsCAFile))
		}

		opts = append(opts, nats.Secure(tlsConfig))
	}

	// 4. Connect
	// If useTLS is false, this connects via plain TCP
	nc, err := nats.Connect(d.natsURL, opts...)
	if err != nil {
		return &TransportError{Err: fmt.Errorf("failed to connect to NATS: %w", err)}
	}

	d.nc = nc

	// JetStream initialization logic has been removed.
	// NATS Core is ready to use immediately after connection.

	return nil
}

// Send publishes a protobuf message to the NATS subject.
func (d *NatsDriver) Send(_, data []byte) error {
	if d.nc == nil {
		return &TransportError{Err: fmt.Errorf("NATS connection not initialized")}
	}

	// NATS Core Publish does not use Context and is "fire and forget" by default,
	// but it will return an error if the connection is closed or buffer is full.
	err := d.nc.Publish(d.subject, data)
	if err != nil {
		return &TransportError{Err: fmt.Errorf("failed to publish message: %w", err)}
	}

	return nil
}

// Close gracefully shuts down the NATS connection.
func (d *NatsDriver) Close() error {
	if d.nc != nil {
		err := d.nc.Drain()
		if err != nil {
			log.Printf("Error draining NATS connection: %v", err)
			return err
		}
		d.nc.Close()
		log.Println("NATS connection closed")
	}
	return nil
}

func init() {
	d := &NatsDriver{}
	transport.RegisterTransportDriver("nats", d)
}