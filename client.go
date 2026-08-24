package rabbitjetstream

import (
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	jsapi "github.com/nats-io/nats.go/jetstream"
)

type Client struct {
	nc      *nats.Conn
	js      nats.JetStreamContext
	jsapi   jsapi.JetStream
	owned   bool
	closeMu sync.Mutex
	closed  bool
}

// Connect creates and owns a NATS connection. Pass standard nats.Options for
// credentials, TLS, reconnect policy and callbacks.
func Connect(url string, options ...nats.Option) (*Client, error) {
	nc, err := nats.Connect(url, options...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	client, err := New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	client.owned = true
	return client, nil
}

// New uses a caller-owned NATS connection.
func New(nc *nats.Conn, options ...nats.JSOpt) (*Client, error) {
	if nc == nil {
		return nil, fmt.Errorf("NATS connection is required")
	}
	js, err := nc.JetStream(options...)
	if err != nil {
		return nil, fmt.Errorf("open JetStream context: %w", err)
	}
	modern, err := jsapi.New(nc)
	if err != nil {
		return nil, fmt.Errorf("open modern JetStream context: %w", err)
	}
	return &Client{nc: nc, js: js, jsapi: modern}, nil
}

func (c *Client) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.owned {
		if err := c.nc.Drain(); err != nil {
			c.nc.Close()
			return fmt.Errorf("drain NATS connection: %w", err)
		}
		c.nc.Close()
	}
	return nil
}

func (c *Client) checkOpen() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed || c.nc.IsClosed() {
		return ErrClosed
	}
	return nil
}
