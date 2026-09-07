// Package netcheck answers one question the benchmark asks in three places:
// is this address free for a server this run is about to start.
package netcheck

import (
	"fmt"
	"net"
	"time"
)

// dialTimeout bounds the connection attempt. The addresses checked are local,
// so anything that has not answered by then is not listening.
const dialTimeout = 500 * time.Millisecond

// Available reports whether address can carry a server this run starts. It
// asks twice, because neither question alone is enough.
//
// Listening is the obvious one, but on its own it lies: Go sets SO_REUSEADDR
// on every listener, so binding 127.0.0.1:9090 succeeds while another process
// holds 0.0.0.0:9090 and is still accepting connections there. A published
// Docker port is exactly that shape. The check would pass, the client would
// start, and which of the two servers a connection reaches is then not
// something this benchmark decides.
//
// So it dials first. A connection that is accepted means something is serving
// the address a client is about to be pointed at, whatever the bind would say.
func Available(address string) error {
	connection, err := net.DialTimeout("tcp", address, dialTimeout)
	if err == nil {
		_ = connection.Close()
		return fmt.Errorf("%s already answers connections, so a server started here would not reliably be the one clients reach", address)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("%s cannot be bound: %w", address, err)
	}
	return listener.Close()
}
