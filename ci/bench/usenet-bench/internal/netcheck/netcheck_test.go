package netcheck

import (
	"net"
	"testing"
)

func TestAFreeAddressIsAvailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Available(address); err != nil {
		t.Fatalf("a free address was refused: %v", err)
	}
}

func TestAnAddressSomethingAnswersOnIsRefused(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	if err := Available(listener.Addr().String()); err == nil {
		t.Fatal("an address with a live listener was reported free")
	}
}

// The case a bind test alone gets wrong: a wildcard listener elsewhere in the
// address space still serves the specific address a client will be given, and
// SO_REUSEADDR lets a second bind succeed anyway.
func TestAWildcardListenerCoversTheSpecificAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := Available(net.JoinHostPort("127.0.0.1", port)); err == nil {
		t.Fatal("a loopback address covered by a wildcard listener was reported free")
	}
}
