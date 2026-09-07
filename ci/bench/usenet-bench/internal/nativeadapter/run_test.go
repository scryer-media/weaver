package nativeadapter

import (
	"net"
	"testing"
)

// SABnzbd does not refuse a busy web port -- it moves to the next free one and
// rewrites its own ini. The adapter then polls the original port and reads
// whatever stranger answers, which nothing downstream can detect. The launch
// has to be refused instead.
func TestTheAdapterRefusesToLaunchOntoAnOccupiedAPIPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	endpoint := "http://" + listener.Addr().String()
	if err := checkAPIPortFree(endpoint); err == nil {
		t.Fatal("accepted a port something else is listening on")
	}

	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := checkAPIPortFree("http://" + address); err != nil {
		t.Fatalf("refused a free port: %v", err)
	}
}
