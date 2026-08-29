package composeutil

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSelectNonOverlappingSubnets(t *testing.T) {
	got, err := SelectNonOverlappingSubnets(
		2,
		[]string{"10.250.1.0/24", "10.250.2.0/24", "10.250.3.0/24"},
		[]string{"10.250.2.0/24"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.250.1.0/24", "10.250.3.0/24"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected subnets = %v, want %v", got, want)
	}
}

func TestWriteNetworkOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "network.yml")
	if err := WriteNetworkOverride(path, "10.250.9.0/24"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `subnet: "10.250.9.0/24"`) {
		t.Fatalf("network override = %s", body)
	}
}
