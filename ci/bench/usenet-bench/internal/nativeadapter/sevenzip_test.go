package nativeadapter

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// The corpus now contains 7z fixtures, so a native NZBGet run states which
// 7-Zip binary it expects instead of relying on the built-in default.
func TestNativeNZBGetNamesItsSevenZipBinary(t *testing.T) {
	spec, err := renderProduct(testConfig(benchmark.NZBGet))
	if err != nil {
		t.Fatal(err)
	}
	named := false
	for _, name := range NZBGetSevenZipNames {
		if strings.Contains(string(spec.Content), "SevenZipCmd="+name) || strings.Contains(string(spec.Content), string(filepath.Separator)+name+"\n") {
			named = true
			break
		}
	}
	if !named {
		t.Fatalf("native NZBGet config does not name a 7-Zip binary:\n%s", spec.Content)
	}
}

func TestNativeSevenZipSettingIsNZBGetOnly(t *testing.T) {
	for _, client := range []benchmark.Client{benchmark.SABnzbd, benchmark.Weaver} {
		spec, err := renderProduct(testConfig(client))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(spec.Content), "SevenZipCmd=") {
			t.Fatalf("%s config picked up an NZBGet-only setting", client)
		}
	}
}
