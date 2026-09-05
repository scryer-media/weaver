package clientadapter

import (
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// The corpus now contains 7z fixtures, so NZBGet's external 7-Zip tool has to
// be named explicitly rather than left to whatever the image's PATH resolves.
func TestNZBGetNamesThePinnedImageSevenZipBinary(t *testing.T) {
	for _, transport := range []struct {
		transport  benchmark.Transport
		validation benchmark.TLSValidation
	}{
		{benchmark.Plaintext, benchmark.TLSNotApplicable},
		{benchmark.TLS, benchmark.TLSCAVerified},
	} {
		cfg := testConfig(t, benchmark.NZBGet, transport.transport, transport.validation)
		spec, err := cfg.RenderProductConfig()
		if err != nil {
			t.Fatal(err)
		}
		content := string(spec.ConfigContent)
		if !strings.Contains(content, "SevenZipCmd=/usr/bin/7zz") {
			t.Fatalf("NZBGet config does not name the image's 7-Zip build:\n%s", content)
		}
	}
}

func TestOnlyNZBGetCarriesTheSevenZipSetting(t *testing.T) {
	for _, client := range []benchmark.Client{benchmark.SABnzbd, benchmark.Weaver} {
		cfg := testConfig(t, client, benchmark.Plaintext, benchmark.TLSNotApplicable)
		spec, err := cfg.RenderProductConfig()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(spec.ConfigContent), "SevenZipCmd=") {
			t.Fatalf("%s config picked up an NZBGet-only setting", client)
		}
	}
}
