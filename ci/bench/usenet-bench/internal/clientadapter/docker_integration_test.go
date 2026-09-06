package clientadapter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// TestProductImagesReachAPI is intentionally opt-in: it validates that the
// pinned images accept the renderer's clean configuration without needing an
// NNTP server or downloading images during a normal unit-test run.
func TestProductImagesReachAPI(t *testing.T) {
	if os.Getenv("NNTP_BENCH_INTEGRATION") == "" {
		t.Skip("set NNTP_BENCH_INTEGRATION=1 to start the locally available client images")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker is unavailable")
	}
	cases := []struct {
		client benchmark.Client
		image  string
	}{
		{benchmark.Weaver, "ghcr.io/scryer-media/weaver@sha256:7e693e201efbd4876fffc346872e17b1eb0eef0e6f761b6931d9207154cc9b71"},
		{benchmark.SABnzbd, "lscr.io/linuxserver/sabnzbd@sha256:64c4c2b6ed546237451cbfec33aa8bac1396865c1a266dd247c02b36ffe27c62"},
		{benchmark.NZBGet, "lscr.io/linuxserver/nzbget@sha256:4cc4afc944e0239037b7b95c64deb8e39e49ca770b7e2f61476a04443d7fcab9"},
	}
	for _, testCase := range cases {
		t.Run(string(testCase.client), func(t *testing.T) {
			if err := exec.Command("docker", "image", "inspect", testCase.image).Run(); err != nil {
				t.Skipf("image is not present locally: %s", testCase.image)
			}
			cfg := testConfig(t, testCase.client, benchmark.Plaintext, benchmark.TLSNotApplicable)
			cfg.Image = testCase.image
			cfg.Network = "bridge"
			cfg.StartupTimeout = 90 * time.Second
			cfg.PollInterval = 500 * time.Millisecond
			spec, err := cfg.RenderProductConfig()
			if err != nil {
				t.Fatal(err)
			}
			if testCase.client == benchmark.Weaver {
				// The benchmark itself invokes Weaver's one-shot CLI. This smoke
				// test intentionally restores the image's default serve command so
				// the shared API-readiness probe remains useful for all products.
				spec.ExposeAPI = true
				spec.APIPort = 9090
				spec.Command = nil
				spec.NeedsNZBMount = false
				spec.CompletionReportName = ""
				spec.CompletionAckName = ""
			}
			if err := writeProductFiles(cfg, spec); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			container, err := startContainer(ctx, cfg, spec)
			if err != nil {
				t.Fatal(err)
			}
			defer container.cleanup()
			if err := container.resolveEndpoint(ctx, spec.APIPort); err != nil {
				t.Fatal(err)
			}
			api, err := newProductAPI(cfg, container.endpoint)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := waitUntilContainerReady(ctx, cfg.PollInterval, api, container); err != nil {
				logPath := filepath.Join(filepath.Dir(cfg.ConfigDir), "client-container.log")
				t.Fatalf("client did not become ready: %v (container log will be saved at %s)", err, logPath)
			}
		})
	}
}
