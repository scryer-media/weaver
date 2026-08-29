package weaver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWeaverPlaywrightImageFingerprintTracksBuildContext(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "Dockerfile"), "FROM scratch\n")
	writeFile(t, filepath.Join(root, "package.json"), "{}\n")
	writeFile(t, filepath.Join(root, ".dockerignore"), "node_modules\n")

	initial, err := weaverPlaywrightImageFingerprint(root)
	if err != nil {
		t.Fatalf("initial fingerprint: %v", err)
	}
	writeFile(t, filepath.Join(root, "node_modules", "ignored.js"), "ignored\n")
	ignored, err := weaverPlaywrightImageFingerprint(root)
	if err != nil {
		t.Fatalf("fingerprint with ignored file: %v", err)
	}
	if ignored != initial {
		t.Fatalf("ignored build output changed fingerprint: %s != %s", ignored, initial)
	}

	writeFile(t, filepath.Join(root, "package.json"), "{\"scripts\":{}}\n")
	changed, err := weaverPlaywrightImageFingerprint(root)
	if err != nil {
		t.Fatalf("changed fingerprint: %v", err)
	}
	if changed == initial {
		t.Fatal("Playwright source change did not change image fingerprint")
	}
}

func TestWeaverPlaywrightComposeBuildCarriesFingerprintLabel(t *testing.T) {
	compose, err := os.ReadFile(filepath.Join(e2eDir(), "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	text := string(compose)
	if !strings.Contains(text, weaverPlaywrightImageFingerprintLabel) ||
		!strings.Contains(text, "E2E_WEAVER_PLAYWRIGHT_SOURCE_FINGERPRINT") {
		t.Fatal("weaver-playwright build does not persist its source fingerprint label")
	}
}
