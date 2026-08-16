package weaver

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const (
	weaverNNTPDefaultImage = "weaver-e2e-nntp:local"
	weaverNNTPPreparedEnv  = "E2E_WEAVER_NNTP_IMAGE_PREPARED"
	// weaverNNTPDefaultModuleVersion is the published e2e-nntp release the
	// harness builds its NNTP image from when E2E_NNTP_MODULE_VERSION is unset.
	// It is fetched as a public Go module; nothing here depends on a checkout
	// sitting anywhere on the machine.
	weaverNNTPDefaultModuleVersion = "v0.1.0"
)

// weaverNNTPModuleVersion is the pinned e2e-nntp module version in effect.
func weaverNNTPModuleVersion() string {
	if version := strings.TrimSpace(os.Getenv("E2E_NNTP_MODULE_VERSION")); version != "" {
		return version
	}
	return weaverNNTPDefaultModuleVersion
}

var weaverNNTPImageMu sync.Mutex

func ensureLocalWeaverNNTPImage() error {
	weaverNNTPImageMu.Lock()
	defer weaverNNTPImageMu.Unlock()
	if envBool(weaverNNTPPreparedEnv, false) {
		return nil
	}

	image := strings.TrimSpace(os.Getenv("E2E_NNTP_IMAGE"))
	if image != "" && image != weaverNNTPDefaultImage {
		setEnv(weaverNNTPPreparedEnv, "1")
		return nil
	}
	if image == "" {
		image = weaverNNTPDefaultImage
		setEnv("E2E_NNTP_IMAGE", image)
	}

	version := weaverNNTPModuleVersion()
	sourceDir := strings.TrimSpace(os.Getenv("E2E_NNTP_SOURCE_DIR"))
	force := envBool("E2E_FORCE_REBUILD_NNTP_IMAGE", false) ||
		envBool("E2E_FORCE_REBUILD_E2E_INFRA_IMAGES", false)
	// A pinned module version is reproducible, so an existing local image can
	// be reused; a source-directory build is a developer override whose bytes
	// can change between runs, so it is always rebuilt.
	if !force && sourceDir == "" && dockerImageExists(image) {
		setEnv(weaverNNTPPreparedEnv, "1")
		log.Printf("reusing pinned Weaver NNTP image: %s (%s)", image, version)
		return nil
	}

	cmd, err := weaverNNTPImageBuildCommand(image)
	if err != nil {
		return err
	}
	if err := runExternalCommand(cmd, "build Weaver NNTP fixture image from source"); err != nil {
		return fmt.Errorf("build current Weaver NNTP fixture image: %w", err)
	}
	setEnv(weaverNNTPPreparedEnv, "1")
	log.Printf("built current Weaver NNTP fixture image: %s", image)
	return nil
}

func weaverNNTPImageBuildCommand(image string) (*exec.Cmd, error) {
	var arguments []string
	if sourceDir := strings.TrimSpace(os.Getenv("E2E_NNTP_SOURCE_DIR")); sourceDir != "" {
		// Explicit developer override only: build the image from a local
		// e2e-nntp working tree. The harness never guesses where such a tree
		// might be; it must be named.
		absoluteSource, err := filepath.Abs(sourceDir)
		if err != nil {
			return nil, fmt.Errorf("resolve E2E_NNTP_SOURCE_DIR: %w", err)
		}
		if _, err := os.Stat(filepath.Join(absoluteSource, "go.mod")); err != nil {
			return nil, fmt.Errorf("E2E_NNTP_SOURCE_DIR %s is not a Go module root: %w", absoluteSource, err)
		}
		arguments = []string{
			"-C", absoluteSource, "run", "./cmd/e2e-nntp",
			"image", "build", "--source-dir", absoluteSource,
		}
	} else {
		version := weaverNNTPModuleVersion()
		arguments = []string{
			"run", "github.com/scryer-media/e2e-nntp/cmd/e2e-nntp@" + version,
			"image", "build", "--version", version,
		}
	}
	arguments = append(arguments, "--tag", image)
	cmd := exec.Command("go", arguments...)
	cmd.Dir = e2eDir()
	return cmd, nil
}
