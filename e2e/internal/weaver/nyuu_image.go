package weaver

import (
	"fmt"
	"sync"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

var (
	sharedNyuuBuildOnce sync.Once
	sharedNyuuBuildErr  error
)

func ensureNyuuImageBuilt() error {
	sharedNyuuBuildOnce.Do(func() {
		image := nyuuImage()
		if !envBool("E2E_FORCE_REBUILD_NYUU_IMAGE", false) && dockerImageExists(image) {
			return
		}
		cmd := containerengine.Command(dockerComposeArgs("build", "nyuu")...)
		cmd.Dir = e2eDir()
		sharedNyuuBuildErr = runExternalCommand(cmd, "docker compose build")
	})
	if sharedNyuuBuildErr != nil {
		return fmt.Errorf("build nyuu image: %w", sharedNyuuBuildErr)
	}
	return nil
}
