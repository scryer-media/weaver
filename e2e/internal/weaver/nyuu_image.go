package weaver

import (
	"fmt"
	"os/exec"
	"sync"
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
		cmd := exec.Command("docker", dockerComposeArgs("build", "nyuu")...)
		cmd.Dir = e2eDir()
		sharedNyuuBuildErr = runExternalCommand(cmd, "docker compose build")
	})
	if sharedNyuuBuildErr != nil {
		return fmt.Errorf("build nyuu image: %w", sharedNyuuBuildErr)
	}
	return nil
}
