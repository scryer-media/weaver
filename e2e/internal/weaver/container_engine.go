package weaver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

// commandSkipsContainerPreflight lists subcommands that never touch the
// container engine, so they run on a host without one.
func commandSkipsContainerPreflight(command string) bool {
	switch command {
	case "scenarios", "release-console", "doctor":
		return true
	}
	return false
}

// activateContainerEngine resolves the engine, fails fast on a hard
// incompatibility, installs it for the process and generates the Podman
// Compose overlay. The returned function removes generated secret files.
func activateContainerEngine() (func(), error) {
	engine, err := containerengine.Resolve(context.Background(), containerengine.HostSystem())
	if err != nil {
		return nil, err
	}
	if failed := containerengine.HardFailures(engine.Checks()); len(failed) > 0 {
		lines := make([]string, 0, len(failed))
		for _, check := range failed {
			lines = append(lines, fmt.Sprintf("%s: %s (%s)", check.Name, check.Detail, check.Remedy))
		}
		return nil, fmt.Errorf("container engine %s cannot run this harness (run `%s doctor`):\n  %s",
			engine.Kind, cliProgramName, strings.Join(lines, "\n  "))
	}
	containerengine.Use(engine)
	base := filepath.Join(e2eDir(), "docker-compose.yml")
	overlay, err := engine.WriteOverlay(containerEngineStateDir(), base, os.Getenv)
	if err != nil {
		return nil, err
	}
	engine.SetComposeFiles(base, overlay.Compose)
	return overlay.Remove, nil
}

func containerEngineStateDir() string {
	return filepath.Join(localRunDir(), "container-engine")
}

func cmdDoctor(w io.Writer) error {
	engine, err := containerengine.Resolve(context.Background(), containerengine.HostSystem())
	if err != nil {
		fmt.Fprintf(w, "container engine: FAIL\n  %v\n", err)
		return err
	}
	fmt.Fprintf(w, "container engine: %s (%s)\n", engine.Kind, orDash(engine.Version))
	fmt.Fprintf(w, "  selection:       %s=%s\n", containerengine.EnvVar, orDash(os.Getenv(containerengine.EnvVar)))
	fmt.Fprintf(w, "  compose:         %s compose -> %s %s\n", engine.Binary, engine.ComposeProvider, orDash(engine.ComposeVersion))
	fmt.Fprintf(w, "  rootless:        %t\n", engine.Rootless)
	fmt.Fprintf(w, "  host gateway:    %s (host.docker.internal is mapped to host-gateway by the Compose file)\n", engine.HostGateway())
	checks := engine.Checks()
	for _, check := range checks {
		status := "PASS"
		if !check.OK {
			status = "WARN"
			if check.Hard {
				status = "FAIL"
			}
		}
		fmt.Fprintf(w, "  [%s] %s: %s\n", status, check.Name, check.Detail)
		if !check.OK && check.Remedy != "" {
			fmt.Fprintf(w, "         remedy: %s\n", check.Remedy)
		}
	}
	if failed := containerengine.HardFailures(checks); len(failed) > 0 {
		return errors.New("container engine preflight failed")
	}
	return nil
}

// cmdCompose runs `compose <args>` in the e2e directory through the resolved
// engine. Arguments pass through unchanged; only the engine overlay is added.
func cmdCompose(args []string) error {
	cmd := containerengine.Command(append([]string{"compose"}, args...)...)
	cmd.Dir = e2eDir()
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func orDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func mustActivateContainerEngine() func() {
	cleanup, err := activateContainerEngine()
	if err != nil {
		log.Fatal(err)
	}
	return cleanup
}
