package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

// storageEnv writes the immutable environment file the shaped NFS server
// container consumes. It is the storage counterpart of server-env: the same
// declared profile that goes into the plan configures the container, so a run
// can never be shaped differently from the way it is reported.
func storageEnv(args []string) error {
	flags := flag.NewFlagSet("storage-env", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var profileID, nfsLink, output string
	flags.StringVar(&profileID, "storage-profile", "", "storage profile: nfs-all or nfs-complete")
	flags.StringVar(&nfsLink, "nfs-link", "", "NFS link profile: nas-100mbit, nas-1gbit, or nas-2.5gbit")
	flags.StringVar(&output, "output", "", "new Compose-compatible environment file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if output == "" {
		return fmt.Errorf("--output is required")
	}
	profile, err := benchmark.ResolveStorageProfile(profileID, nfsLink)
	if err != nil {
		return err
	}
	return benchmark.WriteStorageLinkEnvironment(output, profile)
}

// deleteOutput removes a verified completion directory's contents. The
// controller uses it inside the NFS helper container, where the export is only
// reachable through a mount the harness itself made; it is the same deletion
// the local lane performs in process.
func deleteOutput(args []string) error {
	flags := flag.NewFlagSet("delete-output", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var outputDir string
	flags.StringVar(&outputDir, "output-dir", "", "client completion directory to empty")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if outputDir == "" {
		return fmt.Errorf("--output-dir is required")
	}
	return benchmark.DeleteOutputFiles(outputDir)
}

// resolveStoragePlanProfile turns the plan command's two storage flags into
// one complete, serializable profile.
func resolveStoragePlanProfile(profileID, nfsLink string) (benchmark.StorageProfile, error) {
	return benchmark.ResolveStorageProfile(profileID, nfsLink)
}

// printStorageAttestations writes one line per shaped-storage suite so an
// operator watching a long run can see immediately that the export was
// throttled and carried the client's bytes.
func printStorageAttestations(artifacts []benchmark.QueueArtifact) {
	for _, artifact := range artifacts {
		if artifact.StorageAttestation == nil {
			continue
		}
		fmt.Fprintf(os.Stderr, "nntpbench: %s %s\n", artifact.SuiteID, artifact.StorageAttestation.Summary())
	}
}

func printRunStorageAttestations(artifacts []benchmark.RunArtifact) {
	for _, artifact := range artifacts {
		if artifact.StorageAttestation == nil {
			continue
		}
		fmt.Fprintf(os.Stderr, "nntpbench: %s %s\n", artifact.Run.ID, artifact.StorageAttestation.Summary())
	}
}
