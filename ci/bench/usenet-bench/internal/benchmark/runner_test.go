package benchmark

import (
	"strings"
	"testing"
	"time"
)

func TestAdapterCatalogRequiresPlannedClients(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver, SABnzbd, NZBGet},
		Transports:  []Transport{Plaintext},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog := AdapterCatalog{SchemaVersion: 4, Adapters: []Adapter{{Client: Weaver, ArchiveToolchain: VanillaArchiveToolchain, Target: DockerLinux, Command: []string{"weaver-adapter"}}}}
	if err := catalog.ValidateFor(plan, DockerLinux); err == nil {
		t.Fatal("catalog missing clients should fail")
	}
}

func TestAdapterResultMustMatchTLSPlanMetadata(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{SABnzbd},
		Transports:  []Transport{TLS},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	run := plan.Runs[0]
	now := time.Now().UTC()
	result := AdapterResult{
		SchemaVersion:            6,
		RunID:                    run.ID,
		Client:                   run.Client,
		ArchiveToolchain:         run.ArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          run.ExecutionTarget,
		Transport:                run.Transport,
		TLSValidation:            TLSCAVerified,
		TransportLabel:           "tls-ca-verified",
		ServerLink:               run.ServerLink,
		StorageProfile:           run.StorageProfile,
		QueuedAt:                 now,
		CompletionAt:             now.Add(time.Second),
		ClientIdentity:           "sha256:test",
		ClientVersion:            "test",
		RenderedConfigSHA256:     "0123456789012345678901234567890123456789012345678901234567890123",
		ResourceMetrics: ResourceMetrics{
			CPUTimeNanoseconds:  UnavailableMeasurement("client_container", "test", "1", "not collected in unit test"),
			InstructionsRetired: UnavailableMeasurement("client_process", "test", "1", "not collected in unit test"),
		},
	}
	if err := result.ValidateFor(run); err == nil {
		t.Fatal("SAB verified TLS result should not satisfy unverified planned metadata")
	}
}

func TestAdapterResultRequiresExplicitResourceCounterOutcomes(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Targets:     []ExecutionTarget{DockerLinux},
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	run := plan.Runs[0]
	now := time.Now().UTC()
	result := AdapterResult{
		SchemaVersion:            6,
		RunID:                    run.ID,
		Client:                   run.Client,
		ArchiveToolchain:         run.ArchiveToolchain,
		ArchiveToolchainIdentity: "stock",
		ExecutionTarget:          run.ExecutionTarget,
		Transport:                run.Transport,
		TLSValidation:            run.TLSValidation,
		TransportLabel:           run.TransportLabel,
		ServerLink:               run.ServerLink,
		StorageProfile:           run.StorageProfile,
		QueuedAt:                 now,
		CompletionAt:             now.Add(time.Second),
		ClientIdentity:           "sha256:test",
		ClientVersion:            "test",
		RenderedConfigSHA256:     "0123456789012345678901234567890123456789012345678901234567890123",
	}
	if err := result.ValidateFor(run); err == nil {
		t.Fatal("missing resource measurements should not validate")
	}
	result.ResourceMetrics = ResourceMetrics{
		CPUTimeNanoseconds:  MeasuredMeasurement("client_container", "test", "1", 100),
		InstructionsRetired: UnavailableMeasurement("client_process", "test", "1", "hardware counter not exposed"),
	}
	if err := result.ValidateFor(run); err != nil {
		t.Fatalf("explicit unavailable instructions counter should validate: %v", err)
	}
}

func TestRunConfigRejectsProfileDifferentFromPlan(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Profile:     ProfileStock,
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	config := RunConfig{
		Plan:         plan,
		Catalog:      AdapterCatalog{SchemaVersion: 4, Adapters: []Adapter{{Client: Weaver, ArchiveToolchain: VanillaArchiveToolchain, Target: DockerLinux, Command: []string{"weaver-adapter"}}}},
		Target:       DockerLinux,
		FixtureRoot:  "/fixtures",
		ArtifactRoot: "/artifacts",
		NNTPHost:     "nntp",
		NNTPUsername: "user",
		NNTPPassword: "password",
		Profile:      ProfileEquivalentThroughput,
		Connections:  8,
		Timeout:      time.Minute,
	}
	if err := config.Validate(); err == nil {
		t.Fatal("profile mismatch should not validate")
	}
}

func TestRunArtifactWriteFailurePropagates(t *testing.T) {
	artifact := RunArtifact{Status: "passed"}
	persistRunArtifact(t.TempDir(), &artifact)
	if artifact.Status != "failed" || !strings.Contains(artifact.Error, "write run artifact") {
		t.Fatalf("artifact write failure was not propagated: %#v", artifact)
	}
}
