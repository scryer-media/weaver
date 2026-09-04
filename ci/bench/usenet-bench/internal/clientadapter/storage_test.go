package clientadapter

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func encodedStorageProfile(t *testing.T, profile benchmark.StorageProfile) string {
	t.Helper()
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestParseStorageProfileFailsClosedOnDrift(t *testing.T) {
	profile, err := benchmark.ResolveStorageProfile(benchmark.StorageProfileNFSComplete, benchmark.NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseStorageProfile(encodedStorageProfile(t, profile))
	if err != nil {
		t.Fatal(err)
	}
	if parsed != profile {
		t.Fatalf("the adapter must reconstruct the plan's exact storage profile, got %#v", parsed)
	}
	tampered := profile
	tampered.RTTMicros = 10
	if _, err := parseStorageProfile(encodedStorageProfile(t, tampered)); err == nil {
		t.Fatal("a storage profile whose fixed values drifted must be rejected")
	}
	if _, err := parseStorageProfile(`{"id":"local","surprise":1}`); err == nil {
		t.Fatal("an unknown storage field must fail closed")
	}
}

func TestStorageValidationMatchesVolumesToTheProfile(t *testing.T) {
	nfsAll, err := benchmark.ResolveStorageProfile(benchmark.StorageProfileNFSAll, benchmark.NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	nfsComplete, err := benchmark.ResolveStorageProfile(benchmark.StorageProfileNFSComplete, benchmark.NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	for name, testCase := range map[string]struct {
		profile              benchmark.StorageProfile
		complete, incomplete string
		valid                bool
	}{
		"local without volumes":  {benchmark.DefaultStorageProfile(), "", "", true},
		"local with a volume":    {benchmark.DefaultStorageProfile(), "vol-complete", "", false},
		"nfs-all with both":      {nfsAll, "vol-complete", "vol-incomplete", true},
		"nfs-all missing one":    {nfsAll, "vol-complete", "", false},
		"nfs-complete with one":  {nfsComplete, "vol-complete", "", true},
		"nfs-complete with both": {nfsComplete, "vol-complete", "vol-incomplete", false},
		"nfs without any":        {nfsComplete, "", "", false},
	} {
		cfg := Config{StorageProfile: testCase.profile, CompleteVolume: testCase.complete, IncompleteVolume: testCase.incomplete}
		err := cfg.validateStorage()
		if testCase.valid && err != nil {
			t.Fatalf("%s should validate: %v", name, err)
		}
		if !testCase.valid && err == nil {
			t.Fatalf("%s should not validate", name)
		}
	}
}

func TestDownloadMountsFollowTheStorageProfile(t *testing.T) {
	cfg := Config{OutputDir: "/scratch/complete"}
	mounts := downloadMounts(cfg, "/scratch/incomplete")
	if mounts[0] != "type=bind,src=/scratch/incomplete,dst=/downloads/incomplete" ||
		mounts[1] != "type=bind,src=/scratch/complete,dst=/downloads/complete" {
		t.Fatalf("local runs must keep both directories on host binds: %v", mounts)
	}

	cfg.CompleteVolume = "nntpbench-run-0001-complete-abcdef12"
	mounts = downloadMounts(cfg, "/scratch/incomplete")
	if mounts[0] != "type=bind,src=/scratch/incomplete,dst=/downloads/incomplete" {
		t.Fatalf("nfs-complete must keep the intermediate directory local: %v", mounts)
	}
	if mounts[1] != "type=volume,src=nntpbench-run-0001-complete-abcdef12,dst=/downloads/complete" {
		t.Fatalf("nfs-complete must place only the completion directory on the export: %v", mounts)
	}

	cfg.IncompleteVolume = "nntpbench-run-0001-incomplete-abcdef12"
	mounts = downloadMounts(cfg, "/scratch/incomplete")
	if mounts[0] != "type=volume,src=nntpbench-run-0001-incomplete-abcdef12,dst=/downloads/incomplete" {
		t.Fatalf("nfs-all must place the intermediate directory on the export too: %v", mounts)
	}
	// The container paths never change, so no product can tell which storage
	// profile it is running under from its own configuration.
	for _, mount := range mounts {
		if !strings.Contains(mount, "dst=/downloads/") {
			t.Fatalf("the client's container paths must be identical across profiles: %v", mounts)
		}
	}
}

func TestAuditConfigRecordsEveryStorageSetting(t *testing.T) {
	profile, err := benchmark.ResolveStorageProfile(benchmark.StorageProfileNFSAll, benchmark.NFSLink2500Mbit)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t, benchmark.Weaver, benchmark.Plaintext, benchmark.TLSNotApplicable)
	cfg.StorageProfile = profile
	spec, err := cfg.RenderProductConfig()
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(spec.Rendered)
	for _, expected := range []string{
		"schema_version=2",
		"storage_profile_id=nfs-all",
		"storage_kind=nfs",
		"storage_nfs_link_id=nas-2.5gbit",
		"storage_intermediate_on_nfs=true",
		"storage_complete_on_nfs=true",
		"storage_link_bits_per_second=2500000000",
		"storage_link_burst_bytes=2097152",
		"storage_rtt_micros=1000",
		"storage_mount_options=" + profile.MountOptions,
		"storage_export_options=" + profile.ExportOptions,
		"storage_shaper=tbf+netem",
		"storage_attestation_scope=nfs_server_link",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("audit config lacks %q:\n%s", expected, rendered)
		}
	}
}
