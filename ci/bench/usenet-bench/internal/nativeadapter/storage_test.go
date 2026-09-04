package nativeadapter

import (
	"strings"
	"testing"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func TestNativeAdapterRejectsNFSStorageProfiles(t *testing.T) {
	for _, id := range []string{benchmark.StorageProfileNFSAll, benchmark.StorageProfileNFSComplete} {
		profile, err := benchmark.ResolveStorageProfile(id, benchmark.NFSLink1Gbit)
		if err != nil {
			t.Fatal(err)
		}
		cfg := testConfig(benchmark.Weaver)
		cfg.StorageProfile = profile
		err = cfg.Validate()
		if err == nil {
			t.Fatalf("the native lane must refuse storage profile %q", id)
		}
		// Mounting an export would need the operator's own kernel to mount it
		// as root, so the refusal names the only profile this lane supports.
		if !strings.Contains(err.Error(), benchmark.StorageProfileLocal) {
			t.Fatalf("the refusal should name the supported profile, got %v", err)
		}
	}
}

func TestNativeAdapterRejectsATamperedLocalProfile(t *testing.T) {
	cfg := testConfig(benchmark.Weaver)
	cfg.StorageProfile.LinkBitsPerSecond = 1
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "storage profile") {
		t.Fatalf("a local profile carrying a link rate should be refused as a storage error, got %v", err)
	}
}

func TestNativeAuditConfigRecordsTheStorageProfile(t *testing.T) {
	cfg := testConfig(benchmark.Weaver)
	spec, err := renderProduct(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rendered := string(spec.Rendered)
	for _, expected := range []string{
		"schema_version=2",
		"storage_profile_id=local",
		"storage_kind=local",
		"storage_nfs_link_id=none",
		"storage_shaper=none",
		"storage_attestation_scope=none",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("native audit config lacks %q:\n%s", expected, rendered)
		}
	}
}
