package main

import (
	"strings"
	"testing"
	"time"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/benchmark"
)

func nfsStorageProfile(t *testing.T) benchmark.StorageProfile {
	t.Helper()
	profile, err := benchmark.ResolveStorageProfile(benchmark.StorageProfileNFSComplete, benchmark.NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

// storageAttestationFor builds the evidence a completed NFS run publishes. It
// is assembled from parsed counter values rather than raw tc text because the
// parser has its own tests; what matters here is that the summarizer refuses
// anything this shape cannot support.
func storageAttestationFor(profile benchmark.StorageProfile) *benchmark.StorageAttestation {
	opened := time.Now().UTC().Add(-time.Minute)
	counters := func(at time.Time, egress, ingress, read, written uint64) benchmark.StorageCounters {
		return benchmark.StorageCounters{
			CapturedAt:         at,
			EgressBytes:        egress,
			IngressBytes:       ingress,
			ServerReadBytes:    read,
			ServerWrittenBytes: written,
			EgressRateBits:     profile.LinkBitsPerSecond,
			EgressDelayMicros:  profile.OneWayDelayMicros(),
			IngressRateBits:    profile.LinkBitsPerSecond,
			IngressDelayMicros: profile.OneWayDelayMicros(),
		}
	}
	before := counters(opened, 1_000, 500, 1_000, 500)
	after := counters(opened.Add(30*time.Second), 5_368_710_120, 1_073_742_324, 5_368_710_120, 1_073_742_324)
	return &benchmark.StorageAttestation{
		SchemaVersion:    1,
		Profile:          profile,
		Container:        "bench-nfs",
		Network:          "bench_storage",
		ServerAddress:    "172.31.0.2",
		ExportDevice:     ":/queue-0001-abcdef12/complete",
		ExportOptionsRaw: "/export <world>(no_subtree_check,fsid=0,rw,insecure,no_root_squash)",
		ClientMountLine:  "172.31.0.2:/queue-0001-abcdef12/complete /mnt/complete nfs4 rw,vers=4.1 0 0",
		HelperImage:      benchmark.DefaultNFSImage,
		LeaseID:          strings.Repeat("0f", 32),
		LeaseAcquiredAt:  opened,
		Shaper: benchmark.StorageShaperReport{
			SchemaVersion:     1,
			Interface:         "eth0",
			IngressDevice:     "ifb-nfs",
			EgressMechanism:   "tbf+netem",
			IngressMechanism:  "ifb-tbf+netem",
			LinkBitsPerSecond: profile.LinkBitsPerSecond,
			LinkBurstBytes:    profile.LinkBurstBytes,
			RTTMicros:         profile.RTTMicros,
			OneWayDelayMicros: profile.OneWayDelayMicros(),
			ExportOptions:     profile.ExportOptions,
			NFSVersions:       "-2 -3 +4 +4.1 +4.2",
			KernelRelease:     "6.1.0-test",
		},
		Before:               before,
		After:                after,
		EgressBytes:          after.EgressBytes - before.EgressBytes,
		IngressBytes:         after.IngressBytes - before.IngressBytes,
		ServerReadBytes:      after.ServerReadBytes - before.ServerReadBytes,
		ServerWrittenBytes:   after.ServerWrittenBytes - before.ServerWrittenBytes,
		VerificationStrategy: "helper_container_server_side_harness_verify_output",
		CPUAccountingCaveat:  "cpu_time excludes NFS client kernel time for the nfs storage profiles",
	}
}

func storageSummaryArtifact(t *testing.T, client benchmark.Client, repetition int, measurement int64, profile benchmark.StorageProfile) benchmark.QueueArtifact {
	t.Helper()
	artifact := summaryTestArtifact(client, repetition, measurement)
	artifact.Runs[0].StorageProfile = profile
	artifact.Jobs[0].Run.StorageProfile = profile
	artifact.AdapterResult.StorageProfile = profile
	if profile.Kind == benchmark.StorageNFS {
		artifact.StorageAttestation = storageAttestationFor(profile)
	}
	return artifact
}

func storageSummarySet(t *testing.T, profile benchmark.StorageProfile) []benchmark.QueueArtifact {
	t.Helper()
	artifacts := make([]benchmark.QueueArtifact, 0, 40)
	for repetition := 1; repetition <= 20; repetition++ {
		artifacts = append(artifacts,
			storageSummaryArtifact(t, benchmark.Weaver, repetition, int64(100+repetition), profile),
			storageSummaryArtifact(t, benchmark.SABnzbd, repetition, int64(80+repetition), profile),
		)
	}
	return artifacts
}

func TestSummaryStratumCarriesTheStorageProfile(t *testing.T) {
	profile := nfsStorageProfile(t)
	report, err := buildSummaryReport(storageSummarySet(t, profile), nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Comparisons) != 1 {
		t.Fatalf("expected one shaped-storage stratum, got %d", len(report.Comparisons))
	}
	stratum := report.Comparisons[0].Stratum
	if stratum.StorageProfileID != benchmark.StorageProfileNFSComplete || stratum.StorageNFSLinkID != benchmark.NFSLink1Gbit {
		t.Fatalf("the stratum must name the storage profile and its link: %#v", stratum)
	}
	if stratum.StorageLinkBPS != profile.LinkBitsPerSecond || stratum.StorageRTTMicros != profile.RTTMicros {
		t.Fatalf("the stratum must carry the storage link's fixed values: %#v", stratum)
	}
}

func TestSummaryNeverPoolsLocalAndShapedStorage(t *testing.T) {
	local := storageSummarySet(t, benchmark.DefaultStorageProfile())
	shaped := storageSummarySet(t, nfsStorageProfile(t))
	mixed := append(append([]benchmark.QueueArtifact{}, local...), shaped...)
	_, err := buildSummaryReport(mixed, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000)
	if err == nil {
		t.Fatal("local and shaped-storage runs must never be summarized together")
	}
	if !strings.Contains(err.Error(), "mix storage profiles") {
		t.Fatalf("the refusal should name the mixed strata, got %v", err)
	}
}

func TestSummaryRequiresStorageEvidenceThatMatchesThePlan(t *testing.T) {
	profile := nfsStorageProfile(t)

	missing := storageSummarySet(t, profile)
	missing[0].StorageAttestation = nil
	if _, err := buildSummaryReport(missing, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("a shaped-storage artifact without its attestation must not be summarized")
	}

	unexpected := storageSummarySet(t, benchmark.DefaultStorageProfile())
	unexpected[0].StorageAttestation = storageAttestationFor(profile)
	if _, err := buildSummaryReport(unexpected, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("a local artifact carrying storage evidence must be refused")
	}

	other, err := benchmark.ResolveStorageProfile(benchmark.StorageProfileNFSComplete, benchmark.NFSLink100Mbit)
	if err != nil {
		t.Fatal(err)
	}
	mismatched := storageSummarySet(t, profile)
	mismatched[0].StorageAttestation = storageAttestationFor(other)
	if _, err := buildSummaryReport(mismatched, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("an attestation describing a different link must be refused")
	}

	unproven := storageSummarySet(t, profile)
	unproven[0].StorageAttestation.After.IngressBytes = unproven[0].StorageAttestation.Before.IngressBytes
	unproven[0].StorageAttestation.IngressBytes = 0
	if _, err := buildSummaryReport(unproven, nil, benchmark.Weaver, benchmark.SABnzbd, 20, 17, 1_000); err == nil {
		t.Fatal("an attestation whose qdiscs saw no client traffic must be refused")
	}
}

func TestResolveStoragePlanProfileMatchesTheLibrary(t *testing.T) {
	profile, err := resolveStoragePlanProfile(benchmark.StorageProfileNFSAll, benchmark.NFSLink2500Mbit)
	if err != nil {
		t.Fatal(err)
	}
	if profile.ID != benchmark.StorageProfileNFSAll || profile.LinkBitsPerSecond != 2_500_000_000 {
		t.Fatalf("plan flags must resolve to the named link, got %#v", profile)
	}
	if _, err := resolveStoragePlanProfile(benchmark.StorageProfileNFSAll, ""); err == nil {
		t.Fatal("an NFS plan must name its link explicitly")
	}
	if _, err := resolveStoragePlanProfile("", ""); err != nil {
		t.Fatalf("a plan that says nothing about storage is local: %v", err)
	}
}
