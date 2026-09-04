package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveStorageProfileNamesFixedLinks(t *testing.T) {
	local, err := ResolveStorageProfile("", "")
	if err != nil {
		t.Fatal(err)
	}
	if local != DefaultStorageProfile() || local.Kind != StorageLocal || local.usesNFS() {
		t.Fatalf("empty storage profile should resolve to the local default, got %#v", local)
	}

	profile, err := ResolveStorageProfile(StorageProfileNFSComplete, NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Kind != StorageNFS || profile.LinkBitsPerSecond != 1_000_000_000 || profile.RTTMicros != 1_000 {
		t.Fatalf("nas-1gbit must stay a fixed 1 Gbit/s 1000us link, got %#v", profile)
	}
	if profile.OneWayDelayMicros() != 500 {
		t.Fatalf("one-way delay should be half the declared round trip, got %dus", profile.OneWayDelayMicros())
	}
	if profile.IntermediateOnNFS || !profile.CompleteOnNFS {
		t.Fatalf("nfs-complete must place only the completion directory on the export, got %#v", profile)
	}
	if profile.Shaper != storageShaperTBFNetem || profile.AttestationScope != storageScopeNFSLink {
		t.Fatalf("nfs profile must declare its shaper and attestation scope, got %#v", profile)
	}

	all, err := ResolveStorageProfile(StorageProfileNFSAll, NFSLink100Mbit)
	if err != nil {
		t.Fatal(err)
	}
	if !all.IntermediateOnNFS || !all.CompleteOnNFS {
		t.Fatalf("nfs-all must place both directories on the export, got %#v", all)
	}
}

func TestResolveStorageProfileRejectsIncoherentSelections(t *testing.T) {
	for name, testCase := range map[string]struct{ id, link string }{
		"local with a link":     {StorageProfileLocal, NFSLink1Gbit},
		"nfs without a link":    {StorageProfileNFSAll, ""},
		"nfs with none":         {StorageProfileNFSAll, NFSLinkNone},
		"nfs with unknown link": {StorageProfileNFSComplete, "nas-40gbit"},
		"unknown profile":       {"nas", NFSLink1Gbit},
	} {
		if _, err := ResolveStorageProfile(testCase.id, testCase.link); err == nil {
			t.Fatalf("%s should not resolve", name)
		}
	}
}

func TestStorageProfileValidateRejectsTamperedValues(t *testing.T) {
	profile, err := ResolveStorageProfile(StorageProfileNFSAll, NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	tampered := profile
	tampered.LinkBitsPerSecond = 10_000_000_000
	if err := tampered.Validate(); err == nil {
		t.Fatal("a named link that changed its rate should not validate")
	}
	tampered = profile
	tampered.MountOptions = "nfsvers=4.0"
	if err := tampered.Validate(); err == nil {
		t.Fatal("a changed mount option set should not validate")
	}
	tampered = profile
	tampered.ExportOptions = "rw"
	if err := tampered.Validate(); err == nil {
		t.Fatal("a changed export option set should not validate")
	}
}

func TestPlanRejectsNFSStorageOnNativeTargets(t *testing.T) {
	profile, err := ResolveStorageProfile(StorageProfileNFSComplete, NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []ExecutionTarget{MacOSNative, WindowsNative} {
		if _, err := BuildPlan(PlanOptions{
			FixtureIDs:     []string{"fixture"},
			Clients:        []Client{Weaver},
			Transports:     []Transport{Plaintext},
			Targets:        []ExecutionTarget{target},
			StorageProfile: profile,
			Repetitions:    1,
		}); err == nil {
			t.Fatalf("an NFS storage profile should not plan against %s", target)
		}
	}
	if _, err := BuildPlan(PlanOptions{
		FixtureIDs:     []string{"fixture"},
		Clients:        []Client{Weaver},
		Transports:     []Transport{Plaintext},
		Targets:        []ExecutionTarget{DockerLinux, MacOSNative},
		StorageProfile: profile,
		Repetitions:    1,
	}); err == nil {
		t.Fatal("a mixed-target plan should not carry an NFS storage profile")
	}
}

func TestPlanCarriesStorageProfileIntoEveryRun(t *testing.T) {
	profile, err := ResolveStorageProfile(StorageProfileNFSAll, NFSLink2500Mbit)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:     []string{"fixture"},
		Clients:        []Client{Weaver, SABnzbd},
		Transports:     []Transport{Plaintext},
		Targets:        []ExecutionTarget{DockerLinux},
		StorageProfile: profile,
		Repetitions:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.SchemaVersion != 6 {
		t.Fatalf("plan schema should be 6, got %d", plan.SchemaVersion)
	}
	if plan.StorageProfile != profile {
		t.Fatalf("plan should carry the resolved storage profile, got %#v", plan.StorageProfile)
	}
	for _, run := range plan.Runs {
		if run.StorageProfile != profile {
			t.Fatalf("run %s does not carry the plan's storage profile", run.ID)
		}
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	plan.Runs[0].StorageProfile = DefaultStorageProfile()
	if err := plan.Validate(); err == nil {
		t.Fatal("a run whose storage profile drifted from its plan should not validate")
	}
}

func TestDefaultPlanStaysLocal(t *testing.T) {
	plan, err := BuildPlan(PlanOptions{
		FixtureIDs:  []string{"fixture"},
		Clients:     []Client{Weaver},
		Transports:  []Transport{Plaintext},
		Targets:     []ExecutionTarget{DockerLinux, MacOSNative},
		Repetitions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.StorageProfile != DefaultStorageProfile() {
		t.Fatalf("a plan that declares no storage profile must be local, got %#v", plan.StorageProfile)
	}
}

func TestWriteStorageLinkEnvironmentIsImmutableAndComplete(t *testing.T) {
	profile, err := ResolveStorageProfile(StorageProfileNFSComplete, NFSLink100Mbit)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "storage.env")
	if err := WriteStorageLinkEnvironment(path, profile); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"NFS_LINK_BITS_PER_SECOND=100000000",
		"NFS_LINK_BURST_BYTES=131072",
		"NFS_RTT_MICROS=1000",
		"NFS_EXPORT_OPTIONS=" + nfsExportOptions,
	} {
		if !strings.Contains(string(contents), expected) {
			t.Fatalf("storage environment lacks %q:\n%s", expected, contents)
		}
	}
	if err := WriteStorageLinkEnvironment(path, profile); err == nil {
		t.Fatal("an existing storage environment file should never be overwritten")
	}
	if err := WriteStorageLinkEnvironment(filepath.Join(t.TempDir(), "local.env"), DefaultStorageProfile()); err == nil {
		t.Fatal("a local profile has no NFS server to configure")
	}
}
