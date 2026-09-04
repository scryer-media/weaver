package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeNFSServer answers the exact docker command lines the controller issues,
// so the whole storage lifecycle can be exercised without a Linux host: the
// lease, the per-run export, the volumes, the helper container and the
// before/after counters.
type fakeNFSServer struct {
	t             *testing.T
	calls         []string
	leaseID       string
	exportDirs    map[string]bool
	volumes       map[string]string
	report        StorageShaperReport
	movedBytes    bool
	failLeaseOnce bool
	stolenLease   string
}

func newFakeNFSServer(t *testing.T) *fakeNFSServer {
	return &fakeNFSServer{
		t:          t,
		exportDirs: map[string]bool{},
		volumes:    map[string]string{},
		report:     testShaperReport(),
	}
}

func (f *fakeNFSServer) runner() StorageCommandRunner {
	return func(_ context.Context, name string, arguments ...string) (string, error) {
		line := name + " " + strings.Join(arguments, " ")
		f.calls = append(f.calls, line)
		switch {
		case strings.Contains(line, "nntpbench-nfs-control lease-acquire"):
			if f.failLeaseOnce {
				f.failLeaseOnce = false
				return "", fmt.Errorf("the NFS execution lease is already held")
			}
			if f.leaseID != "" {
				return "", fmt.Errorf("the NFS execution lease is already held")
			}
			f.leaseID = arguments[len(arguments)-1]
			return "", nil
		case strings.Contains(line, "nntpbench-nfs-control lease-release"):
			f.leaseID = ""
			return "", nil
		case strings.Contains(line, "nntpbench-nfs-control lease-id"):
			if f.stolenLease != "" {
				return f.stolenLease, nil
			}
			return f.leaseID, nil
		case strings.Contains(line, "nntpbench-nfs-control export-create"):
			f.exportDirs[arguments[len(arguments)-1]] = true
			return "", nil
		case strings.Contains(line, "nntpbench-nfs-control export-remove"):
			delete(f.exportDirs, arguments[len(arguments)-1])
			return "", nil
		case strings.Contains(line, "nntpbench-nfs-control export-report"):
			return "/export <world>(sync,wdelay,hide,no_subtree_check,fsid=0,sec=sys,rw,insecure,no_root_squash,no_all_squash)", nil
		case strings.Contains(line, "cat /run/nntpbench-storage-shaper.json"):
			encoded, err := json.Marshal(f.report)
			if err != nil {
				f.t.Fatal(err)
			}
			return string(encoded), nil
		case strings.HasPrefix(line, "docker inspect"):
			return "172.31.0.2\n", nil
		case strings.HasPrefix(line, "docker volume create"):
			f.volumes[arguments[len(arguments)-1]] = line
			return arguments[len(arguments)-1], nil
		case strings.HasPrefix(line, "docker volume rm"):
			delete(f.volumes, arguments[len(arguments)-1])
			return "", nil
		case strings.Contains(line, "/proc/mounts"):
			return "proc /proc proc rw 0 0\n172.31.0.2:/run/complete " + storageHelperMountPoint +
				" nfs4 rw,relatime,vers=4.1,rsize=1048576,wsize=1048576 0 0", nil
		case strings.Contains(line, "tc -s qdisc show dev eth0"):
			if f.movedBytes {
				return egressQdiscFixture, nil
			}
			return strings.ReplaceAll(egressQdiscFixture, "5368709120", "1000"), nil
		case strings.Contains(line, "tc -s qdisc show dev ifb-nfs"):
			if f.movedBytes {
				return ingressQdiscFixture, nil
			}
			return strings.ReplaceAll(ingressQdiscFixture, "1073741824", "500"), nil
		case strings.Contains(line, "cat /proc/net/rpc/nfsd"):
			if f.movedBytes {
				return nfsdCountersFixture, nil
			}
			return "io 1000 500", nil
		case strings.Contains(line, "verify-output"):
			verification := OutputVerification{
				FixtureID: "fixture-a",
				Files:     []VerifiedOutputFile{{ExpectedPath: "movie.mkv", ActualPath: "movie.mkv", Size: 1}},
			}
			encoded, err := json.Marshal(verification)
			if err != nil {
				f.t.Fatal(err)
			}
			return string(encoded), nil
		case strings.Contains(line, "delete-output"):
			return "", nil
		}
		f.t.Fatalf("unexpected controller command: %s", line)
		return "", nil
	}
}

// lastCall returns the most recent controller command containing fragment, so
// an assertion can inspect one whole docker argument vector rather than hoping
// a substring came from the command it means.
func (f *fakeNFSServer) lastCall(t *testing.T, fragment string) string {
	t.Helper()
	for index := len(f.calls) - 1; index >= 0; index-- {
		if strings.Contains(f.calls[index], fragment) {
			return f.calls[index]
		}
	}
	t.Fatalf("no controller command contained %q", fragment)
	return ""
}

func (f *fakeNFSServer) called(fragment string) bool {
	for _, call := range f.calls {
		if strings.Contains(call, fragment) {
			return true
		}
	}
	return false
}

func testStorageOptions(t *testing.T, server *fakeNFSServer, profile StorageProfile) StorageOptions {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "nntpbench")
	if err := os.WriteFile(binary, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	return StorageOptions{
		Profile:      profile,
		DockerBinary: "docker",
		Container:    "bench-nfs",
		Network:      "bench_storage",
		HelperImage:  DefaultNFSImage,
		VerifyBinary: binary,
		Runner:       server.runner(),
	}
}

func TestStorageSessionLifecycleIsSelfCleaning(t *testing.T) {
	server := newFakeNFSServer(t)
	profile, err := ResolveStorageProfile(StorageProfileNFSAll, NFSLink1Gbit)
	if err != nil {
		t.Fatal(err)
	}
	options := testStorageOptions(t, server, profile)
	session, err := OpenStorageSession(context.Background(), options, "run-0001")
	if err != nil {
		t.Fatal(err)
	}
	if server.leaseID == "" || len(server.leaseID) != 64 {
		t.Fatalf("the session must hold a 64-character exclusive lease, got %q", server.leaseID)
	}
	if len(server.exportDirs) != 1 {
		t.Fatalf("expected exactly one per-run export directory, got %v", server.exportDirs)
	}
	for directory := range server.exportDirs {
		if !strings.HasPrefix(directory, "/run-0001-") {
			t.Fatalf("export directory %q should be a per-run subtree of the pseudo-root", directory)
		}
	}
	if len(server.volumes) != 2 {
		t.Fatalf("nfs-all needs a complete and an incomplete volume, got %v", server.volumes)
	}
	for name, call := range server.volumes {
		for _, expected := range []string{
			"--driver local",
			"--opt type=nfs",
			"--opt o=addr=172.31.0.2," + nfsMountOptions,
			"--label com.scryer-media.weaver.nntp-bench.run=run-0001",
		} {
			if !strings.Contains(call, expected) {
				t.Fatalf("volume %s was created without %q: %s", name, expected, call)
			}
		}
		if !strings.Contains(call, "--opt device=:/run-0001-") {
			t.Fatalf("volume %s does not address the run's export subtree: %s", name, call)
		}
	}

	environment := session.Environment()
	if len(environment) != 2 || !strings.HasPrefix(environment[0], "BENCH_STORAGE_COMPLETE_VOLUME=nntpbench-run-0001-complete-") {
		t.Fatalf("the adapter must be told the volume names and nothing else: %v", environment)
	}
	if strings.Contains(strings.Join(environment, " "), "172.31.0.2") {
		t.Fatal("the export address must never reach a product's environment")
	}

	server.movedBytes = true
	verification, err := session.Verify(context.Background(), "/fixtures/fixture-a")
	if err != nil {
		t.Fatal(err)
	}
	if verification.FixtureID != "fixture-a" {
		t.Fatalf("helper verification result was not returned: %#v", verification)
	}
	if !server.called("--entrypoint " + storageHelperBinaryPath) {
		t.Fatal("verification must run the harness's own binary, not the image's entrypoint")
	}
	if !server.called("type=bind,src=/fixtures/fixture-a,dst=" + storageHelperFixturePath + ",readonly") {
		t.Fatal("the fixture must be mounted read-only into the verification helper")
	}
	verifyCall := server.lastCall(t, "verify-output")
	if !strings.Contains(verifyCall, "--volumes-from bench-nfs:ro") {
		t.Fatalf("verification must read the export from the server's own volume: %s", verifyCall)
	}
	if !strings.Contains(verifyCall, "--output-dir "+storageExportRoot+"/run-0001-") {
		t.Fatalf("verification must address the run's server-side completion directory: %s", verifyCall)
	}
	if strings.Contains(verifyCall, "type=volume,src=nntpbench-run-0001-complete-") {
		t.Fatalf("verification must not pull the output back over the shaped NFS mount: %s", verifyCall)
	}
	if strings.Contains(verifyCall, "--network") {
		t.Fatalf("server-side verification needs no benchmark network: %s", verifyCall)
	}
	if err := session.Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	deleteCall := server.lastCall(t, "delete-output")
	if !strings.Contains(deleteCall, "--volumes-from bench-nfs ") {
		t.Fatalf("deletion needs the server's export volume read-write: %s", deleteCall)
	}
	if !strings.Contains(deleteCall, "--output-dir "+storageExportRoot+"/run-0001-") {
		t.Fatalf("deletion must address the run's server-side completion directory: %s", deleteCall)
	}
	if strings.Contains(deleteCall, "type=volume,src=nntpbench-run-0001-complete-") {
		t.Fatalf("deletion must not go back over the shaped NFS mount: %s", deleteCall)
	}

	attestation, err := session.Finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := attestation.Validate(); err != nil {
		t.Fatalf("a completed session should produce publishable evidence: %v", err)
	}
	if attestation.EgressBytes == 0 || attestation.IngressBytes == 0 {
		t.Fatalf("the attestation should carry both directions' byte deltas: %#v", attestation)
	}
	if attestation.LeaseID != server.leaseID {
		t.Fatal("the attestation must name the lease the session actually held")
	}

	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(server.volumes) != 0 || len(server.exportDirs) != 0 || server.leaseID != "" {
		t.Fatalf("close left resources behind: volumes %v exports %v lease %q", server.volumes, server.exportDirs, server.leaseID)
	}
}

func TestStorageSessionCompleteOnlyProfileCreatesOneVolume(t *testing.T) {
	server := newFakeNFSServer(t)
	options := testStorageOptions(t, server, testNFSProfile(t))
	session, err := OpenStorageSession(context.Background(), options, "run-0002")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	if len(server.volumes) != 1 {
		t.Fatalf("nfs-complete keeps the intermediate directory local, got %v", server.volumes)
	}
	environment := session.Environment()
	if environment[1] != "BENCH_STORAGE_INCOMPLETE_VOLUME=" {
		t.Fatalf("nfs-complete must not hand the adapter an intermediate volume: %v", environment)
	}
}

func TestStorageSessionRefusesToOpenWithoutAnExclusiveLease(t *testing.T) {
	server := newFakeNFSServer(t)
	server.failLeaseOnce = true
	options := testStorageOptions(t, server, testNFSProfile(t))
	if _, err := OpenStorageSession(context.Background(), options, "run-0003"); err == nil {
		t.Fatal("a held lease must stop a second run from opening the export")
	}
	if len(server.volumes) != 0 || len(server.exportDirs) != 0 {
		t.Fatalf("a failed open must leave nothing behind: %v %v", server.volumes, server.exportDirs)
	}
}

func TestStorageSessionRejectsALeaseThatChangedHands(t *testing.T) {
	server := newFakeNFSServer(t)
	options := testStorageOptions(t, server, testNFSProfile(t))
	session, err := OpenStorageSession(context.Background(), options, "run-0004")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	server.movedBytes = true
	server.stolenLease = strings.Repeat("b", 64)
	if _, err := session.Finish(context.Background()); err == nil {
		t.Fatal("a lease that changed hands mid-run must invalidate the measurement")
	}
}

func TestStorageSessionRejectsAnUnshapedServer(t *testing.T) {
	server := newFakeNFSServer(t)
	server.report.LinkBitsPerSecond = 10_000_000_000
	options := testStorageOptions(t, server, testNFSProfile(t))
	if _, err := OpenStorageSession(context.Background(), options, "run-0005"); err == nil {
		t.Fatal("a container shaped differently from the plan must not open a session")
	}
	if server.leaseID != "" {
		t.Fatal("a refused open must release the lease it took")
	}
}

func TestStorageSessionOptionsRequireCompleteWiring(t *testing.T) {
	server := newFakeNFSServer(t)
	base := testStorageOptions(t, server, testNFSProfile(t))
	for name, mutate := range map[string]func(*StorageOptions){
		"no container":    func(o *StorageOptions) { o.Container = "" },
		"no network":      func(o *StorageOptions) { o.Network = "" },
		"no helper image": func(o *StorageOptions) { o.HelperImage = "" },
		"no binary":       func(o *StorageOptions) { o.VerifyBinary = "" },
		"missing binary":  func(o *StorageOptions) { o.VerifyBinary = filepath.Join(t.TempDir(), "absent") },
		"local profile":   func(o *StorageOptions) { o.Profile = DefaultStorageProfile() },
	} {
		options := base
		mutate(&options)
		if _, err := OpenStorageSession(context.Background(), options, "run-0006"); err == nil {
			t.Fatalf("%s should not open a storage session", name)
		}
	}
	if _, err := OpenStorageSession(context.Background(), base, "  "); err == nil {
		t.Fatal("a session without a run identifier should not open")
	}
}

func TestLocalOutputStoreAnnouncesNoVolumes(t *testing.T) {
	store := NewLocalOutputStore(t.TempDir())
	environment := store.Environment()
	if len(environment) != 2 || environment[0] != "BENCH_STORAGE_COMPLETE_VOLUME=" || environment[1] != "BENCH_STORAGE_INCOMPLETE_VOLUME=" {
		t.Fatalf("the local store must declare empty volume names explicitly: %v", environment)
	}
}

func TestStorageNamesStayWithinDockerLimits(t *testing.T) {
	long := strings.Repeat("run-0001-weaver-vanilla-plaintext-", 4)
	name := storageVolumeName(long, strings.Repeat("f", 64), "complete")
	if len(name) > 63 {
		t.Fatalf("volume name %q is too long for a Docker resource name", name)
	}
	if strings.ContainsAny(name, "/ .:") {
		t.Fatalf("volume name %q contains a character Docker rejects", name)
	}
	if got := sanitizeStorageName("Run/0001"); got != "run-0001" {
		t.Fatalf("run identifiers should sanitize to a Docker-safe name, got %q", got)
	}
}

// TestStorageSessionAgainstALiveServer is the executable half of the storage
// lane's host checklist. It is skipped unless an operator points it at a
// running shaped NFS container, and then exercises the real docker, tc and
// nfsd surfaces the fakes above stand in for: lease, export, volume, mount,
// byte movement, attestation and cleanup.
//
//	docker build -f docker/nfs-server/Dockerfile -t weaver-nntp-bench-nfs:dev .
//	docker network create bench_storage
//	docker volume create bench-nfs-export
//	docker run -d --privileged --name bench-nfs --network bench_storage \
//	    --mount type=volume,src=bench-nfs-export,dst=/export \
//	    --env-file storage.env weaver-nntp-bench-nfs:dev
//	NNTPBENCH_NFS_CONTAINER=bench-nfs NNTPBENCH_NFS_NETWORK=bench_storage \
//	    NNTPBENCH_NFS_VERIFY_BINARY=$(pwd)/nntpbench go test ./internal/benchmark -run LiveServer -v
func TestStorageSessionAgainstALiveServer(t *testing.T) {
	container := os.Getenv("NNTPBENCH_NFS_CONTAINER")
	network := os.Getenv("NNTPBENCH_NFS_NETWORK")
	binary := os.Getenv("NNTPBENCH_NFS_VERIFY_BINARY")
	if container == "" || network == "" || binary == "" {
		t.Skip("set NNTPBENCH_NFS_CONTAINER, NNTPBENCH_NFS_NETWORK and NNTPBENCH_NFS_VERIFY_BINARY to exercise a live shaped export")
	}
	image := os.Getenv("NNTPBENCH_NFS_IMAGE")
	if image == "" {
		image = DefaultNFSImage
	}
	link := os.Getenv("NNTPBENCH_NFS_LINK")
	if link == "" {
		link = NFSLink1Gbit
	}
	id := os.Getenv("NNTPBENCH_STORAGE_PROFILE")
	if id == "" {
		id = StorageProfileNFSComplete
	}
	profile, err := ResolveStorageProfile(id, link)
	if err != nil {
		t.Fatal(err)
	}
	options := StorageOptions{
		Profile:      profile,
		DockerBinary: "docker",
		Container:    container,
		Network:      network,
		HelperImage:  image,
		VerifyBinary: binary,
	}
	ctx := context.Background()
	session, err := OpenStorageSession(ctx, options, "run-live")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("the session must leave no docker resources behind: %v", err)
		}
	}()
	if !strings.Contains(session.clientMountLine, "vers=4.1") {
		t.Fatalf("the export did not negotiate NFS 4.1: %q", session.clientMountLine)
	}
	// Move enough through the export that the attestation's own floor is met.
	if _, err := session.helperRun(ctx, true, []string{
		"dd", "if=/dev/zero", "of=" + storageHelperMountPoint + "/probe.bin", "bs=1M", "count=8",
	}); err != nil {
		t.Fatal(err)
	}
	attestation, err := session.Finish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := attestation.Validate(); err != nil {
		t.Fatalf("a live shaped run should produce publishable evidence: %v", err)
	}
	t.Log(attestation.Summary())
	// Deletion is server-side: it reaches the export through the NFS
	// container's own volume, so it must empty the directory the client still
	// sees over the mount without the bytes crossing the shaped link.
	if err := session.Delete(ctx); err != nil {
		t.Logf("delete-output needs a real harness binary at %s: %v", binary, err)
		return
	}
	remaining, err := session.helperRun(ctx, false, []string{"ls", "-A", storageHelperMountPoint})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(remaining) != "" {
		t.Fatalf("server-side deletion left output visible to the client: %q", remaining)
	}
}
