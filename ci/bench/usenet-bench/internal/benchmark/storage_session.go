package benchmark

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// StorageCommandRunner executes one controller-side command and returns its
// standard output. Failures carry the command's diagnostics in the error so a
// benchmark log never has to be correlated with a separate stream.
type StorageCommandRunner func(ctx context.Context, name string, arguments ...string) (string, error)

// StorageOptions is everything the controller needs to drive the shaped NFS
// server for one run. Only the controller holds these values: the client
// adapter is told which volumes to mount and nothing else, so no product ever
// participates in creating, attesting, or verifying its own storage.
type StorageOptions struct {
	Profile      StorageProfile
	DockerBinary string
	Container    string
	Network      string
	HelperImage  string
	VerifyBinary string
	Runner       StorageCommandRunner
}

const (
	storageHelperMountPoint  = "/mnt/complete"
	storageHelperFixturePath = "/mnt/fixture"
	storageHelperBinaryPath  = "/usr/local/bin/nntpbench-verify"
	// storageExportRoot is where the NFS container mounts its export volume.
	// `--volumes-from` reproduces that mount at the same path, which is how the
	// harness reaches a run's output on the server's own filesystem.
	storageExportRoot       = "/export"
	storageShaperReportPath = "/run/nntpbench-storage-shaper.json"
	storageControlScript    = "/usr/local/bin/nntpbench-nfs-control"
	storageInterface        = "eth0"
)

// StorageSession owns one run's shaped export: an exclusive lease on the NFS
// server, an empty per-run export directory, the Docker volumes the client
// mounts, and the before/after attestation around the measured client.
type StorageSession struct {
	options          StorageOptions
	id               string
	leaseID          string
	leaseAcquiredAt  time.Time
	exportDir        string
	completeVolume   string
	incompleteVolume string
	report           StorageShaperReport
	serverAddress    string
	exportOptionsRaw string
	clientMountLine  string
	before           StorageCounters
}

// OutputStore is where one run's completion directory actually lives. The
// local store is an ordinary host directory; the NFS store is a shaped export
// the controller can only reach through a helper container.
type OutputStore interface {
	// Environment contributes adapter environment describing the store.
	Environment() []string
	// Verify runs the harness's own BLAKE3 verification over the output.
	Verify(ctx context.Context, fixtureDir string) (OutputVerification, error)
	// Delete removes the verified output, retaining the output root.
	Delete(ctx context.Context) error
}

type localOutputStore struct {
	outputDir string
}

func (s localOutputStore) Environment() []string {
	return []string{"BENCH_STORAGE_COMPLETE_VOLUME=", "BENCH_STORAGE_INCOMPLETE_VOLUME="}
}

func (s localOutputStore) Verify(_ context.Context, fixtureDir string) (OutputVerification, error) {
	return VerifyOutput(fixtureDir, s.outputDir)
}

func (s localOutputStore) Delete(_ context.Context) error {
	return DeleteOutputFiles(s.outputDir)
}

// NewLocalOutputStore is the default store for the local storage profile.
func NewLocalOutputStore(outputDir string) OutputStore {
	return localOutputStore{outputDir: outputDir}
}

// NewStorageExecutionLeaseID mints the exclusive NFS-server lease identity for
// one run, using the same unguessable identity as the NNTP shaper lease.
func NewStorageExecutionLeaseID() (string, error) {
	id, err := newExecutionLeaseID()
	if err != nil {
		return "", fmt.Errorf("generate NFS execution lease ID: %w", err)
	}
	return id, nil
}

func execStorageRunner(ctx context.Context, name string, arguments ...string) (string, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	var stderr strings.Builder
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		diagnostics := strings.TrimSpace(stderr.String())
		if len(diagnostics) > 2_000 {
			diagnostics = diagnostics[:2_000] + "…"
		}
		if diagnostics == "" {
			return "", fmt.Errorf("%s %s: %w", name, strings.Join(arguments, " "), err)
		}
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(arguments, " "), err, diagnostics)
	}
	return strings.TrimRight(string(output), "\n"), nil
}

func (o StorageOptions) validate() error {
	if err := o.Profile.Validate(); err != nil {
		return err
	}
	if !o.Profile.usesNFS() {
		return fmt.Errorf("storage profile %q does not use an NFS server", o.Profile.ID)
	}
	for name, value := range map[string]string{
		"Docker binary":     o.DockerBinary,
		"NFS container":     o.Container,
		"NFS network":       o.Network,
		"NFS helper image":  o.HelperImage,
		"NFS verify binary": o.VerifyBinary,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required for storage profile %q", name, o.Profile.ID)
		}
		if strings.ContainsAny(value, ",\r\n") {
			return fmt.Errorf("%s must not contain a comma or line break: %q", name, value)
		}
	}
	if _, err := os.Stat(o.VerifyBinary); err != nil {
		return fmt.Errorf("inspect NFS verify binary: %w", err)
	}
	return nil
}

func (o StorageOptions) runner() StorageCommandRunner {
	if o.Runner != nil {
		return o.Runner
	}
	return execStorageRunner
}

func (o StorageOptions) docker(ctx context.Context, arguments ...string) (string, error) {
	return o.runner()(ctx, o.DockerBinary, arguments...)
}

func (o StorageOptions) control(ctx context.Context, arguments ...string) (string, error) {
	return o.docker(ctx, append([]string{"exec", o.Container, storageControlScript}, arguments...)...)
}

// OpenStorageSession takes the exclusive NFS lease, creates the run's empty
// export directory and volumes, and captures the opening attestation. Every
// failure path releases whatever it already acquired.
func OpenStorageSession(ctx context.Context, options StorageOptions, id string) (session *StorageSession, err error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("storage session requires a run identifier")
	}
	leaseID, err := NewStorageExecutionLeaseID()
	if err != nil {
		return nil, err
	}
	if _, err := options.control(ctx, "lease-acquire", leaseID); err != nil {
		return nil, fmt.Errorf("acquire exclusive NFS execution lease (another run may still hold it): %w", err)
	}
	opened := &StorageSession{
		options:         options,
		id:              id,
		leaseID:         leaseID,
		leaseAcquiredAt: time.Now().UTC(),
		exportDir:       storageExportDirectory(id, leaseID),
	}
	defer func() {
		if err != nil {
			_ = opened.Close(context.WithoutCancel(ctx))
		}
	}()
	rawReport, err := options.docker(ctx, "exec", options.Container, "cat", storageShaperReportPath)
	if err != nil {
		return nil, fmt.Errorf("read NFS shaper report: %w", err)
	}
	report, err := decodeStorageShaperReport(rawReport)
	if err != nil {
		return nil, err
	}
	if err := report.validateFor(options.Profile); err != nil {
		return nil, err
	}
	opened.report = report
	address, err := options.docker(ctx, "inspect", "--format",
		fmt.Sprintf("{{ (index .NetworkSettings.Networks %q).IPAddress }}", options.Network), options.Container)
	if err != nil {
		return nil, fmt.Errorf("inspect NFS server address: %w", err)
	}
	opened.serverAddress = strings.TrimSpace(address)
	if opened.serverAddress == "" {
		return nil, fmt.Errorf("NFS container %s has no address on network %s", options.Container, options.Network)
	}
	if _, err := options.control(ctx, "export-create", opened.exportDir); err != nil {
		return nil, fmt.Errorf("create per-run NFS export directory: %w", err)
	}
	exportOptions, err := options.control(ctx, "export-report")
	if err != nil {
		return nil, fmt.Errorf("read NFS export options: %w", err)
	}
	opened.exportOptionsRaw = exportOptions
	opened.completeVolume = storageVolumeName(id, leaseID, "complete")
	if err := opened.createVolume(ctx, opened.completeVolume, opened.exportDir+"/complete"); err != nil {
		return nil, err
	}
	if options.Profile.IntermediateOnNFS {
		opened.incompleteVolume = storageVolumeName(id, leaseID, "incomplete")
		if err := opened.createVolume(ctx, opened.incompleteVolume, opened.exportDir+"/incomplete"); err != nil {
			return nil, err
		}
	}
	mountLine, err := opened.captureClientMountLine(ctx)
	if err != nil {
		return nil, err
	}
	opened.clientMountLine = mountLine
	before, err := opened.captureCounters(ctx)
	if err != nil {
		return nil, err
	}
	if err := before.validateCountersFor(options.Profile, report); err != nil {
		return nil, fmt.Errorf("opening NFS attestation: %w", err)
	}
	opened.before = before
	return opened, nil
}

func (s *StorageSession) createVolume(ctx context.Context, name, exportPath string) error {
	mountOptions := "addr=" + s.serverAddress + "," + s.options.Profile.MountOptions
	_, err := s.options.docker(ctx, "volume", "create",
		"--driver", "local",
		"--opt", "type=nfs",
		"--opt", "o="+mountOptions,
		"--opt", "device=:"+exportPath,
		"--label", "com.scryer-media.weaver.nntp-bench.run="+s.id,
		name)
	if err != nil {
		return fmt.Errorf("create NFS volume %s: %w", name, err)
	}
	return nil
}

// captureClientMountLine mounts the run's volume in a throwaway helper and
// records the kernel's own view of the negotiated mount. It doubles as an
// early failure: an export that cannot be mounted fails before any client
// container is created.
func (s *StorageSession) captureClientMountLine(ctx context.Context) (string, error) {
	output, err := s.helperRun(ctx, false, []string{"cat", "/proc/mounts"})
	if err != nil {
		return "", fmt.Errorf("inspect NFS client mount: %w", err)
	}
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, " "+storageHelperMountPoint+" ") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", fmt.Errorf("helper container did not report an NFS mount at %s", storageHelperMountPoint)
}

// helperRun executes one command inside the pinned helper image with the run's
// completion volume attached over NFS. Nothing a product wrote can influence
// it: the binary is the harness's own and the entrypoint is overridden
// explicitly. This is the only helper that crosses the shaped link, and it
// exists for the negotiated-mount evidence, which moves no output bytes;
// verification and deletion take helperServerSideRun instead.
func (s *StorageSession) helperRun(ctx context.Context, writable bool, command []string) (string, error) {
	mount := "type=volume,src=" + s.completeVolume + ",dst=" + storageHelperMountPoint
	if !writable {
		mount += ",readonly"
	}
	arguments := []string{
		"run", "--rm",
		"--network", s.options.Network,
		"--label", "com.scryer-media.weaver.nntp-bench.run=" + s.id,
		"--mount", mount,
		"--mount", "type=bind,src=" + s.options.VerifyBinary + ",dst=" + storageHelperBinaryPath + ",readonly",
		"--entrypoint", command[0],
		s.options.HelperImage,
	}
	arguments = append(arguments, command[1:]...)
	return s.options.docker(ctx, arguments...)
}

// helperServerSideRun runs the harness's own binary against the export from
// the server's side of the shaped link: `--volumes-from` gives the helper the
// NFS container's backing export volume at the same path, so reading or
// deleting a run's output never crosses the throttled link and never competes
// with the server it is measuring. No network is attached because nothing here
// speaks NFS.
//
// NFS close-to-open semantics make the server-side view complete. The harness
// touches the export only after the client has reported terminal status and
// closed its files, and closing flushes a client's writes to the server; a
// local reader on the server then sees the server's own page cache, so no
// `sync` is required.
func (s *StorageSession) helperServerSideRun(ctx context.Context, writable bool, mounts []string, command []string) (string, error) {
	volumesFrom := s.options.Container
	if !writable {
		volumesFrom += ":ro"
	}
	arguments := []string{
		"run", "--rm",
		"--label", "com.scryer-media.weaver.nntp-bench.run=" + s.id,
		"--volumes-from", volumesFrom,
	}
	for _, mount := range mounts {
		arguments = append(arguments, "--mount", mount)
	}
	arguments = append(arguments,
		"--mount", "type=bind,src="+s.options.VerifyBinary+",dst="+storageHelperBinaryPath+",readonly",
		"--entrypoint", storageHelperBinaryPath,
		s.options.HelperImage,
	)
	arguments = append(arguments, command...)
	return s.options.docker(ctx, arguments...)
}

// serverSideCompleteDir is the run's completion directory as the NFS server's
// own filesystem sees it, which is the export root plus the per-run subtree the
// client reaches through the pseudo-root.
func (s *StorageSession) serverSideCompleteDir() string {
	return storageExportRoot + s.exportDir + "/complete"
}

func (s *StorageSession) captureCounters(ctx context.Context) (StorageCounters, error) {
	egress, err := s.options.docker(ctx, "exec", s.options.Container, "tc", "-s", "qdisc", "show", "dev", storageInterface)
	if err != nil {
		return StorageCounters{}, fmt.Errorf("read NFS server-to-client qdisc counters: %w", err)
	}
	var ingress, filter string
	switch s.report.IngressMechanism {
	case storageEgressIFB:
		ingress, err = s.options.docker(ctx, "exec", s.options.Container, "tc", "-s", "qdisc", "show", "dev", s.report.IngressDevice)
		if err != nil {
			return StorageCounters{}, fmt.Errorf("read NFS client-to-server qdisc counters: %w", err)
		}
	case storageEgressPolice:
		filter, err = s.options.docker(ctx, "exec", s.options.Container, "tc", "-s", "filter", "show", "dev", storageInterface, "parent", "ffff:")
		if err != nil {
			return StorageCounters{}, fmt.Errorf("read NFS client-to-server policing counters: %w", err)
		}
	}
	nfsd, err := s.options.docker(ctx, "exec", s.options.Container, "cat", "/proc/net/rpc/nfsd")
	if err != nil {
		return StorageCounters{}, fmt.Errorf("read NFS server counters: %w", err)
	}
	return buildStorageCounters(time.Now().UTC(), s.report, egress, ingress, filter, nfsd)
}

// Environment tells the client adapter which volumes to mount. It never leaks
// the export path or the server address into a product's configuration.
func (s *StorageSession) Environment() []string {
	return []string{
		"BENCH_STORAGE_COMPLETE_VOLUME=" + s.completeVolume,
		"BENCH_STORAGE_INCOMPLETE_VOLUME=" + s.incompleteVolume,
	}
}

// Verify runs the harness's own verifier against the export from inside a
// helper container attached to the server's export volume. It is charged to no
// product: callers time it separately, exactly as they time local verification.
func (s *StorageSession) Verify(ctx context.Context, fixtureDir string) (OutputVerification, error) {
	output, err := s.helperServerSideRun(ctx, false,
		[]string{"type=bind,src=" + fixtureDir + ",dst=" + storageHelperFixturePath + ",readonly"},
		[]string{
			"verify-output",
			"--fixture-dir", storageHelperFixturePath,
			"--output-dir", s.serverSideCompleteDir(),
		})
	if err != nil {
		return OutputVerification{}, err
	}
	var verification OutputVerification
	if err := json.Unmarshal([]byte(output), &verification); err != nil {
		return OutputVerification{}, fmt.Errorf("decode helper verification result: %w", err)
	}
	if verification.FixtureID == "" || len(verification.Files) == 0 {
		return OutputVerification{}, fmt.Errorf("helper verification returned no verified files")
	}
	return verification, nil
}

// Delete empties the verified output from the server's side of the link, for
// the same reason Verify reads it there: the bytes never re-cross the shaper.
func (s *StorageSession) Delete(ctx context.Context) error {
	if _, err := s.helperServerSideRun(ctx, true, nil, []string{
		"delete-output", "--output-dir", s.serverSideCompleteDir(),
	}); err != nil {
		return fmt.Errorf("delete verified output from the NFS export: %w", err)
	}
	return nil
}

// Finish captures the closing attestation and asserts that the shaped link was
// configured as planned and actually carried this run's storage traffic.
func (s *StorageSession) Finish(ctx context.Context) (StorageAttestation, error) {
	after, err := s.captureCounters(ctx)
	if err != nil {
		return StorageAttestation{}, err
	}
	if err := after.validateCountersFor(s.options.Profile, s.report); err != nil {
		return StorageAttestation{}, fmt.Errorf("closing NFS attestation: %w", err)
	}
	holder, err := s.options.control(ctx, "lease-id")
	if err != nil {
		return StorageAttestation{}, fmt.Errorf("read NFS execution lease holder: %w", err)
	}
	if strings.TrimSpace(holder) != s.leaseID {
		return StorageAttestation{}, fmt.Errorf("the NFS execution lease changed hands during the measured run")
	}
	egress, ingress, read, written, err := storageDeltas(s.before, after)
	if err != nil {
		return StorageAttestation{}, err
	}
	attestation := StorageAttestation{
		SchemaVersion:        1,
		Profile:              s.options.Profile,
		Container:            s.options.Container,
		Network:              s.options.Network,
		ServerAddress:        s.serverAddress,
		ExportDevice:         ":" + s.exportDir + "/complete",
		ExportOptionsRaw:     s.exportOptionsRaw,
		ClientMountLine:      s.clientMountLine,
		HelperImage:          s.options.HelperImage,
		LeaseID:              s.leaseID,
		LeaseAcquiredAt:      s.leaseAcquiredAt,
		Shaper:               s.report,
		Before:               s.before,
		After:                after,
		EgressBytes:          egress,
		IngressBytes:         ingress,
		ServerReadBytes:      read,
		ServerWrittenBytes:   written,
		VerificationStrategy: storageVerificationStrategy,
		CPUAccountingCaveat:  storageCPUCaveat,
	}
	if err := attestation.Validate(); err != nil {
		return StorageAttestation{}, err
	}
	return attestation, nil
}

// Close removes every resource this session created and releases the lease.
// It is safe to call after a partial open and reports the first failure so a
// leaked volume can never be mistaken for a clean run.
func (s *StorageSession) Close(ctx context.Context) error {
	var failures []string
	for _, volume := range []string{s.completeVolume, s.incompleteVolume} {
		if volume == "" {
			continue
		}
		if _, err := s.options.docker(ctx, "volume", "rm", "--force", volume); err != nil {
			failures = append(failures, fmt.Sprintf("remove NFS volume %s: %v", volume, err))
		}
	}
	s.completeVolume = ""
	s.incompleteVolume = ""
	if s.exportDir != "" {
		if _, err := s.options.control(ctx, "export-remove", s.exportDir); err != nil {
			failures = append(failures, fmt.Sprintf("remove NFS export directory %s: %v", s.exportDir, err))
		}
		s.exportDir = ""
	}
	if s.leaseID != "" {
		if _, err := s.options.control(ctx, "lease-release", s.leaseID); err != nil {
			failures = append(failures, fmt.Sprintf("release NFS execution lease: %v", err))
		}
		s.leaseID = ""
	}
	if len(failures) > 0 {
		return fmt.Errorf("%s", strings.Join(failures, "; "))
	}
	return nil
}

// storageExportDirectory names one run's export subtree. NFSv4 clients address
// an export relative to the pseudo-root (the fsid=0 export), so the path is
// absolute there and the server resolves it under /export.
func storageExportDirectory(id, leaseID string) string {
	return "/" + sanitizeStorageName(id) + "-" + leaseID[:8]
}

func storageVolumeName(id, leaseID, role string) string {
	return "nntpbench-" + sanitizeStorageName(id) + "-" + role + "-" + leaseID[:8]
}

func sanitizeStorageName(value string) string {
	var builder strings.Builder
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z', character >= '0' && character <= '9', character == '-':
			builder.WriteRune(character)
		case character >= 'A' && character <= 'Z':
			builder.WriteRune(character - 'A' + 'a')
		default:
			builder.WriteByte('-')
		}
	}
	name := strings.Trim(builder.String(), "-")
	if name == "" {
		name = "run"
	}
	// A volume name is this plus a fixed prefix, role and lease fragment, so
	// the sanitized identifier is capped well inside Docker's own limit and a
	// long identifier collapses to a digest rather than a collision.
	if len(name) > 24 {
		digest := sha256.Sum256([]byte(value))
		name = fmt.Sprintf("%s-%x", name[:16], digest[:4])
	}
	return name
}
