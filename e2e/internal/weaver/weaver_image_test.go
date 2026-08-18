package weaver

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The two Cargo.toml shapes the parser is exercised against: one with a
// [patch.crates-io] block whose paths climb out of the repository (which the
// image build must refuse) and the published shape with no patch block at all.
const patchedCargoToml = `[workspace]
members = ["server/app/weaver"]

[workspace.dependencies]
par2-rs = "0.3"
unrar-rs = "0.5"

# LOCAL ONLY - remove before merging. par2-rs 0.3 is not published yet.
[patch.crates-io]
par2-rs = { path = "../rarpar/crates/weaver-par2" }
unrar-rs = { path = "../rarpar/crates/weaver-unrar" }
reedsolomon-rs = { path = "../rarpar/crates/weaver-reed-solomon" }

[profile.release]
lto = "thin"
`

const publishedCargoToml = `[workspace]
members = ["server/app/weaver"]

[workspace.dependencies]
par2-rs = "0.3"
unrar-rs = "0.5"

# A [patch.crates-io] block that points outside the repository is not
# supported by the e2e image build; publish the crate or vendor it in-repo.

[profile.release]
lto = "thin"
`

func TestCargoCratesIoPatchPathsReadsThePatchBlock(t *testing.T) {
	got := cargoCratesIoPatchPaths(patchedCargoToml)
	want := []string{
		"../rarpar/crates/weaver-par2",
		"../rarpar/crates/weaver-unrar",
		"../rarpar/crates/weaver-reed-solomon",
	}
	if len(got) != len(want) {
		t.Fatalf("patch paths = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("patch path %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCargoCratesIoPatchPathsIgnoresCommentaryAndOtherTables(t *testing.T) {
	if got := cargoCratesIoPatchPaths(publishedCargoToml); got != nil {
		t.Fatalf("published manifest yielded patch paths %v, want none", got)
	}
	// A [patch.crates-io] header that has been commented out is not a patch block.
	commented := strings.Replace(patchedCargoToml, "[patch.crates-io]", "# [patch.crates-io]", 1)
	if got := cargoCratesIoPatchPaths(commented); got != nil {
		t.Fatalf("commented-out patch table yielded %v, want none", got)
	}
	// Path dependencies declared in some other table must not be picked up.
	unrelated := `[dependencies]
weaver-core = { path = "server/crates/weaver-server-core" }
`
	if got := cargoCratesIoPatchPaths(unrelated); got != nil {
		t.Fatalf("unrelated path dependency yielded %v, want none", got)
	}
}

func TestCargoCratesIoPatchPathsAcceptsQuotedTableName(t *testing.T) {
	quoted := strings.Replace(patchedCargoToml, "[patch.crates-io]", `[patch."crates-io"]`, 1)
	if got := cargoCratesIoPatchPaths(quoted); len(got) != 3 {
		t.Fatalf(`[patch."crates-io"] yielded %v, want 3 paths`, got)
	}
}

func TestCargoCratesIoPatchPathsAcceptsSubTableEntries(t *testing.T) {
	manifest := `[workspace]
members = ["server/app/weaver"]

[patch.crates-io.par2-rs]
path = "../rarpar/crates/weaver-par2"

[patch.crates-io."unrar-rs"]
path = "../rarpar/crates/weaver-unrar"

[profile.release]
lto = "thin"
`
	got := cargoCratesIoPatchPaths(manifest)
	want := []string{"../rarpar/crates/weaver-par2", "../rarpar/crates/weaver-unrar"}
	if len(got) != len(want) {
		t.Fatalf("patch paths = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("patch path %d = %q, want %q", i, got[i], want[i])
		}
	}
	// A bare `path =` key outside a patch table must not be mistaken for one.
	unrelated := "[package.metadata.thing]\npath = \"../elsewhere\"\n"
	if paths := cargoCratesIoPatchPaths(unrelated); paths != nil {
		t.Fatalf("unrelated bare path key yielded %v, want none", paths)
	}
}

func TestParseTomlTableHeader(t *testing.T) {
	for _, testCase := range []struct {
		line string
		want []string
		ok   bool
	}{
		{`[patch.crates-io]`, []string{"patch", "crates-io"}, true},
		{`[patch."crates-io"]`, []string{"patch", "crates-io"}, true},
		{`[patch.'crates-io'.par2-rs]`, []string{"patch", "crates-io", "par2-rs"}, true},
		{`[workspace]  # trailing comment`, []string{"workspace"}, true},
		{`[[bin]]`, nil, false},
		{`[patch.crates-io`, nil, false},
		{`[]`, nil, false},
	} {
		got, ok := parseTomlTableHeader(testCase.line)
		if ok != testCase.ok {
			t.Fatalf("parseTomlTableHeader(%q) ok = %t, want %t", testCase.line, ok, testCase.ok)
		}
		if !ok {
			continue
		}
		if strings.Join(got, ".") != strings.Join(testCase.want, ".") {
			t.Fatalf("parseTomlTableHeader(%q) = %v, want %v", testCase.line, got, testCase.want)
		}
	}
}

func TestCargoCratesIoPatchPathsStopsAtTheNextTable(t *testing.T) {
	manifest := patchedCargoToml + `
[patch.'https://github.com/example/other']
somecrate = { path = "../other/crates/somecrate" }
`
	got := cargoCratesIoPatchPaths(manifest)
	for _, value := range got {
		if strings.Contains(value, "other") {
			t.Fatalf("patch paths leaked past the table boundary: %v", got)
		}
	}
	if len(got) != 3 {
		t.Fatalf("patch paths = %v, want the 3 crates-io entries", got)
	}
}

func TestParseRustToolchainChannel(t *testing.T) {
	if got := parseRustToolchainChannel("[toolchain]\nchannel = \"1.97.1\"\n"); got != "1.97.1" {
		t.Fatalf("channel = %q, want %q", got, "1.97.1")
	}
	if got := parseRustToolchainChannel("[toolchain]\nchannel = \"stable\"\ncomponents = [\"clippy\"]\n"); got != "stable" {
		t.Fatalf("channel = %q, want %q", got, "stable")
	}
	if got := parseRustToolchainChannel("[toolchain]\n# channel = \"1.90.0\"\n"); got != "" {
		t.Fatalf("commented channel = %q, want empty", got)
	}
}

func TestWeaverImagePlanDockerfilePublishedShape(t *testing.T) {
	plan := weaverImagePlan{Toolchain: "1.97.1"}
	dockerfile := plan.dockerfile()
	if !strings.Contains(dockerfile, "cargo build --release --locked -p weaver") {
		t.Fatalf("published dockerfile must restore --locked:\n%s", dockerfile)
	}
	if strings.Contains(dockerfile, "COPY --from=") && strings.Contains(dockerfile, "/rarpar") {
		t.Fatalf("published dockerfile must not carry a patch context:\n%s", dockerfile)
	}
	if !strings.Contains(dockerfile, "rustup toolchain install 1.97.1") {
		t.Fatalf("published dockerfile must still honour the repo toolchain pin:\n%s", dockerfile)
	}
	for _, fragment := range []string{
		"# syntax=docker/dockerfile:1.7",
		"COPY apps/weaver-web/package.json apps/weaver-web/package-lock.json",
		"--mount=type=cache,target=/root/.npm",
		"--mount=type=cache,target=/app/target",
	} {
		if !strings.Contains(dockerfile, fragment) {
			t.Fatalf("published dockerfile is missing cache optimization %q:\n%s", fragment, dockerfile)
		}
	}
}

func TestWeaverImagePlanBuildArgs(t *testing.T) {
	plan := weaverImagePlan{Toolchain: "1.97.1"}
	args := plan.buildArgs("/tmp/x.Dockerfile", "weaver-e2e-weaver:local", "/repos/weaver", "abc123")
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "buildx build") {
		t.Fatalf("image build goes through buildx: %v", args)
	}
	if !strings.Contains(joined, "--label "+weaverImageFingerprintLabel+"=abc123") {
		t.Fatalf("build args are missing the source fingerprint label: %v", args)
	}
	if strings.Contains(joined, "--build-context") {
		t.Fatalf("the image build never carries a sibling build context: %v", args)
	}
	if args[len(args)-1] != "/repos/weaver" {
		t.Fatalf("build context must be the weaver repo root: %v", args)
	}
}

func TestWeaverImageBuildCommandStreamsDockerfile(t *testing.T) {
	plan := weaverImagePlan{Toolchain: "1.97.1"}
	cmd := newWeaverImageBuildCommand(
		"weaver-e2e-weaver:local",
		"/repos/weaver",
		plan,
		"abc123",
	)
	if joined := strings.Join(cmd.Args, " "); !strings.Contains(joined, " -f - ") {
		t.Fatalf("generated Dockerfile must be read from stdin: %v", cmd.Args)
	}
	got, err := io.ReadAll(cmd.Stdin)
	if err != nil {
		t.Fatalf("read Dockerfile stdin: %v", err)
	}
	if string(got) != plan.dockerfile() {
		t.Fatal("Dockerfile stdin does not match the rendered image plan")
	}
}

func TestWeaverImageFingerprintTracksBuildInputs(t *testing.T) {
	root := filepath.Join(t.TempDir(), "weaver")
	writeFile(t, filepath.Join(root, "Cargo.toml"), publishedCargoToml)
	writeFile(t, filepath.Join(root, "rust-toolchain.toml"), "[toolchain]\nchannel = \"1.97.1\"\n")
	writeFile(t, filepath.Join(root, ".dockerignore"), "target\ne2e\n")
	writeFile(t, filepath.Join(root, "server/src/main.rs"), "fn main() {}\n")
	writeFile(t, filepath.Join(root, "target/release/weaver"), "ignored build output")
	writeFile(t, filepath.Join(root, "e2e/internal/weaver/main.go"), "package weaver\n")

	plan := weaverImagePlan{Toolchain: "1.97.1"}
	initial := fingerprintForTest(t, root, plan)
	if repeated := fingerprintForTest(t, root, plan); repeated != initial {
		t.Fatalf("unchanged inputs produced different fingerprints: %s != %s", initial, repeated)
	}

	writeFile(t, filepath.Join(root, "target/release/weaver"), "new ignored build output")
	if ignored := fingerprintForTest(t, root, plan); ignored != initial {
		t.Fatalf("ignored target output changed fingerprint: %s != %s", initial, ignored)
	}

	writeFile(t, filepath.Join(root, "e2e/internal/weaver/main.go"), "package weaver\n// changed\n")
	if ignored := fingerprintForTest(t, root, plan); ignored != initial {
		t.Fatalf("ignored E2E source changed fingerprint: %s != %s", ignored, initial)
	}

	writeFile(t, filepath.Join(root, "server/src/main.rs"), "fn main() { println!(\"changed\"); }\n")
	dirty := fingerprintForTest(t, root, plan)
	if dirty == initial {
		t.Fatal("modified source did not change fingerprint")
	}

	writeFile(t, filepath.Join(root, "server/src/untracked.rs"), "pub fn untracked() {}\n")
	untracked := fingerprintForTest(t, root, plan)
	if untracked == dirty {
		t.Fatal("new untracked source did not change fingerprint")
	}

	if differentPlan := fingerprintForTest(t, root, weaverImagePlan{Toolchain: "stable"}); differentPlan == untracked {
		t.Fatal("generated Dockerfile/toolchain change did not change fingerprint")
	}
}

func TestNewWeaverImagePlanReadsTheWorkingTree(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "Cargo.toml"), publishedCargoToml)
	writeFile(t, filepath.Join(root, "rust-toolchain.toml"), "[toolchain]\nchannel = \"1.97.1\"\n")

	plan, err := newWeaverImagePlan(root)
	if err != nil {
		t.Fatalf("newWeaverImagePlan returned error: %v", err)
	}
	if plan.Toolchain != "1.97.1" {
		t.Fatalf("toolchain = %q, want %q", plan.Toolchain, "1.97.1")
	}
}

func TestNewWeaverImagePlanRefusesSiblingCheckoutPatches(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "Cargo.toml"), patchedCargoToml)
	writeFile(t, filepath.Join(root, "rust-toolchain.toml"), "[toolchain]\nchannel = \"1.97.1\"\n")
	if _, err := newWeaverImagePlan(root); err == nil || !strings.Contains(err.Error(), "outside the repository") {
		t.Fatalf("a [patch.crates-io] path that climbs out of the repo must be refused, got %v", err)
	}
}

func TestRejectOutOfTreePatchesAcceptsInRepoAndRefusesAbsolute(t *testing.T) {
	root := t.TempDir()
	if err := rejectOutOfTreePatches(root, "[patch.crates-io]\npar2-rs = { path = \"engines/par2\" }\n"); err != nil {
		t.Fatalf("an in-repository patch path is fine: %v", err)
	}
	if err := rejectOutOfTreePatches(root, "[patch.crates-io]\npar2-rs = { path = \"/repos/rarpar/crates/par2\" }\n"); err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("an absolute patch path must be refused, got %v", err)
	}
	if err := rejectOutOfTreePatches(root, "[patch.crates-io]\npar2-rs = { path = \"../rarpar/crates/par2\" }\n"); err == nil {
		t.Fatal("a sibling patch path must be refused")
	}
}

// weaver a8faf19d rejects the compose service name with 421. The allowlist entry
// belongs to the deployment, not the image, so that it also holds when
// E2E_WEAVER_IMAGE points at the published image.
func TestWeaverComposeDeclaresTheServiceHostnameOnTheAllowlist(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(weaverE2ETestRoot(t), "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker compose file: %v", err)
	}
	const mapping = `WEAVER_HTTP_ALLOWED_HOSTS: "${E2E_WEAVER_ALLOWED_HOSTS:-weaver}"`
	if !strings.Contains(string(body), mapping) {
		t.Fatalf("Weaver Compose service is missing the Host allowlist mapping %q; in-network clients get 421", mapping)
	}
}

func writeFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func fingerprintForTest(t *testing.T, root string, plan weaverImagePlan) string {
	t.Helper()
	fingerprint, err := weaverImageFingerprint(root, plan)
	if err != nil {
		t.Fatalf("weaverImageFingerprint returned error: %v", err)
	}
	return fingerprint
}
