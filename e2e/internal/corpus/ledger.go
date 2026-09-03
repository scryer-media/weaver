package corpus

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Ledger is test-corpus/sources.json: the provenance record for every fixture
// path the harness can read.
type Ledger struct {
	SchemaVersion int                  `json:"schema_version"`
	Toolchains    string               `json:"toolchains"`
	Generators    map[string]Generator `json:"generators"`
	Files         []FileEntry          `json:"files"`
}

// Generator is one fixture-producing script and the toolchain ids it invokes.
type Generator struct {
	Path             string   `json:"path"`
	Toolchains       []string `json:"toolchains"`
	ByteReproducible bool     `json:"byte_reproducible"`
	Notes            string   `json:"notes,omitempty"`
}

// FileEntry is one fixture: where it lives, what it is, and where it came from.
//
// A **salted** entry is the exception to all of that. Some writers draw a random
// salt or IV for every archive they produce — 7-Zip's AES chains do, with no
// switch to fix them — so those bytes differ on every machine that generates
// them and there is no digest to pin. Such an entry carries no hash and no
// size, and its committed form therefore never moves: `sources.json` is itself
// a fingerprint input, and an entry that changed on each run would move the
// fingerprint with it.
//
// The cost is real and deliberate: a salted fixture is verified by *presence*
// only, never by content. It is an escape hatch, and [`Ledger.Validate`] keeps
// it narrow — see the byte-reproducibility guard in the generator.
type FileEntry struct {
	Path string `json:"path"`
	// Salted marks an entry whose bytes cannot be pinned. Size and BLAKE3 must
	// both be empty when it is set.
	Salted bool   `json:"salted,omitempty"`
	Size   int64  `json:"size"`
	BLAKE3 string `json:"blake3"`
	Format string `json:"format,omitempty"`
	Source Source `json:"source"`
}

// Source is either `generated` (generator + toolchains + inputs) or `blocked`
// (a generator does not exist yet; publication is refused while any remain).
type Source struct {
	Kind       string   `json:"kind"`
	Generator  string   `json:"generator,omitempty"`
	Toolchains []string `json:"toolchains,omitempty"`
	Inputs     []string `json:"inputs,omitempty"`
	Notes      string   `json:"notes,omitempty"`
	Reason     string   `json:"reason,omitempty"`
}

// LoadLedger reads and validates the ledger under root against the toolchain
// lock it names.
func LoadLedger(root string) (*Ledger, ToolchainLock, error) {
	contents, err := os.ReadFile(HostPath(root, LedgerFile))
	if err != nil {
		return nil, ToolchainLock{}, fmt.Errorf("read ledger: %w", err)
	}
	var ledger Ledger
	if err := json.Unmarshal(contents, &ledger); err != nil {
		return nil, ToolchainLock{}, fmt.Errorf("decode %s: %w", LedgerFile, err)
	}
	if ledger.Toolchains == "" {
		ledger.Toolchains = ToolchainsFile
	}
	lock, err := LoadToolchainLock(HostPath(root, ledger.Toolchains))
	if err != nil {
		return nil, ToolchainLock{}, err
	}
	if err := ledger.Validate(lock); err != nil {
		return nil, ToolchainLock{}, err
	}
	return &ledger, lock, nil
}

// Validate enforces the ledger rules: schema, unique root-relative paths,
// digests, generators that exist, toolchains that exist in the lock and that
// the generator declares, and a reason on every blocked entry.
func (ledger *Ledger) Validate(lock ToolchainLock) error {
	var problems []string
	problem := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	if ledger.SchemaVersion != SchemaVersion {
		problem("schema_version %d is not %d", ledger.SchemaVersion, SchemaVersion)
	}
	if !ValidRelativePath(ledger.Toolchains) {
		problem("toolchains path %q is not root-relative", ledger.Toolchains)
	}
	for name, generator := range ledger.Generators {
		if !ValidRelativePath(generator.Path) {
			problem("generator %s: path %q is not root-relative", name, generator.Path)
		}
		for _, id := range generator.Toolchains {
			if !lock.Has(id) {
				problem("generator %s: toolchain %q is not in %s", name, id, ledger.Toolchains)
			}
		}
	}
	seen := make(map[string]struct{}, len(ledger.Files))
	for _, file := range ledger.Files {
		if !ValidRelativePath(file.Path) {
			problem("file %q: path is not root-relative", file.Path)
			continue
		}
		if _, duplicate := seen[file.Path]; duplicate {
			problem("file %s: listed more than once", file.Path)
		}
		seen[file.Path] = struct{}{}
		if file.Size < 0 {
			problem("file %s: negative size", file.Path)
		}
		if file.Salted {
			// Nothing about a salted entry may move, or the ledger stops being
			// a stable fingerprint input. Carrying a hash or a size would also
			// invite a reader to compare against them.
			if file.BLAKE3 != "" {
				problem("file %s: salted entries carry no blake3, got %q", file.Path, file.BLAKE3)
			}
			if file.Size != 0 {
				problem("file %s: salted entries carry no size, got %d", file.Path, file.Size)
			}
		} else if !IsDigest(file.BLAKE3) {
			problem("file %s: blake3 %q is not a lowercase 64-hex digest", file.Path, file.BLAKE3)
		}
		switch file.Source.Kind {
		case SourceGenerated:
			generator, ok := ledger.Generators[file.Source.Generator]
			if !ok {
				problem("file %s: generator %q is not declared", file.Path, file.Source.Generator)
				continue
			}
			declared := make(map[string]struct{}, len(generator.Toolchains))
			for _, id := range generator.Toolchains {
				declared[id] = struct{}{}
			}
			for _, id := range file.Source.Toolchains {
				if !lock.Has(id) {
					problem("file %s: toolchain %q is not in %s", file.Path, id, ledger.Toolchains)
				} else if _, ok := declared[id]; !ok {
					problem("file %s: toolchain %q is not declared by generator %s", file.Path, id, file.Source.Generator)
				}
			}
			for _, input := range file.Source.Inputs {
				if !ValidRelativePath(input) {
					problem("file %s: input %q is not root-relative", file.Path, input)
				}
			}
			if file.Source.Reason != "" {
				problem("file %s: generated entries do not carry a reason", file.Path)
			}
		case SourceBlocked:
			if strings.TrimSpace(file.Source.Reason) == "" {
				problem("file %s: blocked entries need a reason", file.Path)
			}
			if file.Source.Generator != "" || len(file.Source.Toolchains) != 0 || len(file.Source.Inputs) != 0 {
				problem("file %s: blocked entries carry no generator, toolchains or inputs", file.Path)
			}
		default:
			problem("file %s: unsupported source kind %q (only %q and %q exist)", file.Path, file.Source.Kind, SourceGenerated, SourceBlocked)
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return errors.New("ledger is invalid:\n  " + strings.Join(problems, "\n  "))
}

// Blocked lists the entries whose generator does not exist yet.
func (ledger *Ledger) Blocked() []FileEntry {
	var blocked []FileEntry
	for _, file := range ledger.Files {
		if file.Source.Kind == SourceBlocked {
			blocked = append(blocked, file)
		}
	}
	return blocked
}

// Paths returns every ledger path, sorted.
func (ledger *Ledger) Paths() []string {
	paths := make([]string, 0, len(ledger.Files))
	for _, file := range ledger.Files {
		paths = append(paths, file.Path)
	}
	sort.Strings(paths)
	return paths
}

// Entry finds one ledger entry by path.
func (ledger *Ledger) Entry(path string) (FileEntry, bool) {
	for _, file := range ledger.Files {
		if file.Path == path {
			return file, true
		}
	}
	return FileEntry{}, false
}

// Save writes the ledger back with stable formatting (files sorted by path).
func (ledger *Ledger) Save(root string) error {
	sort.Slice(ledger.Files, func(left, right int) bool { return ledger.Files[left].Path < ledger.Files[right].Path })
	contents, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	return os.WriteFile(HostPath(root, LedgerFile), contents, 0o644)
}

// ToolchainLock is the set of pinned toolchain ids a generator may name. The
// lock file follows the rarpar shape (`rar_writers`, `video_encoder`,
// `par2_generator`, plus any additional arrays); every object with an `id`
// anywhere in the document contributes one id.
type ToolchainLock struct {
	Path   string
	BLAKE3 string
	IDs    []string
	ids    map[string]struct{}
}

// LoadToolchainLock reads a toolchain lock and collects its ids.
func LoadToolchainLock(path string) (ToolchainLock, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return ToolchainLock{}, fmt.Errorf("read toolchain lock: %w", err)
	}
	var document any
	if err := json.Unmarshal(contents, &document); err != nil {
		return ToolchainLock{}, fmt.Errorf("decode toolchain lock %s: %w", path, err)
	}
	lock := ToolchainLock{Path: path, BLAKE3: DigestBytes(contents), ids: map[string]struct{}{}}
	collectIDs(document, &lock)
	sort.Strings(lock.IDs)
	if len(lock.IDs) == 0 {
		return ToolchainLock{}, fmt.Errorf("toolchain lock %s declares no toolchain ids", path)
	}
	return lock, nil
}

func collectIDs(node any, lock *ToolchainLock) {
	switch typed := node.(type) {
	case map[string]any:
		if id, ok := typed["id"].(string); ok && id != "" {
			if _, seen := lock.ids[id]; !seen {
				lock.ids[id] = struct{}{}
				lock.IDs = append(lock.IDs, id)
			}
		}
		for _, value := range typed {
			collectIDs(value, lock)
		}
	case []any:
		for _, value := range typed {
			collectIDs(value, lock)
		}
	}
}

// Has reports whether id is pinned by the lock.
func (lock ToolchainLock) Has(id string) bool {
	_, ok := lock.ids[id]
	return ok
}
