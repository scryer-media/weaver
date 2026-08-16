package corpus

import (
	"strings"
	"testing"
)

func TestLedgerValidateAcceptsAWellFormedLedger(t *testing.T) {
	root := writeTree(t, nil)
	ledger := newLedger(
		entry("testdata/one/a.rar", "aaa", generatedSource()),
		entry("testdata/two/b.par2", "bbb", blockedSource("generator pending: par2 set")),
	)
	if err := ledger.Validate(loadToolchains(t, root)); err != nil {
		t.Fatal(err)
	}
	if got := len(ledger.Blocked()); got != 1 {
		t.Fatalf("%d blocked entries, want 1", got)
	}
	if got := ledger.Paths(); len(got) != 2 || got[0] != "testdata/one/a.rar" {
		t.Fatalf("Paths() = %v", got)
	}
	if _, ok := ledger.Entry("testdata/two/b.par2"); !ok {
		t.Fatal("Entry should find a listed path")
	}
	if _, ok := ledger.Entry("testdata/nope"); ok {
		t.Fatal("Entry should not invent a path")
	}
}

func TestLedgerValidateRejectsBadEntries(t *testing.T) {
	toolchains := loadToolchains(t, writeTree(t, nil))
	cases := []struct {
		name    string
		mutate  func(*Ledger)
		problem string
	}{
		{"unknown source kind", func(ledger *Ledger) {
			ledger.Files[0].Source = Source{Kind: "upstream"}
		}, "unsupported source kind"},
		{"generated names an undeclared generator", func(ledger *Ledger) {
			ledger.Files[0].Source.Generator = "elsewhere.sh"
		}, "is not declared"},
		{"generated names a toolchain the lock does not pin", func(ledger *Ledger) {
			ledger.Files[0].Source.Toolchains = []string{"rarlab-9.99"}
		}, "is not in test-corpus/toolchains.json"},
		{"generated names a toolchain its generator does not declare", func(ledger *Ledger) {
			ledger.Files[0].Source.Toolchains = []string{"par2cmdline-turbo-1.4.0"}
		}, "is not declared by generator"},
		{"generated carries a blocked reason", func(ledger *Ledger) {
			ledger.Files[0].Source.Reason = "why"
		}, "generated entries do not carry a reason"},
		{"blocked without a reason", func(ledger *Ledger) {
			ledger.Files[1].Source = Source{Kind: SourceBlocked}
		}, "blocked entries need a reason"},
		{"blocked claiming a generator", func(ledger *Ledger) {
			ledger.Files[1].Source.Generator = "make.sh"
		}, "blocked entries carry no generator"},
		{"duplicate path", func(ledger *Ledger) {
			ledger.Files[1].Path = ledger.Files[0].Path
		}, "listed more than once"},
		{"path escapes the root", func(ledger *Ledger) {
			ledger.Files[0].Path = "../elsewhere/a.rar"
		}, "path is not root-relative"},
		{"digest is not blake3 hex", func(ledger *Ledger) {
			ledger.Files[0].BLAKE3 = "not-a-digest"
		}, "is not a lowercase 64-hex digest"},
		{"wrong schema", func(ledger *Ledger) {
			ledger.SchemaVersion = 2
		}, "schema_version"},
		{"generator pins an unknown toolchain", func(ledger *Ledger) {
			generator := ledger.Generators["make.sh"]
			generator.Toolchains = []string{"rarlab-0.01"}
			ledger.Generators["make.sh"] = generator
		}, "is not in test-corpus/toolchains.json"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ledger := newLedger(
				entry("testdata/one/a.rar", "aaa", generatedSource()),
				entry("testdata/two/b.par2", "bbb", blockedSource("generator pending")),
			)
			testCase.mutate(ledger)
			err := ledger.Validate(toolchains)
			if err == nil {
				t.Fatalf("expected %q to be rejected", testCase.name)
			}
			if !strings.Contains(err.Error(), testCase.problem) {
				t.Fatalf("error %q does not mention %q", err, testCase.problem)
			}
		})
	}
}

func TestToolchainLockCollectsEveryIDAndDigestsItself(t *testing.T) {
	root := writeTree(t, nil)
	lock := loadToolchains(t, root)
	if !lock.Has("rarlab-7.23") || !lock.Has("par2cmdline-turbo-1.4.0") {
		t.Fatalf("ids %v should include both pinned toolchains", lock.IDs)
	}
	if lock.Has("rarlab-6.24") {
		t.Fatal("an id the lock does not pin must not be accepted")
	}
	if !IsDigest(lock.BLAKE3) {
		t.Fatalf("lock digest %q is not a digest", lock.BLAKE3)
	}
}

func TestLoadLedgerRoundTrips(t *testing.T) {
	root := writeTree(t, map[string]string{"testdata/one/a.rar": "aaa"})
	ledger := newLedger(entry("testdata/one/a.rar", "aaa", generatedSource()))
	if err := ledger.Save(root); err != nil {
		t.Fatal(err)
	}
	reloaded, toolchains, err := LoadLedger(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded.Files) != 1 || reloaded.Files[0].BLAKE3 != ledger.Files[0].BLAKE3 {
		t.Fatalf("reloaded %+v", reloaded.Files)
	}
	if toolchains.Path == "" {
		t.Fatal("the toolchain lock should be reported alongside the ledger")
	}
}
