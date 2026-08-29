package corpus

import (
	"reflect"
	"testing"
)

func TestMatchGlob(t *testing.T) {
	cases := []struct {
		glob, path string
		want       bool
	}{
		{"testdata/**", "testdata/x/a.rar", true},
		{"testdata/**", "testdata/a.rar", true},
		{"testdata/**", "testdata", true}, // ** matches zero segments
		{"testdata/**", "fixtures/a.rar", false},
		{"testdata/x/**", "testdata/x/a.rar", true},
		{"testdata/x/**", "testdata/xy/a.rar", false},
		{"testdata/*/a.rar", "testdata/x/a.rar", true},
		{"testdata/*/a.rar", "testdata/x/y/a.rar", false}, // * stays inside a segment
		{"testdata/*.rar", "testdata/a.rar", true},
		{"testdata/*.rar", "testdata/a.par2", false},
		{"testdata/archive.part*.rar", "testdata/archive.part12.rar", true},
		{"**/scenario.json", "testdata/x/scenario.json", true},
		{"**/scenario.json", "scenario.json", true},
		{"**/scenario.json", "testdata/x/scenario.json.bak", false},
		{"testdata/**/*.par2", "testdata/x/y/a.par2", true},
		{"testdata/a*c/d.bin", "testdata/abc/d.bin", true},
		{"testdata/a*c/d.bin", "testdata/ac/d.bin", true},
		{"testdata/a*b*c", "testdata/axxbyyc", true},
		{"testdata/a*b*c", "testdata/axxcyyb", false},
	}
	for _, testCase := range cases {
		if got := MatchGlob(testCase.glob, testCase.path); got != testCase.want {
			t.Errorf("MatchGlob(%q, %q) = %v, want %v", testCase.glob, testCase.path, got, testCase.want)
		}
	}
}

func TestResolveAppliesExcludesAndRefusesEmpty(t *testing.T) {
	paths := []string{
		"testdata/one/a.rar",
		"testdata/one/scenario.json",
		"testdata/two/b.par2",
		"testdata/shared/clip.mkv",
	}
	profiles := newProfiles(map[string][]string{
		"one":   {"testdata/one/**", "testdata/shared/**"},
		"empty": {"testdata/nothing/**"},
	})
	resolved, err := profiles.Resolve("one", paths)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"testdata/one/a.rar", "testdata/shared/clip.mkv"}
	if !reflect.DeepEqual(resolved, want) {
		t.Fatalf("resolved %v, want %v (scenario.json is excluded)", resolved, want)
	}
	if _, err := profiles.Resolve("empty", paths); err == nil {
		t.Fatal("a profile that resolves to nothing must be an error: the manifest never freezes an empty bundle")
	}
	if _, err := profiles.Resolve("absent", paths); err == nil {
		t.Fatal("an unknown profile must be an error")
	}
}

func TestResolveAllCoversEveryProfile(t *testing.T) {
	paths := []string{"testdata/one/a.rar", "testdata/two/b.par2"}
	profiles := newProfiles(map[string][]string{
		"one": {"testdata/one/**"},
		"two": {"testdata/two/**"},
	})
	resolved, err := profiles.ResolveAll(paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 2 || len(resolved["one"]) != 1 || len(resolved["two"]) != 1 {
		t.Fatalf("ResolveAll gave %v", resolved)
	}
}
