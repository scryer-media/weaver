// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestAdversarialNZBRecipesAreTheFamily(t *testing.T) {
	seen := map[string]bool{}
	for _, recipe := range AdversarialNZBRecipes() {
		if !strings.HasPrefix(recipe.Slug, "adv-nzb-") {
			t.Errorf("%s is not an adv-nzb- slug", recipe.Slug)
		}
		if recipe.Family != "adversarial" || !recipe.ByteReproducible || recipe.Build == nil {
			t.Errorf("%s: family %q, reproducible %v", recipe.Slug, recipe.Family, recipe.ByteReproducible)
		}
		if seen[recipe.Slug] {
			t.Errorf("%s is declared twice", recipe.Slug)
		}
		seen[recipe.Slug] = true
	}
	for _, staged := range []string{"adv-nzb-yenc-size-lie-tail-lost", "adv-nzb-yenc-size-lie-int64"} {
		if _, ok := ScenarioOnly[staged]; !ok || seen[staged] {
			t.Errorf("%s must be scenario-only", staged)
		}
	}
}

// Each hostile name is a name a filesystem accepts, and every one but the
// ordinary control carries a character wider than one byte.
func TestHostileNZBNamesAreWritableAndMultibyte(t *testing.T) {
	for _, names := range [][]string{ClassifierHostileNames, BackupHostileNames} {
		ordinary := 0
		for _, name := range names {
			if !utf8.ValidString(name) || strings.ContainsAny(name, "/\\\x00") {
				t.Errorf("%q is not a plain file name", name)
			}
			if strings.HasPrefix(name, "ordinary-") {
				ordinary++
				continue
			}
			if len(name) == utf8.RuneCountInString(name) {
				t.Errorf("%q has no multibyte character", name)
			}
		}
		if ordinary != 1 {
			t.Errorf("%d ordinary controls in %q", ordinary, names)
		}
	}
	for _, name := range BackupHostileNames[:len(BackupHostileNames)-1] {
		if !strings.HasPrefix(name, "Backup_12345S") {
			t.Errorf("%q does not open like the Backup_ pattern", name)
		}
	}
}

func TestLooseNZBRecipesWriteOnePayloadPerName(t *testing.T) {
	for _, recipe := range AdversarialNZBRecipes() {
		var names []string
		switch recipe.Slug {
		case "adv-nzb-classifier-names":
			names = ClassifierHostileNames
		case "adv-nzb-backup-names":
			names = BackupHostileNames
		case "adv-nzb-subject-traversal":
			names = SubjectHostileFiles
		default:
			continue
		}
		env := &Env{Work: t.TempDir()}
		if err := recipe.Build(t.Context(), env); err != nil {
			t.Fatalf("%s: %v", recipe.Slug, err)
		}
		outputs, err := env.Outputs()
		if err != nil {
			t.Fatal(err)
		}
		if len(outputs) != len(names) {
			t.Errorf("%s wrote %d files, want %d: %q", recipe.Slug, len(outputs), len(names), outputs)
		}
		for _, name := range names {
			body, err := os.ReadFile(env.OutputPath(name))
			if err != nil {
				t.Fatalf("%s: %v", recipe.Slug, err)
			}
			if !bytes.Equal(body, adversarialPayload(name)) {
				t.Errorf("%s: %q holds %q", recipe.Slug, name, body)
			}
		}
		if recipe.ExpectedOutputs == nil {
			continue
		}
		expected, err := recipe.ExpectedOutputs(t.Context(), env)
		if err != nil {
			t.Fatal(err)
		}
		if len(expected) != len(names) {
			t.Errorf("%s pins %d outputs, want %d", recipe.Slug, len(expected), len(names))
		}
		for name, path := range expected {
			if !bytes.Equal(read(t, path), read(t, env.OutputPath(name))) {
				t.Errorf("%s: the pinned %q is not the posted file", recipe.Slug, name)
			}
		}
	}
}

func TestNZBTarSizeLieHoldsItsMember(t *testing.T) {
	var recipe Recipe
	for _, candidate := range AdversarialNZBRecipes() {
		if candidate.Slug == "adv-nzb-tar-size-lie" {
			recipe = candidate
		}
	}
	env := &Env{Work: t.TempDir()}
	if err := recipe.Build(t.Context(), env); err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(read(t, env.OutputPath(advNZBTarArchive))))
	header, err := reader.Next()
	if err != nil {
		t.Fatalf("archive/tar refuses the encoding: %v", err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if header.Name != advNZBTarMember || len(body) != advNZBTarBytes {
		t.Fatalf("member %q of %d bytes", header.Name, len(body))
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("a second member or a bad end: %v", err)
	}
	expected, err := recipe.ExpectedOutputs(t.Context(), env)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read(t, expected[advNZBTarMember]), body) {
		t.Fatal("the pinned member is not the archived one")
	}
}
