// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"context"
	"fmt"
	"path"
	"strings"
)

// The scene "zip with diz" shape: an inner RAR set whose volumes are each
// packed into a stored zip of their own, beside a `file_id.diz` and an
// `.nfo`, as scene groups pack comic and ebook releases. The zips are posted
// at the root, or are themselves the members of an outer RAR set: RAR inside
// zip inside RAR. Every byte arrives; the inner set unpacks to the end files.
//
// RARLAB writes both RAR layers; Go writes only the zips and the payloads.
// Every member is stored, so the volume counts below follow from the sizes
// alone and the ledger can name every file before the recipe has run.

const (
	// sceneZipsInnerBase names the inner set: lbcn0101.rar, then .r00 onward.
	sceneZipsInnerBase = "lbcn0101"
	// sceneZipsInnerVolume is RARLAB's `-v` in bytes. The end files total
	// 1,269,760 bytes, so four full volumes hold 1,200,000 and the fifth the
	// remaining 69,760 or so after headers.
	sceneZipsInnerVolume = "300000b"
	// sceneZipsInnerVolumes is how many volumes the inner set is cut into.
	sceneZipsInnerVolumes = 5
	// sceneZipsOuterVolume is the outer set's `-v` in bytes. The five zips
	// total about 1,280,000 bytes, so three full volumes hold 1,200,000 and the
	// fourth the remaining 80,000 or so.
	sceneZipsOuterVolume = "400000b"
	// sceneZipsOuterVolumes is how many volumes the outer set is cut into.
	sceneZipsOuterVolumes = 4
	// sceneZipsDIZ and sceneZipsNFO sit beside the volume in every zip, under
	// the same names in each, as the groups post them.
	sceneZipsDIZ     = "file_id.diz"
	sceneZipsNFO     = sceneZipsInnerBase + ".nfo"
	sceneZipsDIZSize = 128
	sceneZipsNFOSize = 1536
)

// sceneZipsEndFile is one file of the inner set: what the job must deliver.
type sceneZipsEndFile struct {
	Name string
	Size int64
}

// sceneZipsEndFiles are the inner set's members, in the order RARLAB adds
// them. The first spans the first three inner volumes, the second and third
// each cross a volume boundary, and the last sits whole in the fifth.
var sceneZipsEndFiles = []sceneZipsEndFile{
	{Name: "Lantern.Beacon.001.2026.pdf", Size: 640 << 10},
	{Name: "Lantern.Beacon.002.2026.pdf", Size: 384 << 10},
	{Name: "Lantern.Beacon.003.2026.pdf", Size: 160 << 10},
	{Name: "Lantern.Beacon.Cover.2026.jpg", Size: 56 << 10},
}

// SceneZipRecipes are the two placements of the zips: inside an outer RAR set,
// and at the root of the posting.
func SceneZipRecipes() []Recipe {
	return []Recipe{
		sceneZipsRecipe("scene-zips-outer-rar", true),
		sceneZipsRecipe("scene-zips-root", false),
	}
}

// sceneZipsInnerVolumeNames are the old-style names RARLAB's `-vn` gives the
// inner set: the first volume `.rar`, then `.r00` onward.
func sceneZipsInnerVolumeNames() []string {
	names := make([]string, 0, sceneZipsInnerVolumes)
	names = append(names, sceneZipsInnerBase+".rar")
	for index := 0; index < sceneZipsInnerVolumes-1; index++ {
		names = append(names, fmt.Sprintf("%s.r%02d", sceneZipsInnerBase, index))
	}
	return names
}

// sceneZipsZipNames are the zips, one per inner volume, lettered from `a`.
func sceneZipsZipNames() []string {
	names := make([]string, 0, sceneZipsInnerVolumes)
	for index := 0; index < sceneZipsInnerVolumes; index++ {
		names = append(names, fmt.Sprintf("%s%c.zip", sceneZipsInnerBase, 'a'+index))
	}
	return names
}

// sceneZipsOuterVolumeNames are the outer set's volumes, RARLAB's RAR5
// `.partN.rar` names.
func sceneZipsOuterVolumeNames() []string {
	names := make([]string, 0, sceneZipsOuterVolumes)
	for index := 1; index <= sceneZipsOuterVolumes; index++ {
		names = append(names, fmt.Sprintf("archive.part%d.rar", index))
	}
	return names
}

// sceneZipsOutputs is the exact file set a recipe posts.
func sceneZipsOutputs(outer bool) []string {
	if outer {
		return sceneZipsOuterVolumeNames()
	}
	return sceneZipsZipNames()
}

// sceneZipsSeed keys an end file's payload to its scenario, so the two
// placements never post the same bytes.
func sceneZipsSeed(slug, name string) string {
	return slug + "/" + name
}

// sceneZipsDIZText is volume `index`'s diz: the groups number each zip's.
func sceneZipsDIZText(index int) string {
	return fmt.Sprintf("Lantern Beacon No. 01 (2026)\r\nCOMiC eBook - TESTGRP\r\n[%02d/%02d]\r\n", index+1, sceneZipsInnerVolumes)
}

const sceneZipsNFOText = "Lantern Beacon No. 01 (2026) COMiC eBook - TESTGRP\r\n" +
	"An invented release for the Weaver e2e corpus.\r\n" +
	"Unzip every lbcn0101?.zip, then unrar lbcn0101.rar.\r\n"

func sceneZipsRecipe(slug string, outer bool) Recipe {
	placement := "at the root of the posting"
	if outer {
		placement = fmt.Sprintf("as the members of an outer stored RAR5 set of %d volumes (RARLAB 7.23)", sceneZipsOuterVolumes)
	}
	return Recipe{
		// RARLAB stamps times into both RAR layers, and the zips carry the
		// inner volumes, so a rebuild moves every posted digest; the end
		// files are seeded and never move.
		Slug: slug, Family: "scene zips", ByteReproducible: false,
		Notes: fmt.Sprintf("A scene zip-with-diz release: a stored RAR4 set of %d old-style volumes (RARLAB 6.24 -ma4 -vn) over %d end files, "+
			"each volume alone in a stored zip beside a file_id.diz and an .nfo, the zips %s. "+
			"The job must end Complete with every end file delivered whole and no zip or RAR volume handed over.",
			sceneZipsInnerVolumes, len(sceneZipsEndFiles), placement),
		Build: func(ctx context.Context, env *Env) error {
			return buildSceneZips(ctx, env, slug, outer)
		},
		ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
			outputs := make(map[string]string, len(sceneZipsEndFiles))
			for _, file := range sceneZipsEndFiles {
				outputs[file.Name] = env.StagePath(file.Name)
			}
			return outputs, nil
		},
	}
}

func buildSceneZips(ctx context.Context, env *Env, slug string, outer bool) error {
	members := make([]string, 0, len(sceneZipsEndFiles))
	for _, file := range sceneZipsEndFiles {
		if err := WritePRNG(env.StagePath(file.Name), sceneZipsSeed(slug, file.Name), file.Size); err != nil {
			return err
		}
		members = append(members, file.Name)
	}

	// The inner set, written into the output directory, where RARLAB puts
	// every archive; each volume leaves it again once it is zipped.
	if err := env.RAR(ctx, RARSpec{
		Toolchain: RAR4Writer, Format: RAR4, Archive: sceneZipsInnerBase + ".rar",
		Method: "-m0", VolumeSize: sceneZipsInnerVolume, Members: members,
		Extra: []string{"-vn"},
	}); err != nil {
		return err
	}
	volumes := sceneZipsInnerVolumeNames()
	if err := requireOutputs(env, volumes); err != nil {
		return fmt.Errorf("inner set: %w", err)
	}

	zips := sceneZipsZipNames()
	for index, volume := range volumes {
		destination := env.OutputPath(zips[index])
		if outer {
			destination = env.StagePath(zips[index])
		}
		scratch := path.Join("layer", zips[index])
		if err := writeSceneZip(destination, env.OutputPath(volume), volume, env.StagePath(path.Join(scratch, sceneZipsDIZ)), env.StagePath(path.Join(scratch, sceneZipsNFO)), index); err != nil {
			return err
		}
		if err := removeOutput(env, volume); err != nil {
			return err
		}
	}

	if outer {
		// The zips are Go's; the outer layer names that, as an extension
		// cannot once they sit inside a RAR set.
		env.usedToolchain("go-archive-zip@go1.27.3")
		if err := env.RAR(ctx, RARSpec{
			Toolchain: RAR5Writer, Format: RAR5, Archive: "archive.rar",
			Method: "-m0", VolumeSize: sceneZipsOuterVolume, Members: zips,
		}); err != nil {
			return err
		}
	}
	return requireOutputs(env, sceneZipsOutputs(outer))
}

// writeSceneZip writes one stored zip holding the diz, the nfo and one inner
// volume, in that order, as the groups pack them. dizPath and nfoPath are
// where the two text files are written before they are stored.
func writeSceneZip(destination, volumePath, volumeName, dizPath, nfoPath string, index int) error {
	if err := WriteText(dizPath, sceneZipsDIZText(index), sceneZipsDIZSize); err != nil {
		return err
	}
	if err := WriteText(nfoPath, sceneZipsNFOText, sceneZipsNFOSize); err != nil {
		return err
	}
	return WriteZip(destination, []Member{
		{Name: sceneZipsDIZ, Source: dizPath},
		{Name: sceneZipsNFO, Source: nfoPath},
		{Name: volumeName, Source: volumePath},
	}, "")
}

// requireOutputs fails unless the output directory holds exactly want, so a
// writer that cuts a different number of volumes stops the recipe with both
// lists rather than at the ledger.
func requireOutputs(env *Env, want []string) error {
	got, err := env.Outputs()
	if err != nil {
		return err
	}
	expected := map[string]bool{}
	for _, name := range want {
		expected[name] = true
	}
	matches := len(got) == len(want)
	for _, name := range got {
		if !expected[name] {
			matches = false
		}
	}
	if !matches {
		return fmt.Errorf("outputs are %s, want %s", strings.Join(got, ", "), strings.Join(want, ", "))
	}
	return nil
}
