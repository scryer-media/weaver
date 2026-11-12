// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package fixturegen

import (
	"context"
	"os"
)

// The adversarial NZB family: posts whose names or article headers lie. The
// bytes are ordinary loose files Go writes itself; the hostile part is the
// name a file is posted under, or a header the seeder rewrites at seed time.
// The scenarios that restage these payloads under a different lie are listed
// in ScenarioOnly.

// ClassifierHostileNames are file names whose multibyte characters sit where
// the file classifier reads single bytes: after a dot, inside a volume
// suffix, or as the whole name.
var ClassifierHostileNames = []string{
	"é",
	"éabc",
	"日本",
	"aé",
	"x.r0é",
	"x.€",
	"ordinary-classifier.txt",
}

// BackupHostileNames are file names that open like the Backup_ obfuscation
// pattern and put a multibyte character where the pattern reads a digit, a
// dash or the S.
var BackupHostileNames = []string{
	"Backup_12345S1é-02",
	"Backup_12345S12-1€",
	"Backup_12345S12€02",
	"Backup_12345S\U0001d11e2-02",
	"Backup_12345S12-\U0001d11e",
	"Backup_12345Sé",
	"ordinary-backup.txt",
}

// SubjectHostileFiles are the loose files the subject traversal case posts;
// its scenario renames each one in the NZB subject only.
var SubjectHostileFiles = []string{
	"subject-a.txt",
	"subject-b.txt",
	"subject-c.txt",
	"subject-d.txt",
	"subject-plain.txt",
}

// The size-lie payload: a little over 3 MiB, posted in 256 KiB articles so
// the last article is short.
const (
	advNZBSizeLieMedia = "lie.whole.s01e02.mkv"
	advNZBSizeLieBytes = 3<<20 + 12345
)

// The tar size lie: one PRNG member inside a ustar archive Go writes.
const (
	advNZBTarArchive = "stated.tar"
	advNZBTarMember  = "inner.s01e04.mkv"
	advNZBTarBytes   = 2<<20 + 777
)

// AdversarialNZBRecipes is the adversarial NZB family.
func AdversarialNZBRecipes() []Recipe {
	return []Recipe{
		{
			Slug: "adv-nzb-classifier-names", Family: "adversarial", ByteReproducible: true,
			Notes: "Loose text files named with a multibyte character after a dot, inside a volume suffix, or as the whole name. " +
				"The classifier reads each name on the download path; every file must be delivered under its own name.",
			Build:           writeAdversarialLoose(ClassifierHostileNames),
			ExpectedOutputs: expectPayloads(identityNames(ClassifierHostileNames)),
		},
		{
			Slug: "adv-nzb-backup-names", Family: "adversarial", ByteReproducible: true,
			Notes: "Loose text files whose names open like the Backup_ obfuscation pattern with a multibyte character where the pattern reads one byte. " +
				"None is obfuscated, so every file must be delivered under its own name.",
			Build:           writeAdversarialLoose(BackupHostileNames),
			ExpectedOutputs: expectPayloads(identityNames(BackupHostileNames)),
		},
		{
			Slug: "adv-nzb-subject-traversal", Family: "adversarial", ByteReproducible: true,
			Notes: "Loose text files whose NZB subjects are rewritten at seed time to a parent path, an absolute path, a backslash parent and a parent inside a directory. " +
				"The yEnc names stay true. Nothing may land outside the job's output directory.",
			Build: writeAdversarialLoose(SubjectHostileFiles),
		},
		{
			Slug: "adv-nzb-yenc-size-lie-whole", Family: "adversarial", ByteReproducible: true,
			Notes: "A PRNG payload of a little over 3 MiB in 256 KiB articles, every =ybegin stating 3 GiB at seed time. " +
				"The whole post arrives, so the file must come out byte-equal at its real length.",
			Build: func(_ context.Context, env *Env) error {
				return WritePRNG(env.OutputPath(advNZBSizeLieMedia), "adversarial/nzb-size-lie", advNZBSizeLieBytes)
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				return map[string]string{advNZBSizeLieMedia: env.OutputPath(advNZBSizeLieMedia)}, nil
			},
		},
		{
			Slug: "adv-nzb-tar-size-lie", Family: "adversarial", ByteReproducible: true,
			Notes: "A ustar archive holding one PRNG member, posted in 256 KiB articles whose =ybegin states 500 GiB at seed time, the most the layout trusts. " +
				"The archive's end blocks arrive where its bytes end, so the member must come out byte-equal.",
			Build: func(_ context.Context, env *Env) error {
				member := env.StagePath(advNZBTarMember)
				if err := WritePRNG(member, "adversarial/nzb-tar-size-lie", advNZBTarBytes); err != nil {
					return err
				}
				body, err := os.ReadFile(member)
				if err != nil {
					return err
				}
				return writeAdversarialTar(advNZBTarArchive, []rawTarEntry{{Name: advNZBTarMember, Type: '0', Body: body}})(context.Background(), env)
			},
			ExpectedOutputs: func(_ context.Context, env *Env) (map[string]string, error) {
				member := env.StagePath("expected-" + advNZBTarMember)
				if err := WritePRNG(member, "adversarial/nzb-tar-size-lie", advNZBTarBytes); err != nil {
					return nil, err
				}
				return map[string]string{advNZBTarMember: member}, nil
			},
		},
	}
}

// writeAdversarialLoose writes one adversarialPayload file per name.
func writeAdversarialLoose(names []string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		for _, name := range names {
			if err := os.WriteFile(env.OutputPath(name), adversarialPayload(name), 0o644); err != nil {
				return err
			}
		}
		return nil
	}
}

// identityNames maps each name to itself: the file is delivered under the
// name it was posted with.
func identityNames(names []string) map[string]string {
	delivered := make(map[string]string, len(names))
	for _, name := range names {
		delivered[name] = name
	}
	return delivered
}
