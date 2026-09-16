package fixturegen

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	// pristineDir holds copies of protected files taken before a recipe damages
	// or withholds them, so the repair proof has something to compare against.
	pristineDir = "pristine"
	// proofDir prefixes the scratch copies of the output directory the reference
	// repairs, one per proof. The published output is never repaired in place.
	proofDir = "par3-proof"
)

// PAR3Spec is a par3cmdline `create` run over files already in the output
// directory. Every knob is the reference's own switch under a shape name, so a
// recipe states the geometry it depends on rather than leaving it to the
// reference's defaults.
type PAR3Spec struct {
	// Index is the index file name, for example "amber.lattice.s01e03.mkv.par3".
	// Recovery files take their names from it.
	Index string
	// Sources are output-relative file names the set covers.
	Sources []string
	// BlockSize fixes the block size in bytes (`-s`). At most one of BlockSize
	// and BlockCount is set.
	BlockSize int64
	// BlockCount fixes the number of input blocks (`-b`).
	BlockCount int
	// RecoveryBlocks is the `-c` recovery block count.
	RecoveryBlocks int
	// RedundancyPercent is the `-r` alternative to RecoveryBlocks.
	RedundancyPercent int
	// RecoveryFiles fixes the number of recovery files (`-n`). Zero leaves the
	// reference's doubling layout in place.
	RecoveryFiles int
	// ECC selects the erasure code (`-e`): 1 is Cauchy Reed-Solomon, 8 is
	// FFT-based Reed-Solomon. Zero leaves the reference's choice.
	ECC int
	// Interleave splits the blocks into this many cohorts (`-i`). FFT only.
	Interleave int
	// Dedup enables input-block deduplication (`-d`): 1 aligned, 2 sliding.
	Dedup int
	// StoreData writes Data packets (`-D`), so the set carries the input
	// itself in `.partNN+NN.par3` files alongside the recovery volumes.
	StoreData bool
}

func (spec PAR3Spec) arguments() ([]string, error) {
	if spec.Index == "" || len(spec.Sources) == 0 {
		return nil, fmt.Errorf("a PAR3 set needs an index name and at least one source")
	}
	args := []string{"create", "-q"}
	switch {
	case spec.BlockSize > 0 && spec.BlockCount > 0:
		return nil, fmt.Errorf("PAR3 set %q names both a block size and a block count", spec.Index)
	case spec.BlockSize > 0:
		args = append(args, "-s"+strconv.FormatInt(spec.BlockSize, 10))
	case spec.BlockCount > 0:
		args = append(args, "-b"+strconv.Itoa(spec.BlockCount))
	}
	switch {
	case spec.RecoveryBlocks > 0 && spec.RedundancyPercent > 0:
		return nil, fmt.Errorf("PAR3 set %q names both a recovery block count and a redundancy", spec.Index)
	case spec.RecoveryBlocks > 0:
		args = append(args, "-c"+strconv.Itoa(spec.RecoveryBlocks))
	case spec.RedundancyPercent > 0:
		args = append(args, "-r"+strconv.Itoa(spec.RedundancyPercent))
	default:
		return nil, fmt.Errorf("PAR3 set %q needs recovery blocks or a redundancy percentage", spec.Index)
	}
	if spec.RecoveryFiles > 0 {
		args = append(args, "-n"+strconv.Itoa(spec.RecoveryFiles))
	}
	if spec.ECC > 0 {
		args = append(args, "-e"+strconv.Itoa(spec.ECC))
	}
	if spec.Interleave > 0 {
		args = append(args, "-i"+strconv.Itoa(spec.Interleave))
	}
	if spec.Dedup > 0 {
		args = append(args, "-d"+strconv.Itoa(spec.Dedup))
	}
	if spec.StoreData {
		args = append(args, "-D")
	}
	args = append(args, spec.Index)
	return append(args, spec.Sources...), nil
}

// PAR3 creates a recovery set with the pinned par3cmdline reference.
func (env *Env) PAR3(ctx context.Context, spec PAR3Spec) error {
	args, err := spec.arguments()
	if err != nil {
		return err
	}
	toolchain, err := env.par3Toolchain(ctx)
	if err != nil {
		return err
	}
	return env.Docker.Run(ctx, toolchain, env.Work, outputDir, args...)
}

// PAR3Insert has the reference insert self-protection into a ZIP or 7z already
// in the output directory, at the given redundancy. The container keeps its
// name and stays readable by any ordinary extractor; the recovery data rides
// in the space the format lets a writer append.
func (env *Env) PAR3Insert(ctx context.Context, archive string, redundancyPercent int) error {
	if redundancyPercent <= 0 {
		return fmt.Errorf("PAR3 insertion into %s needs a redundancy percentage", archive)
	}
	toolchain, err := env.par3Toolchain(ctx)
	if err != nil {
		return err
	}
	return env.Docker.Run(ctx, toolchain, env.Work, outputDir,
		"insert", "-q", "-r"+strconv.Itoa(redundancyPercent), archive)
}

func (env *Env) par3Toolchain(ctx context.Context) (Toolchain, error) {
	toolchain, err := env.Lock.Find(PAR3Toolchain)
	if err != nil {
		return Toolchain{}, err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return Toolchain{}, err
	}
	env.usedToolchain(PAR3Toolchain)
	return toolchain, nil
}

// par3Snapshot keeps a copy of each named output before a later step damages
// or withholds it.
func par3Snapshot(names ...string) func(context.Context, *Env) error {
	return func(_ context.Context, env *Env) error {
		for _, name := range names {
			target := filepath.Join(env.Work, pristineDir, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := CopyFile(env.OutputPath(name), target); err != nil {
				return err
			}
		}
		return nil
	}
}

// PAR3Proof names what the reference is asked to repair.
type PAR3Proof struct {
	// Index is the loose set's index file, or the self-protected container
	// when Inserted is set.
	Index string
	// Inserted repairs a container through its own inserted protection (`rs`)
	// rather than through a loose set (`repair`).
	Inserted bool
	// Restored are the snapshotted outputs the repair must reproduce byte for
	// byte.
	Restored []string
	// Omit are outputs the posting will not carry, so the reference must not
	// see them either: a withheld index, or recovery files the scenario drops.
	Omit []string
	// Extra are further output-relative files handed to the reference to
	// search, for an input posted under a name the set does not record.
	Extra []string
}

// par3ProveRepair runs the reference's repair over a scratch copy of the
// output directory and fails the recipe unless every restored file comes back
// identical to its snapshot.
//
// par3cmdline reports an impossible repair and still exits zero, so the verdict
// is the bytes, never the exit status. A damage recipe that stops being
// repairable — a payload that grew, a geometry that moved — fails here, at
// generation time, instead of surfacing as a weaver failure nobody can explain.
func par3ProveRepair(proof PAR3Proof) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		if len(proof.Restored) == 0 {
			return fmt.Errorf("a PAR3 repair proof over %s must name what it restores", proof.Index)
		}
		transcript, err := env.par3RunProof(ctx, proof)
		if err != nil {
			return err
		}
		for _, name := range proof.Restored {
			want, err := os.ReadFile(filepath.Join(env.Work, pristineDir, filepath.FromSlash(name)))
			if err != nil {
				return fmt.Errorf("PAR3 proof over %s: no snapshot of %s: %w", proof.Index, name, err)
			}
			got, err := os.ReadFile(filepath.Join(env.Work, proof.scratchDir(), filepath.FromSlash(name)))
			if err != nil || !bytes.Equal(got, want) {
				return fmt.Errorf("the PAR3 reference did not restore %s from %s:\n%s", name, proof.Index, transcript)
			}
		}
		return os.RemoveAll(filepath.Join(env.Work, proof.scratchDir()))
	}
}

// par3ProveUnrepairable is the inverse proof for an insufficient set: the
// reference must say outright that the repair cannot be done.
func par3ProveUnrepairable(proof PAR3Proof) func(context.Context, *Env) error {
	return func(ctx context.Context, env *Env) error {
		transcript, err := env.par3RunProof(ctx, proof)
		if err != nil {
			return err
		}
		if !strings.Contains(transcript, "Repair is not possible") {
			return fmt.Errorf("the PAR3 reference did not refuse to repair %s:\n%s", proof.Index, transcript)
		}
		return os.RemoveAll(filepath.Join(env.Work, proof.scratchDir()))
	}
}

// scratchDir names a work-relative directory for this proof alone. A recipe
// with two sets proves each in its own directory: a bind-mounted directory
// deleted and recreated under the same name can reach the container as a
// stale, empty view on Docker Desktop, and the second repair then finds no
// set to read.
func (proof PAR3Proof) scratchDir() string {
	return proofDir + "-" + sanitizeName(proof.Index)
}

func (env *Env) par3RunProof(ctx context.Context, proof PAR3Proof) (string, error) {
	scratch := filepath.Join(env.Work, proof.scratchDir())
	if err := os.RemoveAll(scratch); err != nil {
		return "", err
	}
	outputs, err := env.Outputs()
	if err != nil {
		return "", err
	}
	omitted := make(map[string]bool, len(proof.Omit))
	for _, name := range proof.Omit {
		omitted[name] = true
	}
	for _, name := range outputs {
		if omitted[name] {
			continue
		}
		target := filepath.Join(scratch, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return "", err
		}
		if err := CopyFile(env.OutputPath(name), target); err != nil {
			return "", err
		}
	}
	toolchain, err := env.par3Toolchain(ctx)
	if err != nil {
		return "", err
	}
	command := "repair"
	if proof.Inserted {
		command = "rs"
	}
	args, err := env.Docker.containerArgs(toolchain, env.Work, proof.scratchDir(), false)
	if err != nil {
		return "", err
	}
	args = append(args, command, proof.Index)
	args = append(args, proof.Extra...)
	output, err := exec.CommandContext(ctx, env.Docker.binary(), args...).CombinedOutput()
	if err != nil && ctx.Err() != nil {
		return "", ctx.Err()
	}
	// A non-zero exit is reported alongside the transcript rather than
	// treated as the verdict; the caller judges by bytes or by the refusal.
	transcript := string(output)
	if err != nil {
		transcript += "\n(exit: " + err.Error() + ")"
	}
	return transcript, nil
}
