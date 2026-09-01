package fixturegen

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// RARFormat selects which archive generation RARLAB's writer emits.
type RARFormat string

const (
	// RAR5 is the current format (`-ma5`).
	RAR5 RARFormat = "rar5"
	// RAR4 is the 2.9/1.5 format (`-ma4`, or the native output of the 4.x
	// writers, which predate the selector).
	RAR4 RARFormat = "rar4"
)

// RARSpec is a RARLAB invocation expressed as fixture shape rather than flags.
// Every RAR file in the corpus is written by `rar` from one of these: UnRAR's
// licence forbids creating RAR archives with UnRAR-derived code, so there is no
// second way to author one here.
type RARSpec struct {
	// Toolchain is the pinned RARLAB writer id.
	Toolchain string
	// Format selects RAR5 or RAR4.
	Format RARFormat
	// Archive is the output name, relative to the scenario's output directory.
	Archive string
	// Members are stage-relative paths, stored under exactly those names and
	// added in exactly this order (which is what fixes a solid archive's
	// decode order).
	Members []string
	// Method is the RAR compression selector: "-m0" stores, "-m1" is fastest.
	Method string
	// Dictionary is the `-md` selector, kept explicit so a newer writer does
	// not silently change the shape.
	Dictionary string
	// Solid chains members into one compression run.
	Solid bool
	// Password encrypts member data (headers stay readable).
	Password string
	// HeaderPassword encrypts headers as well (`-hp`).
	HeaderPassword string
	// VolumeSize is the `-v` selector, for example "30000k" or "20m".
	VolumeSize string
	// RecoveryRecord adds a `-rr` recovery record.
	RecoveryRecord string
	// RecoveryVolumes adds `-rv<N>`: RARLAB writes N standalone `.rev`
	// recovery volumes (`archive.partN.rev`) beside the data volumes, from
	// which up to N missing or damaged data volumes can be reconstructed. Only
	// RARLAB ever writes them; a scenario exercises them by withholding data
	// volumes afterwards, never by editing RAR structures.
	RecoveryVolumes int
	// Extra carries any further RARLAB switch a fixture's shape depends on.
	Extra []string
}

func (spec RARSpec) arguments(stageToOutput string) []string {
	args := []string{"a", "-idq", "-y", "-ed"}
	switch spec.Format {
	case RAR4:
		// The 4.x writers predate -ma; RAR4 is their native output.
		if !strings.HasPrefix(spec.Toolchain, "rarlab-3.") && !strings.HasPrefix(spec.Toolchain, "rarlab-4.") {
			args = append(args, "-ma4")
		}
	default:
		args = append(args, "-ma5")
	}
	if spec.Method != "" {
		args = append(args, spec.Method)
	}
	if spec.Dictionary != "" {
		args = append(args, spec.Dictionary)
	}
	if spec.Solid {
		args = append(args, "-s")
	} else {
		args = append(args, "-s-")
	}
	if spec.Password != "" {
		args = append(args, "-p"+spec.Password)
	}
	if spec.HeaderPassword != "" {
		args = append(args, "-hp"+spec.HeaderPassword)
	}
	if spec.VolumeSize != "" {
		args = append(args, "-v"+spec.VolumeSize)
	}
	if spec.RecoveryRecord != "" {
		args = append(args, "-rr"+spec.RecoveryRecord)
	}
	if spec.RecoveryVolumes > 0 {
		args = append(args, "-rv"+strconv.Itoa(spec.RecoveryVolumes))
	}
	args = append(args, spec.Extra...)
	args = append(args, stageToOutput+"/"+spec.Archive)
	return append(args, spec.Members...)
}

// RAR runs RARLAB's writer over the scenario's stage directory.
func (env *Env) RAR(ctx context.Context, spec RARSpec) error {
	toolchain, err := env.Lock.Find(spec.Toolchain)
	if err != nil {
		return err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return err
	}
	env.usedToolchain(spec.Toolchain)
	return env.Docker.Run(ctx, toolchain, env.Work, stageDir, spec.arguments("../"+outputDir)...)
}

// RARList returns RARLAB's own technical listing of an archive, which is what
// the shape checks in the tests and the docs quote.
func (env *Env) RARList(ctx context.Context, toolchainID, relative, password string) (string, error) {
	toolchain, err := env.Lock.Find(toolchainID)
	if err != nil {
		return "", err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return "", err
	}
	args := []string{"lt"}
	if password != "" {
		args = append(args, "-p"+password)
	}
	return env.Docker.Capture(ctx, toolchain, env.Work, outputDir, append(args, relative)...)
}

// SevenZipSpec is a 7-Zip invocation. 7z is an open format, so the only
// constraint here is that the official console binary writes it.
type SevenZipSpec struct {
	// Format selects the 7-Zip archive type. The default is 7z.
	Format string
	// Archive is the output name relative to the output directory.
	Archive string
	// Members are stage-relative paths.
	Members []string
	// Level is the `-mx` selector.
	Level string
	// Store writes with the Copy method instead of LZMA2.
	Store bool
	// Password encrypts member data.
	Password string
	// EncryptHeaders adds `-mhe=on`.
	EncryptHeaders bool
	// Solid packs the members into one block (`-ms=on`). The default is off,
	// which is what every pre-existing fixture relies on.
	Solid bool
	// Methods are raw coder-chain switches, for example
	// []string{"-m0=Delta:4", "-m1=LZMA2"}. Passed through verbatim so a recipe
	// can name any chain the pinned 7-Zip writes; mutually exclusive with
	// Store, which is just the Copy chain spelled shorter.
	Methods []string
}

// SevenZip runs the pinned 7-Zip console binary over the stage directory.
func (env *Env) SevenZip(ctx context.Context, spec SevenZipSpec) error {
	toolchain, err := env.Lock.Find(SevenZipToolchain)
	if err != nil {
		return err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return err
	}
	env.usedToolchain(SevenZipToolchain)
	format := spec.Format
	if format == "" {
		format = "7z"
	}
	args := []string{"a", "-t" + format, "-bso0", "-bsp0", "-y"}
	if format == "7z" {
		if spec.Solid {
			args = append(args, "-ms=on")
		} else {
			args = append(args, "-ms=off")
		}
	}
	switch {
	case spec.Store:
		args = append(args, "-m0=Copy", "-mx0")
	case len(spec.Methods) > 0:
		// An explicit chain still needs a level: 7-Zip derives dictionary and
		// word sizes from it even when the coder is named outright.
		level := spec.Level
		if level == "" {
			level = "-mx1"
		}
		args = append(args, level)
		args = append(args, spec.Methods...)
	default:
		level := spec.Level
		if level == "" {
			level = "-mx1"
		}
		args = append(args, level)
	}
	if spec.Password != "" {
		args = append(args, "-p"+spec.Password)
		if spec.EncryptHeaders {
			args = append(args, "-mhe=on")
		}
	}
	args = append(args, "../"+outputDir+"/"+spec.Archive)
	args = append(args, spec.Members...)
	return env.Docker.Run(ctx, toolchain, env.Work, stageDir, args...)
}

// SevenZipList returns 7-Zip's own technical listing of an archive.
func (env *Env) SevenZipList(ctx context.Context, relative, password string) (string, error) {
	toolchain, err := env.Lock.Find(SevenZipToolchain)
	if err != nil {
		return "", err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return "", err
	}
	args := []string{"l", "-slt"}
	if password != "" {
		args = append(args, "-p"+password)
	}
	return env.Docker.Capture(ctx, toolchain, env.Work, outputDir, append(args, relative)...)
}

// PAR2Spec is a par2cmdline-turbo `create` run over files already in the
// output directory.
type PAR2Spec struct {
	// Base is the index file name, for example "archive.rar.par2".
	Base string
	// Sources are output-relative file names the recovery set covers.
	Sources []string
	// SliceSize fixes the block size in bytes (`-s`). Exactly one of
	// SliceSize and BlockCount is set.
	SliceSize int64
	// BlockCount fixes the number of source blocks (`-b`).
	BlockCount int
	// RecoveryBlocks is the `-c` recovery block count.
	RecoveryBlocks int
	// RedundancyPercent is the `-r` alternative to RecoveryBlocks.
	RedundancyPercent int
	// RecoveryFiles caps the number of `.volNN+NN.par2` files (`-n`). Zero
	// leaves par2's default doubling layout in place.
	RecoveryFiles int
}

// PAR2 creates a recovery set with the pinned par2cmdline-turbo build.
func (env *Env) PAR2(ctx context.Context, spec PAR2Spec) error {
	toolchain, err := env.Lock.Find(PAR2Toolchain)
	if err != nil {
		return err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return err
	}
	env.usedToolchain(PAR2Toolchain)
	args := []string{"create", "-q"}
	switch {
	case spec.SliceSize > 0:
		args = append(args, "-s"+strconv.FormatInt(spec.SliceSize, 10))
	case spec.BlockCount > 0:
		args = append(args, "-b"+strconv.Itoa(spec.BlockCount))
	}
	switch {
	case spec.RecoveryBlocks > 0:
		args = append(args, "-c"+strconv.Itoa(spec.RecoveryBlocks))
	case spec.RedundancyPercent > 0:
		args = append(args, "-r"+strconv.Itoa(spec.RedundancyPercent))
	default:
		return fmt.Errorf("PAR2 set %q needs recovery blocks or a redundancy percentage", spec.Base)
	}
	if spec.RecoveryFiles > 0 {
		args = append(args, "-n"+strconv.Itoa(spec.RecoveryFiles))
	}
	args = append(args, spec.Base)
	args = append(args, spec.Sources...)
	return env.Docker.Run(ctx, toolchain, env.Work, outputDir, args...)
}

// PAR2Verify reports what the pinned PAR2 tool makes of a finished set, which
// is how a damage recipe proves it damaged the right number of blocks.
func (env *Env) PAR2Verify(ctx context.Context, index string) (string, error) {
	toolchain, err := env.Lock.Find(PAR2Toolchain)
	if err != nil {
		return "", err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return "", err
	}
	return env.Docker.Capture(ctx, toolchain, env.Work, outputDir, "v", index)
}

// ClipSpec renders one synthetic video clip. Sources are FFmpeg's own
// generators — no sample media is downloaded, and no title in this corpus
// refers to a real work.
type ClipSpec struct {
	// Name is the output file, relative to the output directory.
	Name string
	// Seconds is the clip duration.
	Seconds int
	// Width and Height size the video.
	Width, Height int
	// VideoCodec is "libsvtav1" or "libx264".
	VideoCodec string
	// AudioCodec is "libopus" or "aac".
	AudioCodec string
	// VideoBitrate is the requested video bitrate in bits per second.
	VideoBitrate int64
	// AudioBitrate is the requested audio bitrate in bits per second.
	AudioBitrate int64
	// SampleRate is the audio sample rate.
	SampleRate int
	// ConstantBitrate pins an H.264 clip to a constant rate, which is what
	// makes a payload land on a predictable size. It is left off for clips
	// whose bitrate is too low for the rate controller to honour.
	ConstantBitrate bool
	// Noise adds a seeded noise filter. A clean synthetic pattern survives
	// H.264 as something an archiver can still compress by 15%, which is a lie
	// about what release media does; noise keeps the encoded payload at the
	// entropy real media has, so a RAR or 7z fixture stays the size the corpus
	// expects.
	Noise bool
	// Seed varies the synthetic source between clips.
	Seed int
	// MinBytes is the size floor the harness needs from this clip. The
	// encoder is retried at the fixed escalation steps below until the floor
	// is met, so a bitrate constant that drifts fails loudly instead of
	// quietly shrinking an asset the seeder depends on.
	MinBytes int64
}

// clipEscalation are the fixed multipliers a clip's bitrate is retried at.
var clipEscalation = []float64{1.0, 1.15, 1.35, 1.6}

// Clip renders a synthetic clip with the pinned FFmpeg image.
func (env *Env) Clip(ctx context.Context, spec ClipSpec) error {
	toolchain, err := env.Lock.Find(VideoToolchain)
	if err != nil {
		return err
	}
	if err := env.Docker.Prepare(ctx, toolchain); err != nil {
		return err
	}
	env.usedToolchain(VideoToolchain)
	destination := env.OutputPath(spec.Name)
	// A clip that already carries its final name is complete, so a resumable
	// artifact does not re-encode what it finished before it failed. The
	// encoder writes under a `.partial` name and the file is renamed only once
	// it has been measured, so an interrupted encode can never be mistaken for
	// a finished one however large it grew.
	if _, err := FileSize(destination); err == nil {
		return nil
	}
	// The partial name keeps the extension: FFmpeg chooses its muxer from it.
	partial := "partial-" + spec.Name
	for attempt, multiplier := range clipEscalation {
		bitrate := int64(float64(spec.VideoBitrate) * multiplier)
		if err := env.Docker.Run(ctx, toolchain, env.Work, outputDir, spec.arguments(bitrate, partial)...); err != nil {
			return err
		}
		size, err := FileSize(env.OutputPath(partial))
		if err != nil {
			return err
		}
		if size >= spec.MinBytes {
			return os.Rename(env.OutputPath(partial), destination)
		}
		if attempt == len(clipEscalation)-1 {
			return fmt.Errorf("clip %s came out at %d bytes, below the %d byte floor, even at %.2fx the recipe bitrate: raise VideoBitrate in the recipe",
				spec.Name, size, spec.MinBytes, multiplier)
		}
	}
	return nil
}

func (spec ClipSpec) arguments(bitrate int64, output string) []string {
	seconds := strconv.Itoa(spec.Seconds)
	video := fmt.Sprintf("testsrc2=size=%dx%d:rate=24:duration=%d", spec.Width, spec.Height, spec.Seconds)
	if spec.Noise {
		video += fmt.Sprintf(",noise=all_seed=%d:all_strength=20:all_flags=t", spec.Seed%65535)
	}
	audio := fmt.Sprintf("sine=frequency=%d:sample_rate=%d:duration=%d", 220+spec.Seed%660, spec.SampleRate, spec.Seconds)
	args := []string{
		"-nostdin", "-hide_banner", "-loglevel", "error", "-y",
		"-f", "lavfi", "-i", video,
		"-f", "lavfi", "-i", audio,
		"-map", "0:v:0", "-map", "1:a:0",
		"-c:v", spec.VideoCodec,
	}
	if spec.VideoCodec == "libsvtav1" {
		args = append(args, "-preset", "12")
	} else {
		args = append(args, "-preset", "veryfast")
		if spec.ConstantBitrate {
			args = append(args, "-x264-params", "nal-hrd=cbr:force-cfr=1",
				"-minrate", strconv.FormatInt(bitrate, 10), "-maxrate", strconv.FormatInt(bitrate, 10),
				"-bufsize", strconv.FormatInt(bitrate, 10))
		}
	}
	args = append(args,
		"-pix_fmt", "yuv420p",
		"-b:v", strconv.FormatInt(bitrate, 10),
		"-c:a", spec.AudioCodec,
		"-b:a", strconv.FormatInt(spec.AudioBitrate, 10),
		"-t", seconds,
		output,
	)
	return args
}
