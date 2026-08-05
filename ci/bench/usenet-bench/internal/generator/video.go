package generator

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/scryer-media/weaver/ci/bench/usenet-bench/internal/fixture"
	"github.com/zeebo/blake3"
)

// renderVideo creates and validates a real video file inside the same pinned
// Docker image used to create its RAR archive. Keeping FFmpeg in that image
// makes the generated corpus independent of the host's media stack.
func renderVideo(ctx context.Context, config Config, toolchain Toolchain, caseDir, relative string, kind fixture.PayloadKind, targetBytes int64, stream uint64) (fixture.FileDigest, error) {
	if targetBytes <= 0 {
		return fixture.FileDigest{}, fmt.Errorf("video target size must be positive, got %d", targetBytes)
	}
	path := filepath.Join(caseDir, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fixture.FileDigest{}, err
	}

	containerPath := "/work/" + filepath.ToSlash(relative)
	args := dockerVideoCommand(toolchain, caseDir, "ffmpeg")
	args = append(args, ffmpegRenderArgs(kind, targetBytes, stream, containerPath)...)
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return fixture.FileDigest{}, fmt.Errorf("render %s: %w", relative, err)
	}
	if err := verifyVideo(ctx, config, toolchain, caseDir, relative); err != nil {
		return fixture.FileDigest{}, err
	}
	if err := os.Chtimes(path, canonicalFileTime, canonicalFileTime); err != nil {
		return fixture.FileDigest{}, err
	}
	return digestFile(relative, path)
}

func dockerVideoCommand(toolchain Toolchain, caseDir, entrypoint string) []string {
	return []string{
		"run", "--rm", "--platform", toolchain.Platform,
		"--user", callerDockerUser(),
		"--mount", "type=bind,src=" + caseDir + ",dst=/work",
		"--workdir", "/work",
		"--entrypoint", entrypoint,
		toolchain.Image,
	}
}

func ffmpegRenderArgs(kind fixture.PayloadKind, targetBytes int64, stream uint64, output string) []string {
	seed := stream % 65_535
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-y"}
	if isTransportStream(output) {
		// The disc-layout fixture uses real MPEG transport streams, including
		// each tiny stream, so every archived payload remains video data.
		filter := fmt.Sprintf("testsrc2=size=1280x720:rate=30,hue=h=%d:s=1", seed%360)
		bitrate := int64(200_192_000)
		if targetBytes <= 1<<20 {
			bitrate = 2_192_000
		}
		args = append(args,
			"-f", "lavfi", "-i", filter,
			"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=%d:sample_rate=48000", 440+seed%440),
			"-map", "0:v:0", "-map", "1:a:0",
			"-c:v", "mpeg2video", "-pix_fmt", "yuv420p",
			"-b:v", strconv.FormatInt(bitrate-192_000, 10), "-minrate", strconv.FormatInt(bitrate-192_000, 10), "-maxrate", strconv.FormatInt(bitrate-192_000, 10), "-bufsize", strconv.FormatInt(bitrate-192_000, 10),
			"-c:a", "mp2", "-b:a", "192k", "-f", "mpegts",
		)
		return append(args, "-t", mediaDuration(targetBytes, bitrate), output)
	}
	switch kind {
	case fixture.IncompressiblePayload:
		// A noisy, CBR H.264 stream models already-compressed release media.
		filter := fmt.Sprintf("testsrc2=size=1280x720:rate=30,noise=all_seed=%d:all_strength=20:all_flags=t", seed)
		const bitrate = int64(20_128_000)
		args = append(args,
			"-f", "lavfi", "-i", filter,
			"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=%d:sample_rate=48000", 440+seed%440),
			"-map", "0:v:0", "-map", "1:a:0",
			"-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
			"-b:v", "20M", "-minrate", "20M", "-maxrate", "20M", "-bufsize", "20M",
			"-x264-params", "nal-hrd=cbr:force-cfr=1",
			"-c:a", "aac", "-b:a", "128k",
			"-t", mediaDuration(targetBytes, bitrate),
		)
	case fixture.CompressiblePayload:
		// Uncompressed YUV AVI is a valid video file whose visual structure is
		// intentionally available to RAR's compression modes.
		filter := fmt.Sprintf("testsrc2=size=320x180:rate=30,hue=h=%d:s=1", seed%360)
		const bitrate = int64(22_272_000)
		args = append(args,
			"-f", "lavfi", "-i", filter,
			"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=%d:sample_rate=48000", 440+seed%440),
			"-map", "0:v:0", "-map", "1:a:0",
			"-c:v", "rawvideo", "-pix_fmt", "yuv420p",
			"-c:a", "pcm_s16le", "-f", "avi",
			"-t", mediaDuration(targetBytes, bitrate),
		)
	default:
		return nil
	}
	return append(args, output)
}

func mediaDuration(targetBytes, bitrate int64) string {
	seconds := float64(targetBytes*8) / float64(bitrate)
	return strconv.FormatFloat(seconds, 'f', 6, 64)
}

func videoExtension(kind fixture.PayloadKind) string {
	if kind == fixture.CompressiblePayload {
		return ".avi"
	}
	return ".mkv"
}

func verifyVideo(ctx context.Context, config Config, toolchain Toolchain, caseDir, relative string) error {
	args := dockerVideoCommand(toolchain, caseDir, "ffmpeg")
	args = append(args,
		"-nostdin", "-hide_banner", "-loglevel", "error",
		"-i", "/work/"+filepath.ToSlash(relative),
		"-map", "0:v:0", "-frames:v", "1", "-f", "null", "-",
	)
	if err := runCommand(ctx, config.DockerBinary, args...); err != nil {
		return fmt.Errorf("validate video %s: %w", relative, err)
	}
	return nil
}

func digestFile(relative, path string) (fixture.FileDigest, error) {
	file, err := os.Open(path)
	if err != nil {
		return fixture.FileDigest{}, err
	}
	defer file.Close()
	hash := blake3.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return fixture.FileDigest{}, err
	}
	return fixture.FileDigest{Path: filepath.ToSlash(relative), Size: size, BLAKE3: hex.EncodeToString(hash.Sum(nil))}, nil
}

func copyVideoFile(source, destination string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

func isTransportStream(relative string) bool {
	return strings.HasSuffix(strings.ToLower(relative), ".m2ts")
}
