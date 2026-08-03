package benchmark

import (
	"fmt"
	"os"
	"strconv"
)

// WriteServerLinkEnvironment produces an immutable Compose-compatible env file
// for the transparent NNTP shaper or a public NNTP server implementing the
// same environment contract.
func WriteServerLinkEnvironment(path string, profile ServerLinkProfile) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	contents := "# nntpbench server link profile: " + profile.ID + "\n" +
		"# scope: " + profile.Scope + "\n" +
		"NNTP_EGRESS_BITS_PER_SECOND=" + strconv.FormatUint(profile.EgressBitsPerSecond, 10) + "\n" +
		"NNTP_EGRESS_BURST_BYTES=" + strconv.FormatUint(profile.BurstBytes, 10) + "\n"
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create server link environment %s: %w", path, err)
	}
	defer file.Close()
	if _, err := file.WriteString(contents); err != nil {
		return fmt.Errorf("write server link environment %s: %w", path, err)
	}
	return nil
}
