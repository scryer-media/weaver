package weaver

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const (
	defaultNNTPUsername = "e2e-user"
	defaultNNTPPassword = "e2e-pass"
)

// applyDefaultNNTPCredentialEnv exports the fixture NNTP credentials into the
// process environment when the caller has not set them.
//
// `nntpUsername`/`nntpPassword` below already fall back to these constants, so
// the harness's own Go code never needed this. Docker Compose does. The
// password reaches the NNTP container as an environment-backed secret:
//
//	secrets:
//	  nntp-password:
//	    environment: E2E_NNTP_PASSWORD
//
// That form names a variable and has no `:-default` syntax — the username
// beside it can write `${E2E_NNTP_USERNAME:-e2e-user}`, the password cannot. An
// unset variable therefore handed the server an empty password and seeding died
// within a second, on any shell that had not exported it: a fresh clone, a new
// worktree, a terminal that lost the export. Defaulting it here, in `Run`,
// means every subcommand gets the same value rather than only the one path
// that happened to notice.
//
// Fills only what is absent, so a real override still wins.
func applyDefaultNNTPCredentialEnv() {
	if strings.TrimSpace(os.Getenv("E2E_NNTP_USERNAME")) == "" {
		_ = os.Setenv("E2E_NNTP_USERNAME", defaultNNTPUsername)
	}
	if strings.TrimSpace(os.Getenv("E2E_NNTP_PASSWORD")) == "" {
		_ = os.Setenv("E2E_NNTP_PASSWORD", defaultNNTPPassword)
	}
}

func nntpUsername() string {
	if username := strings.TrimSpace(os.Getenv("E2E_NNTP_USERNAME")); username != "" {
		return username
	}
	return defaultNNTPUsername
}

func nntpPassword() string {
	if password := os.Getenv("E2E_NNTP_PASSWORD"); password != "" {
		return password
	}
	return defaultNNTPPassword
}

func authenticateNNTPConnection(conn net.Conn, reader *bufio.Reader, addr string) error {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("AUTHINFO USER " + nntpUsername() + "\r\n")); err != nil {
		return fmt.Errorf("write auth user to %s: %w", addr, err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	userResp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read auth user response from %s: %w", addr, err)
	}
	userResp = strings.TrimSpace(userResp)
	if !strings.HasPrefix(userResp, "381") {
		return fmt.Errorf("unexpected auth user response from %s: %s", addr, userResp)
	}

	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte("AUTHINFO PASS " + nntpPassword() + "\r\n")); err != nil {
		return fmt.Errorf("write auth pass to %s: %w", addr, err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	passResp, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read auth pass response from %s: %w", addr, err)
	}
	passResp = strings.TrimSpace(passResp)
	if !strings.HasPrefix(passResp, "281") {
		return fmt.Errorf("unexpected auth pass response from %s: %s", addr, passResp)
	}

	return nil
}
