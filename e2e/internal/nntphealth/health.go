// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Package nntphealth checks the fixture listener without claiming a data seat.
package nntphealth

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

func Check(conn io.ReadWriter) error {
	reader := bufio.NewReader(conn)
	expect := func(code string) error {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if !strings.HasPrefix(line, code+" ") && strings.TrimSpace(line) != code {
			return fmt.Errorf("NNTP health expected %s, got %q", code, line)
		}
		return nil
	}
	if err := expect("200"); err != nil {
		return err
	}
	// The first command classifies the entire session. QUIT first would
	// briefly claim a data seat and contaminate the provider's peak.
	for _, step := range []struct{ command, code string }{{"MODE CONTROL", "200"}, {"QUIT", "205"}} {
		if _, err := fmt.Fprintf(conn, "%s\r\n", step.command); err != nil {
			return err
		}
		if err := expect(step.code); err != nil {
			return err
		}
	}
	return nil
}

func CheckAddress(address string, timeout time.Duration) error {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if timeout > 0 {
		if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	return Check(conn)
}
