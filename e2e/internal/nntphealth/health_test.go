// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package nntphealth

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestHealthDeclaresControlBeforeQuit(t *testing.T) {
	var commands bytes.Buffer
	conn := struct {
		io.Reader
		io.Writer
	}{strings.NewReader("200 ready\r\n200 control\r\n205 bye\r\n"), &commands}
	if err := Check(conn); err != nil {
		t.Fatal(err)
	}
	if got := commands.String(); got != "MODE CONTROL\r\nQUIT\r\n" {
		t.Fatalf("commands %q", got)
	}
}

func TestHealthRejectsAnUnhealthyProtocol(t *testing.T) {
	for _, replies := range []string{"", "400 stopped\r\n", "200 ready\r\n500 no control\r\n", "200 ready\r\n200 control\r\n400 no quit\r\n"} {
		var commands bytes.Buffer
		conn := struct {
			io.Reader
			io.Writer
		}{strings.NewReader(replies), &commands}
		if err := Check(conn); err == nil {
			t.Fatalf("accepted %q", replies)
		}
	}
}
