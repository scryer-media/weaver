// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const reconcileTestNZB = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="e2e-test@example.invalid" subject="archive.rar (1/3)">
    <segments>
      <segment bytes="768000" number="1">e2e-x-1-001@e2e-test</segment>
      <segment bytes="768000" number="2">e2e-x-1-002@e2e-test</segment>
      <segment bytes="1000" number="3">e2e-x-1-003@e2e-test</segment>
    </segments>
  </file>
</nzb>
`

var reconcileTestNyuuErr = errors.New("nyuu post: exit status 32\n\nLast command output:\n" +
	"[INFO] Finished uploading 80.15 MiB in 00:00:49.602 (1654.56 KiB/s), with 4 error(s) across 4 post(s).")

func writeReconcileTestNZB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.nzb")
	if err := os.WriteFile(path, []byte(reconcileTestNZB), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeStat answers STAT from a fixed set of present ids and records what it
// was asked.
type fakeStat struct {
	present map[string]bool
	asked   []string
}

func (f *fakeStat) missing(ids []string) ([]string, error) {
	f.asked = append(f.asked, ids...)
	var missing []string
	for _, id := range ids {
		if !f.present[id] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

func TestReconcileNyuuPostAllPresentIsComplete(t *testing.T) {
	nzb := writeReconcileTestNZB(t)
	stat := &fakeStat{present: map[string]bool{
		"e2e-x-1-001@e2e-test": true,
		"e2e-x-1-002@e2e-test": true,
		"e2e-x-1-003@e2e-test": true,
	}}
	var logged []string
	logf := func(format string, args ...interface{}) { logged = append(logged, fmt.Sprintf(format, args...)) }

	if err := reconcileNyuuPost(reconcileTestNyuuErr, nzb, stat.missing, logf); err != nil {
		t.Fatalf("expected complete seed, got %v", err)
	}
	if len(stat.asked) != 3 {
		t.Fatalf("expected every NZB segment checked, asked %v", stat.asked)
	}
	if len(logged) != 1 {
		t.Fatalf("expected one log line, got %v", logged)
	}
	if !strings.Contains(logged[0], "reported 4 error(s)") || !strings.Contains(logged[0], "all 3 articles") {
		t.Fatalf("log line lacks the counts: %q", logged[0])
	}
}

func TestReconcileNyuuPostOneMissingFailsNamingIt(t *testing.T) {
	nzb := writeReconcileTestNZB(t)
	stat := &fakeStat{present: map[string]bool{
		"e2e-x-1-001@e2e-test": true,
		"e2e-x-1-003@e2e-test": true,
	}}
	logf := func(string, ...interface{}) { t.Fatal("no completion line expected") }

	err := reconcileNyuuPost(reconcileTestNyuuErr, nzb, stat.missing, logf)
	if err == nil {
		t.Fatal("expected failure for a missing article")
	}
	if !errors.Is(err, reconcileTestNyuuErr) {
		t.Fatalf("failure should wrap nyuu's error: %v", err)
	}
	if !strings.Contains(err.Error(), "e2e-x-1-002@e2e-test") || !strings.Contains(err.Error(), "1 of 3") {
		t.Fatalf("failure should name the missing id: %v", err)
	}
	if strings.Contains(err.Error(), "e2e-x-1-001@e2e-test") {
		t.Fatalf("failure should name only missing ids: %v", err)
	}
}

func TestReconcileNyuuPostShortNZBFailsBeforeStat(t *testing.T) {
	nzb := writeReconcileTestNZB(t)
	nyuuErr := errors.New("nyuu post: exit status 32\n\nLast command output:\n" +
		"[INFO] Uploading 4 article(s) from 1 file(s) totalling 2.20 MiB\n" +
		"[INFO] Finished uploading 2.20 MiB, with 1 error(s) across 1 post(s).")
	stat := func([]string) ([]string, error) {
		t.Fatal("a short NZB must fail before any STAT")
		return nil, nil
	}
	err := reconcileNyuuPost(nyuuErr, nzb, stat, func(string, ...interface{}) { t.Fatal("no completion line expected") })
	if !errors.Is(err, nyuuErr) {
		t.Fatalf("expected nyuu's error wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), "names 3 articles") || !strings.Contains(err.Error(), "meant to post 4") {
		t.Fatalf("failure should name both counts: %v", err)
	}
}

func TestReconcileNyuuPostExitZeroUntouched(t *testing.T) {
	stat := func([]string) ([]string, error) {
		t.Fatal("exit zero must not check the server")
		return nil, nil
	}
	logf := func(string, ...interface{}) { t.Fatal("exit zero must not log") }
	if err := reconcileNyuuPost(nil, filepath.Join(t.TempDir(), "absent.nzb"), stat, logf); err != nil {
		t.Fatalf("exit zero should pass through, got %v", err)
	}
}

func TestReconcileNyuuPostUnreadableNZBKeepsNyuuError(t *testing.T) {
	stat := func([]string) ([]string, error) {
		t.Fatal("no NZB, nothing to check")
		return nil, nil
	}
	err := reconcileNyuuPost(reconcileTestNyuuErr, filepath.Join(t.TempDir(), "absent.nzb"), stat, func(string, ...interface{}) {})
	if !errors.Is(err, reconcileTestNyuuErr) {
		t.Fatalf("expected nyuu's error kept, got %v", err)
	}
}

// TestMissingArticleIDsAtReadsStatAnswers drives the STAT client against an
// in-process server that answers 223 or 430 per id.
func TestMissingArticleIDsAtReadsStatAnswers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	present := map[string]bool{"<a@t>": true, "<c@t>": true}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		fmt.Fprint(conn, "200 ready\r\n")
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			fields := strings.Fields(line)
			switch fields[0] {
			case "MODE":
				fmt.Fprint(conn, "200 control\r\n")
			case "AUTHINFO":
				if fields[1] == "USER" {
					fmt.Fprint(conn, "381 more\r\n")
				} else {
					fmt.Fprint(conn, "281 ok\r\n")
				}
			case "STAT":
				if present[fields[1]] {
					fmt.Fprintf(conn, "223 0 %s\r\n", fields[1])
				} else {
					fmt.Fprint(conn, "430 no such article\r\n")
				}
			case "QUIT":
				fmt.Fprint(conn, "205 bye\r\n")
				return
			}
		}
	}()

	host, port, _ := net.SplitHostPort(ln.Addr().String())
	missing, err := missingArticleIDsAt(host, port, []string{"a@t", "b@t", "c@t"})
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 || missing[0] != "b@t" {
		t.Fatalf("expected only b@t missing, got %v", missing)
	}
}
