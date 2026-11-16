// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"strings"
	"testing"
)

// The unavailable-server gate must read to Weaver as an outage, never as a
// refused login: a BODY answered 480 again after the login it asked for is
// the server refusing the account, and Weaver holds such a server for its
// whole login quarantine, so the work waiting on it outlasts the gate.
func TestServerUnavailableChaosRefusesSessionsWithoutAskingForALogin(t *testing.T) {
	options := map[string]string{}
	for _, part := range strings.Split(serverUnavailableChaos, ",") {
		name, value, found := strings.Cut(part, "=")
		if !found {
			t.Fatalf("chaos option %q has no value", part)
		}
		options[name] = value
	}
	if options["greet_400"] != "100" {
		t.Fatalf("gate %q does not refuse every new session", serverUnavailableChaos)
	}
	if options["drop_conn"] != "100" {
		t.Fatalf("gate %q does not drop every pooled session at its BODY", serverUnavailableChaos)
	}
	for _, login := range []string{"reauth_body", "reject_auth"} {
		if _, asked := options[login]; asked {
			t.Fatalf("gate %q sets %s, which Weaver reads as a refused login", serverUnavailableChaos, login)
		}
	}
}
