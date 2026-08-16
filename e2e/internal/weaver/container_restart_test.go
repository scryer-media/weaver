package weaver

import "testing"

func TestParseContainerEncryptionKeyState(t *testing.T) {
	fingerprint := "ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789"

	state, err := parseContainerEncryptionKeyState(fingerprint+"  "+e2eContainerEncryptionKeyPath+"\n", "600\n")
	if err != nil {
		t.Fatalf("parseContainerEncryptionKeyState: %v", err)
	}
	if state.Fingerprint != "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" {
		t.Fatalf("fingerprint = %q", state.Fingerprint)
	}
	if state.Mode != "600" {
		t.Fatalf("mode = %q, want 600", state.Mode)
	}
}

func TestParseContainerEncryptionKeyStateRejectsMalformedOutput(t *testing.T) {
	tests := []struct {
		name              string
		fingerprintOutput string
		modeOutput        string
	}{
		{name: "empty fingerprint", modeOutput: "600"},
		{name: "short fingerprint", fingerprintOutput: "abc /config/encryption.key", modeOutput: "600"},
		{name: "non-hex fingerprint", fingerprintOutput: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz /config/encryption.key", modeOutput: "600"},
		{name: "empty mode", fingerprintOutput: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789 /config/encryption.key"},
		{name: "multiple modes", fingerprintOutput: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789 /config/encryption.key", modeOutput: "600\n644"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseContainerEncryptionKeyState(test.fingerprintOutput, test.modeOutput); err == nil {
				t.Fatal("expected malformed output to fail")
			}
		})
	}
}
