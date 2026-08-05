package fixture

import "testing"

func TestCorpusValidation(t *testing.T) {
	if err := (Corpus{SchemaVersion: 1, FixtureIDs: []string{"fixture-a", "fixture-b"}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Corpus{SchemaVersion: 1, FixtureIDs: []string{"fixture-a", "fixture-a"}}).Validate(); err == nil {
		t.Fatal("duplicate fixture id was accepted")
	}
}
