package fixture

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Corpus is the declared benchmark selection. It keeps a coverage corpus
// explicit without suggesting that its fixture counts model Usenet traffic.
type Corpus struct {
	SchemaVersion int      `json:"schema_version"`
	Description   string   `json:"description"`
	FixtureIDs    []string `json:"fixture_ids"`
}

func LoadCorpus(path string) (Corpus, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Corpus{}, fmt.Errorf("read fixture corpus %s: %w", path, err)
	}
	var corpus Corpus
	if err := json.Unmarshal(contents, &corpus); err != nil {
		return Corpus{}, fmt.Errorf("decode fixture corpus %s: %w", path, err)
	}
	if err := corpus.Validate(); err != nil {
		return Corpus{}, fmt.Errorf("fixture corpus %s: %w", path, err)
	}
	return corpus, nil
}

func (c Corpus) Validate() error {
	if c.SchemaVersion != 1 || len(c.FixtureIDs) == 0 {
		return fmt.Errorf("unsupported schema or empty fixture list")
	}
	seen := make(map[string]bool, len(c.FixtureIDs))
	for _, id := range c.FixtureIDs {
		if strings.TrimSpace(id) == "" || seen[id] {
			return fmt.Errorf("fixture ids must be non-empty and unique")
		}
		seen[id] = true
	}
	return nil
}
