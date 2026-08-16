package weaver

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
)

const progressEventPrefix = "__E2E_EVENT__ "

type progressEvent struct {
	Kind    string `json:"kind"`
	Current int    `json:"current,omitempty"`
	Total   int    `json:"total,omitempty"`
	Status  string `json:"status,omitempty"`
	Detail  string `json:"detail,omitempty"`
	// Name identifies a sub-unit within a phase — currently a release-gate
	// flow/datastore pair — so the dashboard can keep one bar per unit while
	// Detail stays free for human-readable text.
	Name string `json:"name,omitempty"`
}

var progressEmitMu sync.Mutex

func progressEventsEnabled() bool {
	return envBool("E2E_EVENT_STREAM", false)
}

func emitProgressEvent(event progressEvent) {
	if !progressEventsEnabled() {
		return
	}
	progressEmitMu.Lock()
	defer progressEmitMu.Unlock()

	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(os.Stdout, "%s%s\n", progressEventPrefix, data)
}

func parseProgressEventLine(line string) (progressEvent, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, progressEventPrefix) {
		return progressEvent{}, false
	}
	var event progressEvent
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, progressEventPrefix)), &event); err != nil {
		return progressEvent{}, false
	}
	return event, true
}
