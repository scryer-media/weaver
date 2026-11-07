// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import "testing"

func TestArchiveModesDistinguishNestedStoreFromEngineFallback(t *testing.T) {
	store := ScenarioArchiveModeAssertion{Archive: "inner.zip", RequireKind: "zip", ForbidKinds: []string{"zip_engine"}}
	engine := ScenarioArchiveModeAssertion{Archive: "inner.zip", RequireKind: "zip_engine"}
	bound := func(fields string) string { return graphLine(graphObjectBoundMessage, fields) }
	tests := []struct {
		name      string
		assertion ScenarioArchiveModeAssertion
		lines     []string
		wantErr   string
	}{
		{"nested store", store, []string{bound("job_id=40 node=1 kind=zip over=outer.zip"), bound("job_id=40 node=2 kind=zip over=inner.zip")}, ""},
		{"outer cannot stand for inner", store, []string{bound("job_id=40 node=1 kind=zip over=outer.zip")}, "never bound"},
		{"other job cannot satisfy", store, []string{bound("job_id=41 node=2 kind=zip over=inner.zip")}, "never bound"},
		{"store followed by engine fails", store, []string{bound("job_id=40 node=2 kind=zip over=inner.zip"), bound("job_id=40 node=3 kind=zip_engine over=inner.zip")}, "forbidden"},
		{"earlier engine also fails", store, []string{bound("job_id=40 node=3 kind=zip_engine over=inner.zip"), bound("job_id=40 node=4 kind=zip over=inner.zip")}, "forbidden"},
		{"unrelated outer engine", store, []string{bound("job_id=40 node=1 kind=zip_engine over=outer.zip"), bound("job_id=40 node=2 kind=zip over=inner.zip")}, ""},
		{"inner engine required", engine, []string{bound("job_id=40 node=2 kind=zip over=inner.zip")}, "never bound"},
		{"outer engine cannot stand for inner", engine, []string{bound("job_id=40 node=1 kind=zip_engine over=outer.zip")}, "never bound"},
		{"inner engine", engine, []string{bound("job_id=40 node=2 kind=zip over=inner.zip"), bound("job_id=40 node=3 kind=zip_engine over=inner.zip")}, ""},
		{"invalid kind", ScenarioArchiveModeAssertion{Archive: "inner.zip", RequireKind: "zipp"}, nil, "invalid"},
		{"contradictory", ScenarioArchiveModeAssertion{Archive: "inner.zip", RequireKind: "zip", ForbidKinds: []string{"zip"}}, nil, "invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wantVerdict(t, graphArchiveModesVerdict(graphLog(test.lines...), 40, []ScenarioArchiveModeAssertion{test.assertion}), test.wantErr)
		})
	}
}
