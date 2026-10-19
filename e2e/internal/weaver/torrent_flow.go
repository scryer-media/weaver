// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

var torrentFlowServices = []string{"nntp", "nntp2", "weaver", "torrent-tracker", "torrent-seeder", "torrent-leecher", "proxy-fixture"}

// torrentsReleaseFlow runs the torrent lane's API and client-facade
// scenarios against Weaver, a tracker, an rqbit seeder and leecher, and the
// proxy fixture, on each datastore.
func torrentsReleaseFlow() weaverReleaseFlowSpec {
	// A command flow: it drives Weaver from the harness, not a browser. It
	// still runs on each datastore and through the browser flows' teardown.
	return weaverReleaseFlowSpec{
		Name:       "torrents",
		Kind:       weaverReleaseFlowCommand,
		Services:   slices.Clone(torrentFlowServices),
		Datastores: releaseDatastoreMatrix(),
		Artifacts:  append(defaultWeaverReleaseArtifacts(), "torrent-scenarios/summary.json"),
		Timeout:    40 * time.Minute,
	}
}

// uiTorrentsReleaseFlow runs the torrent browser flows against the same
// fixtures.
func uiTorrentsReleaseFlow() weaverReleaseFlowSpec {
	spec := browserReleaseFlow("ui-torrents", 20*time.Minute)
	spec.Services = slices.Clone(torrentFlowServices)
	return spec
}

func isTorrentFlow(name string) bool { return name == "torrents" || name == "ui-torrents" }

// startTorrentLane builds the peer image and the fixtures, starts the
// flow's stack, and hands the seeder every seeded fixture.
func startTorrentLane(spec weaverReleaseFlowSpec, datastore weaverDatastore, artifactsDir string) (*torrentLane, error) {
	if err := ensureLocalWeaverPlaywrightImage(); err != nil {
		return nil, err
	}
	if err := ensureTorrentPeerImage(); err != nil {
		return nil, err
	}
	if _, err := cmdSeedTorrents(); err != nil {
		return nil, fmt.Errorf("build torrent fixtures: %w", err)
	}
	if err := startWeaverReleaseStack(spec, datastore); err != nil {
		return nil, err
	}
	lane, err := newTorrentLane(artifactsDir)
	if err != nil {
		return nil, err
	}
	if err := lane.waitServices(); err != nil {
		return nil, err
	}
	if err := lane.seedAll(); err != nil {
		return nil, err
	}
	return lane, nil
}

// waitServices waits until the tracker, both peers and the proxy fixture
// answer on their published ports.
func (lane *torrentLane) waitServices() error {
	targets := []string{lane.trackerURL + "/health", lane.seederURL + "/", lane.leecherURL + "/"}
	if lane.proxyURL != "" {
		targets = append(targets, lane.proxyURL+"/events?after=0")
	}
	for _, target := range targets {
		if err := waitUntil("service answers at "+target, func() (bool, string, error) {
			resp, err := lane.http.Get(target)
			if err != nil {
				return false, err.Error(), nil
			}
			resp.Body.Close()
			return resp.StatusCode == http.StatusOK, resp.Status, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func selectTorrentScenarios(names []string) ([]torrentScenario, error) {
	all := torrentScenarios()
	if len(names) == 0 {
		return all, nil
	}
	var selected []torrentScenario
	for _, scenario := range all {
		if slices.Contains(names, scenario.Name) {
			selected = append(selected, scenario)
		}
	}
	if len(selected) != len(names) {
		return nil, fmt.Errorf("unknown torrent scenario in %v", names)
	}
	// The lane turns torrents on in its first scenario; a selection without
	// it turns them on before the rest.
	if selected[0].Name != "torrent-disabled" {
		selected = append([]torrentScenario{{Name: "enable-torrents", Asserts: []string{"torrents turn on"},
			Run: func(lane *torrentLane, _ map[string]any) error { return lane.enableTorrents() }}}, selected...)
	}
	return selected, nil
}

func torrentScenarioNames() []string {
	value := strings.TrimSpace(os.Getenv("E2E_TORRENT_SCENARIOS"))
	if value == "" {
		return nil
	}
	return strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ' ' })
}

func runWeaverTorrentsReleaseFlow(ctx context.Context, spec weaverReleaseFlowSpec, datastore weaverDatastore, artifactsDir string) error {
	scenarios, err := selectTorrentScenarios(torrentScenarioNames())
	if err != nil {
		return err
	}
	lane, err := startTorrentLane(spec, datastore, artifactsDir)
	if err != nil {
		return err
	}
	return lane.run(ctx, scenarios)
}

func runWeaverUITorrentsReleaseFlow(ctx context.Context, spec weaverReleaseFlowSpec, datastore weaverDatastore, artifactsDir string) error {
	lane, err := startTorrentLane(spec, datastore, artifactsDir)
	if err != nil {
		return err
	}
	// The browser reads the fixtures' metainfo and magnets from the
	// manifest under /work/fixtures; the lane has handed the seeder all of
	// them and turns torrents on as the operator would before the specs.
	if err := lane.enableTorrents(); err != nil {
		return err
	}
	return runWeaverReleasePlaywright(ctx, spec.PlaywrightScript)
}

// cmdTorrentTest runs torrent scenarios against a stack that is already up,
// as `test` does for the NZB scenarios.
func cmdTorrentTest(names []string) {
	scenarios, err := selectTorrentScenarios(names)
	if err != nil {
		log.Fatal(err)
	}
	artifacts := filepath.Join(e2eDir(), "artifacts", "torrents")
	if value := strings.TrimSpace(os.Getenv("E2E_WEAVER_PLAYWRIGHT_ARTIFACTS_DIR")); value != "" {
		artifacts = absolutePath(value)
	}
	lane, err := newTorrentLane(artifacts)
	if err != nil {
		log.Fatal(err)
	}
	if err := lane.waitServices(); err != nil {
		log.Fatal(err)
	}
	if err := lane.seedAll(); err != nil {
		log.Fatal(err)
	}
	if err := lane.run(context.Background(), scenarios); err != nil {
		log.Fatal(err)
	}
}

func cmdSeedTorrentsCLI() {
	manifest, err := cmdSeedTorrents()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("built %d torrent fixtures under %s", len(manifest.Torrents), torrentFixturesRoot())
}

func urlEncode(value string) string { return url.QueryEscape(value) }

func readFile(path string) ([]byte, error) { return os.ReadFile(path) }

// multipartBody is a form with one file part, as a torrent client uploads.
func multipartBody(fields map[string]string, field, filename string, data []byte) (io.Reader, string) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range fields {
		_ = writer.WriteField(key, value)
	}
	part, err := writer.CreateFormFile(field, filename)
	if err == nil {
		_, err = part.Write(data)
	}
	closeErr := writer.Close()
	if err := errors.Join(err, closeErr); err != nil {
		panic(err)
	}
	return &body, writer.FormDataContentType()
}
