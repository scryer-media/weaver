// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/blake3"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

// The torrent lane's services, as docker-compose.yml names them.
var torrentLaneServices = []string{"torrent-tracker", "torrent-seeder", "torrent-leecher"}

// The public DHT bootstrap names librqbit falls back to when it is given
// none. Weaver passes none and has no setting for them, so the lane points
// the names at the container's own loopback: the DHT starts, finds nobody,
// and never leaves the compose network.
var torrentDHTBootstrapNames = []string{"dht.transmissionbt.com", "dht.libtorrent.org"}

// torrentLanePoll paces condition polls. It is not a deadline: every wait
// polls until its condition holds and the flow's own bound ends a hang.
func torrentLanePoll() { time.Sleep(250 * time.Millisecond) }

// writeTorrentLaneNetwork adds the lane's services to a release phase's
// generated override: the proxy fixture at a fixed address, its control port
// and the lane's ports published, and the DHT bootstrap names pinned.
func writeTorrentLaneNetwork(phase *weaverReleasePhase) error {
	ip, subnet, err := net.ParseCIDR(phase.NetworkSubnet)
	if err != nil || ip.To4() == nil {
		return fmt.Errorf("invalid torrent lane subnet %q", phase.NetworkSubnet)
	}
	ip = ip.To4()
	ip[3] = 250
	if !subnet.Contains(ip) {
		return fmt.Errorf("proxy fixture address outside subnet %q", phase.NetworkSubnet)
	}
	return appendComposeOverride(phase.ComposeOverride, torrentLaneOverride(ip.String()))
}

func torrentLaneOverride(proxyIP string) string {
	var hosts strings.Builder
	for _, name := range torrentDHTBootstrapNames {
		fmt.Fprintf(&hosts, "      - \"%s:127.0.0.1\"\n", name)
	}
	// The proxy fixture forwards only what it is told: the tracker by name,
	// and BitTorrent peers on the lane's peer port by address.
	passthrough := `[{"host":"torrent-tracker","port":8080},{"host":"*","port":8080},{"host":"*","port":6881}]`
	return fmt.Sprintf(`
services:
  proxy-fixture:
    networks:
      default:
        ipv4_address: %s
    ports:
      - "8090"
    environment:
      PROXY_FIXTURE_IP: %s
      PROXY_FIXTURE_PASSTHROUGH: '%s'
  weaver:
    extra_hosts:
%s`, proxyIP, proxyIP, passthrough, hosts.String())
}

func appendComposeOverride(path, addition string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := f.WriteString(addition)
	closeErr := f.Close()
	return errors.Join(writeErr, closeErr)
}

// torrentPeerImageRepository is the rqbit image built from the librqbit fork
// revision Weaver's Cargo.lock pins.
const torrentPeerImageRepository = "weaver-e2e-torrent-peer"

var rqbitLockSource = regexp.MustCompile(`source = "git\+(https://[^"?]+/rqbit\.git)\?rev=([0-9a-f]{40})#[0-9a-f]{40}"`)

// pinnedRqbitRevision reads the fork's repository and revision from Weaver's
// Cargo.lock, so the peers are always the revision Weaver builds against.
func pinnedRqbitRevision() (string, string, error) {
	data, err := os.ReadFile(filepath.Join(weaverRepoPath(), "Cargo.lock"))
	if err != nil {
		return "", "", err
	}
	match := rqbitLockSource.FindSubmatch(data)
	if match == nil {
		return "", "", errors.New("Cargo.lock pins no rqbit git revision")
	}
	return string(match[1]), string(match[2]), nil
}

func ensureTorrentPeerImage() error {
	if image := strings.TrimSpace(os.Getenv("E2E_TORRENT_PEER_IMAGE")); image != "" && dockerImageExists(image) {
		return nil
	}
	repo, rev, err := pinnedRqbitRevision()
	if err != nil {
		return err
	}
	image := torrentPeerImageRepository + ":" + rev[:12]
	setEnv("E2E_TORRENT_PEER_IMAGE", image)
	if dockerImageExists(image) {
		log.Printf("reusing torrent peer image %s", image)
		return nil
	}
	args := []string{
		"buildx", "build", "--load", "--provenance=false",
		"--build-arg", "BOOTSTRAP_IMAGE=" + weaverImageBootstrapBase,
		"--build-arg", "RQBIT_GIT=" + repo,
		"--build-arg", "RQBIT_REV=" + rev,
		"--build-arg", "RUST_TOOLCHAIN=" + weaverPinnedRustToolchain(weaverRepoPath()),
		"-t", image,
		filepath.Join(e2eDir(), "services", "torrent-peer"),
	}
	cmd := containerengine.Command(args...)
	cmd.Dir = e2eDir()
	if err := runExternalCommand(cmd, "build torrent peer image"); err != nil {
		return err
	}
	log.Printf("built torrent peer image %s from %s@%s", image, repo, rev)
	return nil
}

// torrentLane is a running lane: Weaver, the tracker, the two peers and the
// proxy fixture, reached through their published ports.
type torrentLane struct {
	weaverURL    string
	trackerURL   string
	seederURL    string
	leecherURL   string
	proxyURL     string
	manifest     *torrentManifest
	artifacts    string
	http         *http.Client
	weaverIP     string
	proxyIP      string
	results      []torrentScenarioResult
	defaultRoute string
	trackerSeen  map[string]bool
}

func newTorrentLane(artifacts string) (*torrentLane, error) {
	manifest, err := loadTorrentManifest()
	if err != nil {
		return nil, err
	}
	lane := &torrentLane{
		weaverURL: defaultWeaverURL(),
		manifest:  manifest,
		artifacts: artifacts,
		http:      &http.Client{},
	}
	ports := []struct {
		service string
		port    string
		target  *string
	}{
		{"torrent-tracker", "8080/tcp", &lane.trackerURL},
		{"torrent-seeder", "3030/tcp", &lane.seederURL},
		{"torrent-leecher", "3030/tcp", &lane.leecherURL},
	}
	for _, item := range ports {
		port, err := inspectDockerHostPort(item.service, item.port)
		if err != nil {
			return nil, err
		}
		*item.target = fmt.Sprintf("http://127.0.0.1:%d", port)
	}
	if dockerContainerRunning("proxy-fixture") {
		if port, err := inspectDockerHostPort("proxy-fixture", "8090/tcp"); err == nil {
			lane.proxyURL = fmt.Sprintf("http://127.0.0.1:%d", port)
		}
		lane.proxyIP, _ = composeServiceIP("proxy-fixture")
	}
	lane.weaverIP, err = composeServiceIP("weaver")
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(artifacts, 0o755); err != nil {
		return nil, err
	}
	return lane, nil
}

func composeServiceIP(service string) (string, error) {
	id, err := dockerComposeServiceContainerID(service)
	if err != nil {
		return "", err
	}
	out, err := containerengine.Command("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", id).Output()
	if err != nil {
		return "", fmt.Errorf("inspect %s address: %w", service, err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return "", fmt.Errorf("%s has no network address", service)
	}
	return fields[0], nil
}

func (lane *torrentLane) fixture(slug string) *torrentFixture {
	fixture := lane.manifest.Torrents[slug]
	if fixture == nil {
		panic("unknown torrent fixture " + slug)
	}
	return fixture
}

// ---- waits ----

// waitUntil polls check until it reports done. There is no deadline: the
// flow's bound ends a wait whose condition never comes.
func waitUntil(describe string, check func() (bool, string, error)) error {
	started := time.Now()
	lastLog := started
	last := ""
	for {
		done, state, err := check()
		if err != nil {
			return fmt.Errorf("%s: %w", describe, err)
		}
		if done {
			return nil
		}
		if state != last || time.Since(lastLog) > 30*time.Second {
			log.Printf("waiting: %s (%s)", describe, state)
			last, lastLog = state, time.Now()
		}
		torrentLanePoll()
	}
}

// ---- weaver GraphQL ----

type gqlError struct {
	Message    string         `json:"message"`
	Extensions map[string]any `json:"extensions"`
}

type gqlErrors []gqlError

func (errs gqlErrors) Error() string {
	parts := make([]string, 0, len(errs))
	for _, item := range errs {
		parts = append(parts, fmt.Sprintf("%s (%v)", item.Message, item.Extensions["code"]))
	}
	return "graphql: " + strings.Join(parts, "; ")
}

func (errs gqlErrors) code() string {
	if len(errs) == 0 {
		return ""
	}
	code, _ := errs[0].Extensions["code"].(string)
	return code
}

func graphqlCode(err error) string {
	var errs gqlErrors
	if errors.As(err, &errs) {
		return errs.code()
	}
	return ""
}

func (lane *torrentLane) gql(query string, variables map[string]any, out any) error {
	payload, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	resp, err := postGraphQL(lane.weaverURL, payload)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors gqlErrors       `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("graphql status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if len(envelope.Errors) > 0 {
		return envelope.Errors
	}
	if out != nil {
		return json.Unmarshal(envelope.Data, out)
	}
	return nil
}

const torrentFields = `id name infohash state detail category private forced progress totalBytes haveBytes
uploadedBytes downloadedBytes ratio downloadBps uploadBps peers seeds leechers completedAtMs storageDir
seedingLeft seedingSource route seedingPolicy { ratioGoal seedTimeGoalMinutes inactiveGoalMinutes mode action maxUploadBps }
seedingOverride { ratioGoal action }
files { index path length selected progress }
trackers { tier url status message seeders leechers lastAnnounceMs }`

type torrentView struct {
	ID              int     `json:"id"`
	Name            string  `json:"name"`
	Infohash        string  `json:"infohash"`
	State           string  `json:"state"`
	Detail          *string `json:"detail"`
	Category        *string `json:"category"`
	Private         bool    `json:"private"`
	Forced          bool    `json:"forced"`
	Progress        float64 `json:"progress"`
	TotalBytes      int64   `json:"totalBytes"`
	HaveBytes       int64   `json:"haveBytes"`
	UploadedBytes   int64   `json:"uploadedBytes"`
	DownloadedBytes int64   `json:"downloadedBytes"`
	Ratio           float64 `json:"ratio"`
	DownloadBps     int64   `json:"downloadBps"`
	UploadBps       int64   `json:"uploadBps"`
	Peers           int     `json:"peers"`
	Seeds           int     `json:"seeds"`
	Leechers        int     `json:"leechers"`
	CompletedAtMs   *int64  `json:"completedAtMs"`
	StorageDir      *string `json:"storageDir"`
	SeedingLeft     *string `json:"seedingLeft"`
	SeedingSource   string  `json:"seedingSource"`
	Route           *string `json:"route"`
	SeedingPolicy   struct {
		RatioGoal *float64 `json:"ratioGoal"`
		Action    string   `json:"action"`
	} `json:"seedingPolicy"`
	SeedingOverride *struct {
		RatioGoal *float64 `json:"ratioGoal"`
		Action    string   `json:"action"`
	} `json:"seedingOverride"`
	Files []struct {
		Index    int     `json:"index"`
		Path     string  `json:"path"`
		Length   int64   `json:"length"`
		Selected bool    `json:"selected"`
		Progress float64 `json:"progress"`
	} `json:"files"`
	Trackers []struct {
		URL    string  `json:"url"`
		Status string  `json:"status"`
		Msg    *string `json:"message"`
	} `json:"trackers"`
}

func (view *torrentView) detail() string {
	if view == nil || view.Detail == nil {
		return ""
	}
	return *view.Detail
}

func (view *torrentView) summary() string {
	if view == nil {
		return "absent"
	}
	return fmt.Sprintf("%s %q have=%d/%d up=%d peers=%d", view.State, view.detail(), view.HaveBytes, view.TotalBytes, view.UploadedBytes, view.Peers)
}

type submitOptions struct {
	Magnet   bool
	Category string
	Paused   bool
	Forced   bool
	Seeding  map[string]any
}

func (lane *torrentLane) submit(fixture *torrentFixture, options submitOptions) (*torrentView, error) {
	input := map[string]any{"paused": options.Paused, "forced": options.Forced}
	if options.Magnet {
		input["magnet"] = fixture.Magnet
	} else {
		data, err := os.ReadFile(fixture.TorrentFile)
		if err != nil {
			return nil, err
		}
		input["metainfoBase64"] = base64.StdEncoding.EncodeToString(data)
	}
	if options.Category != "" {
		input["category"] = options.Category
	}
	if options.Seeding != nil {
		input["seeding"] = options.Seeding
	}
	var out struct {
		SubmitTorrent torrentView `json:"submitTorrent"`
	}
	err := lane.gql(`mutation($input: SubmitTorrentInput!) { submitTorrent(input: $input) { `+torrentFields+` } }`,
		map[string]any{"input": input}, &out)
	if err != nil {
		return nil, err
	}
	return &out.SubmitTorrent, nil
}

func (lane *torrentLane) torrent(id int) (*torrentView, error) {
	var out struct {
		Torrent *torrentView `json:"torrent"`
	}
	if err := lane.gql(`query($id: Int!) { torrent(id: $id) { `+torrentFields+` } }`, map[string]any{"id": id}, &out); err != nil {
		return nil, err
	}
	return out.Torrent, nil
}

func (lane *torrentLane) torrents() ([]torrentView, error) {
	var out struct {
		Torrents []torrentView `json:"torrents"`
	}
	err := lane.gql(`{ torrents { `+torrentFields+` } }`, nil, &out)
	return out.Torrents, err
}

func (lane *torrentLane) mutateBool(name string, args string, variables map[string]any) error {
	var out map[string]any
	if err := lane.gql(fmt.Sprintf("mutation%s { %s }", argDecl(variables), name+args), variables, &out); err != nil {
		return err
	}
	return nil
}

// argDecl declares the variables a simple mutation uses.
func argDecl(variables map[string]any) string {
	if len(variables) == 0 {
		return ""
	}
	types := map[string]string{
		"id": "Int!", "deleteFiles": "Boolean!", "confirmUnmetGoal": "Boolean!", "forced": "Boolean!",
		"category": "String", "seeding": "SeedingPolicyInput",
	}
	var parts []string
	for _, key := range []string{"id", "deleteFiles", "confirmUnmetGoal", "forced", "category", "seeding"} {
		if _, ok := variables[key]; ok {
			parts = append(parts, "$"+key+": "+types[key])
		}
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func (lane *torrentLane) pause(id int) error {
	return lane.mutateBool("pauseTorrent", "(id: $id)", map[string]any{"id": id})
}

func (lane *torrentLane) resume(id int) error {
	return lane.mutateBool("resumeTorrent", "(id: $id)", map[string]any{"id": id})
}

func (lane *torrentLane) setForced(id int, forced bool) error {
	return lane.mutateBool("setTorrentForced", "(id: $id, forced: $forced)", map[string]any{"id": id, "forced": forced})
}

func (lane *torrentLane) remove(id int, deleteFiles, confirm bool) error {
	return lane.mutateBool("removeTorrent", "(id: $id, deleteFiles: $deleteFiles, confirmUnmetGoal: $confirmUnmetGoal)",
		map[string]any{"id": id, "deleteFiles": deleteFiles, "confirmUnmetGoal": confirm})
}

func (lane *torrentLane) updateTorrentSettings(torrent map[string]any) error {
	return lane.gql(`mutation($input: GeneralSettingsInput!) { updateSettings(input: $input) { torrent { enabled } } }`,
		map[string]any{"input": map[string]any{"torrent": torrent}}, nil)
}

// waitTorrent polls the torrent until want accepts it.
func (lane *torrentLane) waitTorrent(id int, describe string, want func(*torrentView) bool) (*torrentView, error) {
	var seen *torrentView
	err := waitUntil(describe, func() (bool, string, error) {
		view, err := lane.torrent(id)
		if err != nil {
			return false, "", err
		}
		seen = view
		return want(view), view.summary(), nil
	})
	return seen, err
}

func seeding(view *torrentView) bool {
	return view != nil && view.State == "SEEDING" && view.HaveBytes == view.TotalBytes && view.CompletedAtMs != nil
}

// ---- tracker ----

type trackerEvent struct {
	Sequence   int64  `json:"sequence"`
	At         int64  `json:"at"`
	Transport  string `json:"transport"`
	Infohash   string `json:"infohash"`
	PeerID     string `json:"peerId"`
	Client     string `json:"client"`
	IP         string `json:"ip"`
	Port       int    `json:"port"`
	Event      string `json:"event"`
	Left       int64  `json:"left"`
	Uploaded   int64  `json:"uploaded"`
	Downloaded int64  `json:"downloaded"`
}

func (event trackerEvent) weaver() bool { return strings.HasPrefix(event.Client, "-WV") }

func (lane *torrentLane) trackerEvents(after int64) ([]trackerEvent, int64, error) {
	resp, err := lane.http.Get(fmt.Sprintf("%s/events?after=%d", lane.trackerURL, after))
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Sequence int64          `json:"sequence"`
		Events   []trackerEvent `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, err
	}
	return out.Events, out.Sequence, nil
}

func (lane *torrentLane) trackerMark() (int64, error) {
	_, sequence, err := lane.trackerEvents(0)
	return sequence, err
}

// weaverAnnounces are Weaver's announces for one infohash after a mark.
func (lane *torrentLane) weaverAnnounces(infohash string, after int64) ([]trackerEvent, error) {
	events, _, err := lane.trackerEvents(after)
	if err != nil {
		return nil, err
	}
	var out []trackerEvent
	for _, event := range events {
		if event.Infohash == infohash && event.weaver() {
			out = append(out, event)
		}
	}
	return out, nil
}

func (lane *torrentLane) waitAnnounce(infohash string, after int64, describe string, want func(trackerEvent) bool) (trackerEvent, error) {
	var found trackerEvent
	err := waitUntil(describe, func() (bool, string, error) {
		events, err := lane.weaverAnnounces(infohash, after)
		if err != nil {
			return false, "", err
		}
		for _, event := range events {
			if want(event) {
				found = event
				return true, "", nil
			}
		}
		return false, fmt.Sprintf("%d weaver announces", len(events)), nil
	})
	return found, err
}

// ---- rqbit peers ----

type peerStats struct {
	State         string `json:"state"`
	Error         string `json:"error"`
	ProgressBytes int64  `json:"progress_bytes"`
	UploadedBytes int64  `json:"uploaded_bytes"`
	TotalBytes    int64  `json:"total_bytes"`
	Finished      bool   `json:"finished"`
}

func (lane *torrentLane) peerAdd(base string, fixture *torrentFixture, folder string) error {
	data, err := os.ReadFile(fixture.TorrentFile)
	if err != nil {
		return err
	}
	query := url.Values{"overwrite": {"true"}, "output_folder": {folder}}
	resp, err := lane.http.Post(base+"/torrents?"+query.Encode(), "application/x-bittorrent", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("add %s to peer: %d %s", fixture.Slug, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (lane *torrentLane) peerStats(base, infohash string) (*peerStats, error) {
	resp, err := lane.http.Get(base + "/torrents/" + infohash + "/stats/v1")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("peer stats %s: %d %s", infohash, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var stats peerStats
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

func (lane *torrentLane) peerAction(base, infohash, action string) error {
	resp, err := lane.http.Post(base+"/torrents/"+infohash+"/"+action, "application/json", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer %s %s: %d %s", action, infohash, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (lane *torrentLane) waitPeerFinished(base, infohash, describe string) (*peerStats, error) {
	var seen *peerStats
	err := waitUntil(describe, func() (bool, string, error) {
		stats, err := lane.peerStats(base, infohash)
		if err != nil {
			return false, "", err
		}
		seen = stats
		if stats == nil {
			return false, "not added", nil
		}
		if stats.Error != "" {
			return false, "", fmt.Errorf("peer error: %s", stats.Error)
		}
		return stats.Finished && stats.ProgressBytes == stats.TotalBytes, fmt.Sprintf("%s %d/%d", stats.State, stats.ProgressBytes, stats.TotalBytes), nil
	})
	return seen, err
}

// seedAll hands the seeder every seeded torrent and waits until it holds
// each one whole.
func (lane *torrentLane) seedAll() error {
	for slug, fixture := range lane.manifest.Torrents {
		if !fixture.Seeded {
			continue
		}
		if err := lane.peerAdd(lane.seederURL, fixture, "/payload/"+slug); err != nil {
			return err
		}
	}
	for slug, fixture := range lane.manifest.Torrents {
		if !fixture.Seeded {
			continue
		}
		if _, err := lane.waitPeerFinished(lane.seederURL, fixture.Infohash, "seeder holds "+slug); err != nil {
			return err
		}
	}
	return nil
}

// ---- files on the services ----

type remoteFile struct {
	Path  string
	Size  int64
	Inode string
	Links int
}

// listRemote lists the files under dir in a service's container.
func listRemote(service, dir string) ([]remoteFile, error) {
	args := dockerComposeArgs("exec", "-T", service, "find", dir, "-type", "f", "-printf", `%P\t%s\t%i\t%n\n`)
	cmd := containerengine.Command(args...)
	cmd.Dir = e2eDir()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list %s:%s: %w: %s", service, dir, err, strings.TrimSpace(stderr.String()))
	}
	var files []remoteFile
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != 4 {
			continue
		}
		size, _ := strconv.ParseInt(fields[1], 10, 64)
		links, _ := strconv.Atoi(fields[3])
		files = append(files, remoteFile{Path: fields[0], Size: size, Inode: fields[2], Links: links})
	}
	return files, scanner.Err()
}

func remoteExists(service, path string) (bool, error) {
	cmd := containerengine.Command(dockerComposeArgs("exec", "-T", service, "test", "-e", path)...)
	cmd.Dir = e2eDir()
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	var exit interface{ ExitCode() int }
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

// remoteBLAKE3 streams a file out of a service's container and hashes it.
func remoteBLAKE3(service, path string) (string, error) {
	cmd := containerengine.Command(dockerComposeArgs("exec", "-T", service, "cat", path)...)
	cmd.Dir = e2eDir()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	hash := blake3.New()
	_, copyErr := io.Copy(hash, stdout)
	waitErr := cmd.Wait()
	if err := errors.Join(copyErr, waitErr); err != nil {
		return "", fmt.Errorf("read %s:%s: %w: %s", service, path, err, strings.TrimSpace(stderr.String()))
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

// verifyOutput checks Weaver's output for a fixture: every expected file at
// its BLAKE3, and nothing the fixture says unpacking must not leave.
func verifyTorrentOutput(fixture *torrentFixture, dir string) (map[string]any, error) {
	files, err := listRemote("weaver", dir)
	if err != nil {
		return nil, err
	}
	listing := map[string]remoteFile{}
	for _, file := range files {
		listing[file.Path] = file
	}
	evidence := map[string]any{"dir": dir, "files": files}
	var problems []string
	for rel, want := range fixture.Output {
		if _, ok := listing[rel]; !ok {
			problems = append(problems, "missing "+rel)
			continue
		}
		got, err := remoteBLAKE3("weaver", dir+"/"+rel)
		if err != nil {
			return evidence, err
		}
		if got != want {
			problems = append(problems, fmt.Sprintf("%s BLAKE3 %s, want %s", rel, got, want))
		}
	}
	for _, rel := range fixture.Absent {
		if _, ok := listing[rel]; ok {
			problems = append(problems, "unpacking left "+rel)
		}
	}
	retainedAt := map[string]string{}
	for rel, want := range fixture.Retained {
		length := int64(-1)
		for _, file := range fixture.Files {
			if file.Path == rel {
				length = file.Length
			}
		}
		for _, file := range files {
			if file.Size != length {
				continue
			}
			got, err := remoteBLAKE3("weaver", dir+"/"+file.Path)
			if err != nil {
				return evidence, err
			}
			if got == want {
				retainedAt[rel] = file.Path
				break
			}
		}
		if _, ok := retainedAt[rel]; !ok {
			problems = append(problems, "the bytes of "+rel+" are not kept under the output")
		}
	}
	if len(fixture.Retained) > 0 {
		evidence["retained_at"] = retainedAt
	}
	if len(problems) > 0 {
		return evidence, errors.New(strings.Join(problems, "; "))
	}
	return evidence, nil
}

// ---- results ----

type torrentScenarioResult struct {
	Name       string         `json:"name"`
	Status     string         `json:"status"`
	Asserts    []string       `json:"asserts"`
	Evidence   map[string]any `json:"evidence,omitempty"`
	Error      string         `json:"error,omitempty"`
	Datastore  string         `json:"datastore"`
	StartedAt  time.Time      `json:"started_at"`
	DurationMS int64          `json:"duration_ms"`
}

type torrentScenario struct {
	Name    string
	Asserts []string
	Run     func(lane *torrentLane, evidence map[string]any) error
}

func (lane *torrentLane) run(ctx context.Context, scenarios []torrentScenario) error {
	results := filepath.Join(lane.artifacts, "torrent-scenarios")
	if err := os.MkdirAll(results, 0o755); err != nil {
		return err
	}
	var failures []error
	for _, scenario := range scenarios {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Printf("torrent scenario %s: start", scenario.Name)
		started := time.Now()
		evidence := map[string]any{}
		err := runTorrentScenarioSafely(lane, scenario, evidence)
		result := torrentScenarioResult{
			Name: scenario.Name, Status: "passed", Asserts: scenario.Asserts, Evidence: evidence,
			Datastore: string(currentWeaverDatastore()), StartedAt: started.UTC(), DurationMS: time.Since(started).Milliseconds(),
		}
		if err != nil {
			result.Status, result.Error = "failed", err.Error()
			failures = append(failures, fmt.Errorf("%s: %w", scenario.Name, err))
			log.Printf("torrent scenario %s: FAILED: %v", scenario.Name, err)
		} else {
			log.Printf("torrent scenario %s: passed (%s)", scenario.Name, time.Since(started).Round(time.Millisecond))
		}
		lane.results = append(lane.results, result)
		if err := writeWeaverReleaseJSON(filepath.Join(results, scenario.Name+".json"), result); err != nil {
			failures = append(failures, fmt.Errorf("write %s result: %w", scenario.Name, err))
		}
	}
	if err := writeWeaverReleaseJSON(filepath.Join(results, "summary.json"), lane.results); err != nil {
		failures = append(failures, fmt.Errorf("write the scenario summary: %w", err))
	}
	return errors.Join(failures...)
}

func runTorrentScenarioSafely(lane *torrentLane, scenario torrentScenario, evidence map[string]any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v", recovered)
		}
	}()
	return scenario.Run(lane, evidence)
}
