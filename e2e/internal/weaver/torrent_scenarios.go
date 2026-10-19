// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"
)

const torrentLaneCategory = "lane-category"

// torrentScenarios are the lane's API scenarios, in the order they run.
// Each one removes what it added, so a later scenario's peers and tracker
// counts are its own.
func torrentScenarios() []torrentScenario {
	return []torrentScenario{
		{
			Name: "torrent-disabled",
			Asserts: []string{
				"with torrents off, submitTorrent refuses with TORRENTS_DISABLED",
				"the refused torrent is not listed and the tracker never hears its infohash",
			},
			Run: scenarioTorrentDisabled,
		},
		{
			Name: "metainfo-plain",
			Asserts: []string{
				"a public multi-file torrent added by metainfo downloads from the seeder and seeds",
				"every file lands under storageDir with its BLAKE3",
				"the tracker hears Weaver's started announce with the -WV client prefix",
				"files, trackers and the global seeding policy are reported",
			},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.downloadScenario("plain-multi", submitOptions{}, evidence, nil)
			},
		},
		{
			Name: "magnet",
			Asserts: []string{
				"a magnet resolves its metainfo from the seeder, then downloads and seeds",
				"the resolved name and file list match the metainfo; every file has its BLAKE3",
			},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.downloadScenario("magnet", submitOptions{Magnet: true}, evidence, nil)
			},
		},
		{
			Name: "rar-store",
			Asserts: []string{
				"a private store RAR set unpacks to its one member and the volumes are gone",
				"with the seeder gone, a leecher pulls the whole set from Weaver and every volume matches",
			},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.downloadScenario("rar-store", submitOptions{}, evidence, lane.leechFromWeaver)
			},
		},
		{
			Name: "rar-compressed",
			Asserts: []string{
				"a compressed RAR unpacks its members and keeps the archive",
				"with the seeder gone, a leecher pulls the archive from Weaver and it matches",
			},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.downloadScenario("rar-compressed", submitOptions{}, evidence, lane.leechFromWeaver)
			},
		},
		{
			Name:    "par2-set",
			Asserts: []string{"a PAR2-bearing set completes; the payload and every PAR2 file have their BLAKE3"},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.downloadScenario("par2-set", submitOptions{}, evidence, nil)
			},
		},
		{
			Name: "single-file-udp",
			Asserts: []string{
				"a single-file torrent on a UDP tracker downloads to <stem>/<name>",
				"the tracker hears Weaver over UDP",
			},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.downloadScenario("single-file", submitOptions{}, evidence, func(fixture *torrentFixture, view *torrentView, evidence map[string]any) error {
					if want := strings.TrimSuffix(fixture.Name, path.Ext(fixture.Name)); path.Base(deref(view.StorageDir)) != want {
						return fmt.Errorf("storageDir %q, want a directory named %q", deref(view.StorageDir), want)
					}
					return nil
				})
			},
		},
		{
			Name: "ratio-goal",
			Asserts: []string{
				"a private torrent with its own ratio goal 1.0 seeds to a leecher pulling only from Weaver",
				"it stops once the goal is met, with ratio >= 1 and uploaded >= its size",
				"the tracker hears Weaver's stopped announce",
				"a private torrent whose goal is met removes without the unmet-goal confirm",
			},
			Run: scenarioRatioGoal,
		},
		{
			Name: "controls",
			Asserts: []string{
				"a torrent added paused reads PAUSED with nothing in hand and no announce",
				"forced reads back; resume downloads it to seeding",
				"pause reads PAUSED with the data kept; resume returns it to SEEDING",
			},
			Run: scenarioControls,
		},
		{
			Name: "remove",
			Asserts: []string{
				"remove without data leaves every file in place with its BLAKE3",
				"remove with data deletes storageDir",
				"a private torrent with an unmet goal refuses removal with SEEDING_GOAL_UNMET and stays",
				"the confirmed removal of that torrent goes through",
			},
			Run: scenarioRemove,
		},
		{
			Name: "unwanted-extension",
			Asserts: []string{
				"a torrent carrying a .cmd file is refused with REFUSED naming the file",
				"it is not listed and the tracker never hears its infohash (checked again at the lane's end)",
			},
			Run: scenarioUnwanted,
		},
		{
			Name: "duplicate",
			Asserts: []string{
				"a second add of the same torrent, by metainfo or magnet, refuses with DUPLICATE",
				"exactly one torrent with the infohash is listed",
			},
			Run: scenarioDuplicate,
		},
		{
			Name: "category-route-and-policy",
			Asserts: []string{
				"a category with its own torrent route (an HTTP CONNECT ladder) and seeding policy",
				"its torrent lands under complete/<category>, reports seedingSource CATEGORY and the category's ratio goal",
				"its route differs from the default route, and the tracker hears it from the proxy's address, not Weaver's",
			},
			Run: scenarioCategory,
		},
		{
			Name: "restart-persistence",
			Asserts: []string{
				"a seeding private torrent survives a Weaver restart with its completion time",
				"after the restart it is never seen CHECKING_FILES or DOWNLOADING, and announces left=0",
				"with the seeder gone, a leecher pulls it from the restarted Weaver and it matches",
			},
			Run: scenarioRestart,
		},
		{
			Name: "socks-egress",
			Asserts: []string{
				"with the default torrent route on a SOCKS5 ladder, a torrent downloads through the proxy",
				"the tracker hears it only from the proxy's address, and the proxy logs its tracker and peer connections",
			},
			Run: scenarioSOCKS,
		},
		{
			Name: "download-cap",
			Asserts: []string{
				"with a 2 MiB/s global download cap, the last 75% of a 12 MiB torrent takes at least 75% of its size over 1.25x the cap",
			},
			Run: scenarioDownloadCap,
		},
		{
			Name: "bandwidth-split",
			Asserts: []string{
				"an 80% usenet split with no download limit is in force, needs a limit, and holds neither side to a rate",
				"under a 2 MiB/s limit every reading holds usenet to at least its 80% and torrents to at least their 20%, neither past the limit",
				"with usenet idle, the last 75% of a 12 MiB torrent takes at least 75% of its size over 1.25x the whole limit",
			},
			Run: scenarioBandwidthSplit,
		},
		// Both read the client facade, so they run before client-facade's
		// bad logins lock the lane's address out of logging in.
		{
			Name: "upload-cap",
			Asserts: []string{
				"setBandwidth uploadBps reads back, and the client facade's transfer/info up_rate_limit is that limit",
				"with a 1 MiB/s global upload limit, a leecher pulling only from Weaver takes the last 75% of an 8 MiB torrent in at least 75% of its size over 1.25x the limit",
			},
			Run: scenarioUploadCap,
		},
		{
			Name: "pause-all-torrents",
			Asserts: []string{
				"pauseTorrents holds every torrent PAUSED and reads torrentsPaused, with the usenet queue not paused",
				"resumeTorrents lifts the hold: the torrent paused on its own stays PAUSED, the other seeds again",
				"the client facade's torrents/stop hashes=all is the same hold: every torrent reads stoppedUP and torrentsPaused is true",
				"torrents/start hashes=all lifts it: the other seeds again and the torrent paused on its own still reads stoppedUP",
			},
			Run: scenarioPauseAllTorrents,
		},
		{
			Name: "client-facade",
			Asserts: []string{
				"auth/login with an API key returns Ok. and a SID cookie; app/webapiVersion answers 2.11.0",
				"torrents/add by file and by magnet answer Ok. and the torrents seed (uploading or stalledUP)",
				"a duplicate add answers 200 Fails.; an unwanted torrent 409; a non-magnet URL 415",
				"pause reads stoppedUP, resume reads a seeding state, delete removes it",
				"after five bad logins the next answers 429",
			},
			Run: scenarioFacade,
		},
		{
			Name: "refused-never-announced",
			Asserts: []string{
				"at the lane's end, the tracker has never heard any refused torrent's infohash",
			},
			Run: func(lane *torrentLane, evidence map[string]any) error {
				return lane.assertNeverAnnounced(evidence, "disabled", "unwanted", "facade-refused")
			},
		},
	}
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (lane *torrentLane) enableTorrents() error {
	return lane.updateTorrentSettings(map[string]any{"enabled": true, "upnp": false})
}

func (lane *torrentLane) assertNeverAnnounced(evidence map[string]any, slugs ...string) error {
	events, _, err := lane.trackerEvents(0)
	if err != nil {
		return err
	}
	var problems []string
	for _, slug := range slugs {
		infohash := lane.fixture(slug).Infohash
		count := 0
		for _, event := range events {
			if event.Infohash == infohash {
				count++
			}
		}
		evidence[slug+"_announces"] = count
		if count > 0 {
			problems = append(problems, fmt.Sprintf("tracker heard %s %d times", slug, count))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func (lane *torrentLane) assertNotListed(infohash string) error {
	views, err := lane.torrents()
	if err != nil {
		return err
	}
	for _, view := range views {
		if strings.EqualFold(view.Infohash, infohash) {
			return fmt.Errorf("refused torrent is listed as %d (%s)", view.ID, view.State)
		}
	}
	return nil
}

func scenarioTorrentDisabled(lane *torrentLane, evidence map[string]any) error {
	var settings struct {
		Settings struct {
			Torrent struct{ Enabled bool } `json:"torrent"`
		} `json:"settings"`
	}
	if err := lane.gql(`{ settings { torrent { enabled } } }`, nil, &settings); err != nil {
		return err
	}
	evidence["enabled_by_default"] = settings.Settings.Torrent.Enabled
	if settings.Settings.Torrent.Enabled {
		return errors.New("torrents are on before the lane turned them on")
	}
	fixture := lane.fixture("disabled")
	_, err := lane.submit(fixture, submitOptions{})
	evidence["error"] = fmt.Sprint(err)
	if code := graphqlCode(err); code != "TORRENTS_DISABLED" {
		return fmt.Errorf("submit with torrents off: code %q (%v), want TORRENTS_DISABLED", code, err)
	}
	if err := lane.assertNotListed(fixture.Infohash); err != nil {
		return err
	}
	if err := lane.enableTorrents(); err != nil {
		return fmt.Errorf("turn torrents on: %w", err)
	}
	return nil
}

type afterDownload func(fixture *torrentFixture, view *torrentView, evidence map[string]any) error

// downloadScenario adds a fixture, waits until it seeds whole, checks its
// output and announce, runs `after`, and removes it with its data.
type queueItemView struct {
	ID       int    `json:"id"`
	Protocol string `json:"protocol"`
	Name     string `json:"name"`
	State    string `json:"state"`
}

// waitQueueItem waits until the public queue lists the torrent's job as a
// torrent item: what the Downloads page reads.
func (lane *torrentLane) waitQueueItem(jobID int, slug string) (*queueItemView, error) {
	var found *queueItemView
	err := waitUntil("the queue lists "+slug, func() (bool, string, error) {
		var data struct {
			QueuePage struct {
				Items []queueItemView `json:"items"`
			} `json:"queuePage"`
		}
		if err := lane.gql(`query { queuePage(input: {pageIndex: 0, pageSize: 200}) { items { id protocol name state } } }`, nil, &data); err != nil {
			return false, "", err
		}
		for index := range data.QueuePage.Items {
			if data.QueuePage.Items[index].ID == jobID {
				found = &data.QueuePage.Items[index]
				return found.Protocol == "TORRENT", fmt.Sprintf("listed as %s %s", found.Protocol, found.State), nil
			}
		}
		return false, fmt.Sprintf("%d items, job %d absent", len(data.QueuePage.Items), jobID), nil
	})
	return found, err
}

func (lane *torrentLane) downloadScenario(slug string, options submitOptions, evidence map[string]any, after afterDownload) error {
	fixture := lane.fixture(slug)
	mark, err := lane.trackerMark()
	if err != nil {
		return err
	}
	view, err := lane.submit(fixture, options)
	if err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	evidence["job_id"] = view.ID
	evidence["submitted_state"] = view.State
	if !strings.EqualFold(view.Infohash, fixture.Infohash) {
		return fmt.Errorf("infohash %s, want %s", view.Infohash, fixture.Infohash)
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	view, err = lane.waitTorrent(view.ID, slug+" seeds", seeding)
	if err != nil {
		return err
	}
	evidence["seeding"] = view
	if lane.defaultRoute == "" && view.Category == nil && view.Route != nil {
		lane.defaultRoute = *view.Route
	}
	if view.Name != fixture.Name {
		return fmt.Errorf("name %q, want %q", view.Name, fixture.Name)
	}
	if view.TotalBytes != fixture.TotalBytes || view.Private != fixture.Private {
		return fmt.Errorf("total %d private %v, want %d %v", view.TotalBytes, view.Private, fixture.TotalBytes, fixture.Private)
	}
	if len(view.Files) != len(fixture.Files) {
		return fmt.Errorf("%d files reported, want %d", len(view.Files), len(fixture.Files))
	}
	if len(view.Trackers) == 0 || view.Trackers[0].URL != fixture.Tracker {
		return fmt.Errorf("trackers %+v, want %s", view.Trackers, fixture.Tracker)
	}
	output, err := verifyTorrentOutput(fixture, deref(view.StorageDir))
	evidence["output"] = output
	if err != nil {
		return err
	}
	queued, err := lane.waitQueueItem(view.ID, slug)
	evidence["queue_item"] = queued
	if err != nil {
		return err
	}
	announce, err := lane.waitAnnounce(fixture.Infohash, mark, "tracker hears "+slug, func(event trackerEvent) bool { return true })
	if err != nil {
		return err
	}
	evidence["first_announce"] = announce
	if !strings.HasPrefix(announce.Client, "-WV") {
		return fmt.Errorf("announce client %q", announce.Client)
	}
	wantTransport := "http"
	if strings.HasPrefix(fixture.Tracker, "udp://") {
		wantTransport = "udp"
	}
	if announce.Transport != wantTransport {
		return fmt.Errorf("announce over %s, want %s", announce.Transport, wantTransport)
	}
	if after != nil {
		return after(fixture, view, evidence)
	}
	return nil
}

// cleanupTorrent removes a scenario's torrent with its data, confirming
// any unmet goal; a torrent already gone is fine.
func (lane *torrentLane) cleanupTorrent(id int, evidence map[string]any) {
	err := lane.remove(id, true, true)
	if err != nil && graphqlCode(err) != "NOT_FOUND" {
		evidence["cleanup_error"] = err.Error()
	}
}

// leechFromWeaver takes the fixture away from the seeder, then has the
// leecher pull it whole from Weaver alone and checks every file.
func (lane *torrentLane) leechFromWeaver(fixture *torrentFixture, view *torrentView, evidence map[string]any) error {
	if err := lane.peerAction(lane.seederURL, fixture.Infohash, "forget"); err != nil {
		return fmt.Errorf("seeder forgets %s: %w", fixture.Slug, err)
	}
	if err := waitUntil("seeder lets go of "+fixture.Slug, func() (bool, string, error) {
		stats, err := lane.peerStats(lane.seederURL, fixture.Infohash)
		return stats == nil, "held", err
	}); err != nil {
		return err
	}
	// The seeder serves it again for the next lane run on this stack.
	defer func() {
		if err := lane.peerAdd(lane.seederURL, fixture, "/payload/"+fixture.Slug); err != nil {
			evidence["reseed_error"] = err.Error()
		}
	}()
	folder := "/leech/" + fixture.Slug
	if err := lane.peerAdd(lane.leecherURL, fixture, folder); err != nil {
		return err
	}
	defer func() { _ = lane.peerAction(lane.leecherURL, fixture.Infohash, "delete") }()
	stats, err := lane.waitPeerFinished(lane.leecherURL, fixture.Infohash, "leecher pulls "+fixture.Slug+" from Weaver")
	if err != nil {
		return err
	}
	evidence["leecher"] = stats
	var problems []string
	for _, file := range fixture.Files {
		got, err := remoteBLAKE3("torrent-leecher", folder+"/"+file.Path)
		if err != nil {
			return err
		}
		if got != file.BLAKE3 {
			problems = append(problems, fmt.Sprintf("leecher %s BLAKE3 %s, want %s", file.Path, got, file.BLAKE3))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	// The leecher had no other source, so Weaver's upload counter must come
	// to cover the whole set.
	after, err := lane.waitTorrent(view.ID, "Weaver counts the upload of "+fixture.Slug, func(view *torrentView) bool {
		return view != nil && view.UploadedBytes >= fixture.TotalBytes
	})
	evidence["after_leech"] = after
	if err != nil {
		return err
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func scenarioRatioGoal(lane *torrentLane, evidence map[string]any) error {
	fixture := lane.fixture("ratio")
	view, err := lane.submit(fixture, submitOptions{Seeding: map[string]any{"ratioGoal": 1.0, "action": "STOP"}})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	view, err = lane.waitTorrent(view.ID, "ratio torrent seeds", seeding)
	if err != nil {
		return err
	}
	if view.SeedingSource != "TORRENT" || view.SeedingPolicy.RatioGoal == nil || *view.SeedingPolicy.RatioGoal != 1.0 {
		return fmt.Errorf("policy %+v from %s, want the torrent's own ratio goal 1.0", view.SeedingPolicy, view.SeedingSource)
	}
	evidence["seeding"] = view
	mark, err := lane.trackerMark()
	if err != nil {
		return err
	}
	if err := lane.peerAction(lane.seederURL, fixture.Infohash, "forget"); err != nil {
		return err
	}
	defer func() { _ = lane.peerAdd(lane.seederURL, fixture, "/payload/"+fixture.Slug) }()
	if err := lane.peerAdd(lane.leecherURL, fixture, "/leech/"+fixture.Slug); err != nil {
		return err
	}
	defer func() { _ = lane.peerAction(lane.leecherURL, fixture.Infohash, "delete") }()
	stopped, err := lane.waitTorrent(view.ID, "ratio goal stops the torrent", func(view *torrentView) bool {
		return view != nil && view.State == "STOPPED"
	})
	if err != nil {
		return err
	}
	evidence["stopped"] = stopped
	if stopped.Ratio < 1 || stopped.UploadedBytes < fixture.TotalBytes {
		return fmt.Errorf("stopped at ratio %.3f uploaded %d of %d", stopped.Ratio, stopped.UploadedBytes, fixture.TotalBytes)
	}
	if stopped.SeedingLeft != nil {
		return fmt.Errorf("seedingLeft %q after the goal", *stopped.SeedingLeft)
	}
	event, err := lane.waitAnnounce(fixture.Infohash, mark, "tracker hears the stopped announce", func(event trackerEvent) bool {
		return event.Event == "stopped"
	})
	if err != nil {
		return err
	}
	evidence["stopped_announce"] = event
	if err := lane.remove(view.ID, false, false); err != nil {
		return fmt.Errorf("remove a private torrent whose goal is met, unconfirmed: %w", err)
	}
	return nil
}

func scenarioControls(lane *torrentLane, evidence map[string]any) error {
	fixture := lane.fixture("controls")
	mark, err := lane.trackerMark()
	if err != nil {
		return err
	}
	view, err := lane.submit(fixture, submitOptions{Paused: true})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	paused, err := lane.waitTorrent(view.ID, "added paused", func(view *torrentView) bool {
		return view != nil && view.State == "PAUSED"
	})
	if err != nil {
		return err
	}
	evidence["added_paused"] = paused
	if paused.HaveBytes != 0 {
		return fmt.Errorf("paused add has %d bytes", paused.HaveBytes)
	}
	if err := lane.setForced(view.ID, true); err != nil {
		return err
	}
	forced, err := lane.torrent(view.ID)
	if err != nil {
		return err
	}
	if !forced.Forced {
		return errors.New("forced does not read back")
	}
	// Nothing was announced while it sat paused: read before resuming.
	quiet, err := lane.weaverAnnounces(fixture.Infohash, mark)
	if err != nil {
		return err
	}
	evidence["announces_while_paused"] = len(quiet)
	if len(quiet) > 0 {
		return fmt.Errorf("a paused torrent announced %d times", len(quiet))
	}
	if err := lane.resume(view.ID); err != nil {
		return err
	}
	done, err := lane.waitTorrent(view.ID, "resumed torrent seeds", seeding)
	if err != nil {
		return err
	}
	evidence["seeding"] = done
	if err := lane.setForced(view.ID, false); err != nil {
		return err
	}
	if err := lane.pause(view.ID); err != nil {
		return err
	}
	again, err := lane.waitTorrent(view.ID, "pause", func(view *torrentView) bool { return view != nil && view.State == "PAUSED" })
	if err != nil {
		return err
	}
	evidence["paused_complete"] = again
	if again.HaveBytes != fixture.TotalBytes || again.Forced {
		return fmt.Errorf("paused torrent has %d of %d, forced %v", again.HaveBytes, fixture.TotalBytes, again.Forced)
	}
	if err := lane.resume(view.ID); err != nil {
		return err
	}
	_, err = lane.waitTorrent(view.ID, "resume to seeding", seeding)
	return err
}

func (lane *torrentLane) downloadOnly(slug string, options submitOptions) (*torrentView, error) {
	fixture := lane.fixture(slug)
	view, err := lane.submit(fixture, options)
	if err != nil {
		return nil, err
	}
	return lane.waitTorrent(view.ID, slug+" seeds", seeding)
}

func scenarioRemove(lane *torrentLane, evidence map[string]any) error {
	// Without data.
	keep := lane.fixture("remove-keep")
	view, err := lane.downloadOnly("remove-keep", submitOptions{})
	if err != nil {
		return err
	}
	dir := deref(view.StorageDir)
	if err := lane.remove(view.ID, false, false); err != nil {
		return fmt.Errorf("remove without data: %w", err)
	}
	if gone, err := lane.torrent(view.ID); err != nil || gone != nil {
		return fmt.Errorf("removed torrent still reads %s (%v)", gone.summary(), err)
	}
	kept, err := verifyTorrentOutput(keep, dir)
	evidence["kept"] = kept
	if err != nil {
		return fmt.Errorf("files after remove without data: %w", err)
	}
	// With data.
	view, err = lane.downloadOnly("remove-data", submitOptions{})
	if err != nil {
		return err
	}
	dir = deref(view.StorageDir)
	if err := lane.remove(view.ID, true, false); err != nil {
		return fmt.Errorf("remove with data: %w", err)
	}
	exists, err := remoteExists("weaver", dir)
	if err != nil {
		return err
	}
	evidence["deleted_dir"] = dir
	if exists {
		return fmt.Errorf("%s is still on disk after remove with data", dir)
	}
	// A private torrent with an unmet goal.
	view, err = lane.downloadOnly("private-goal", submitOptions{Seeding: map[string]any{"ratioGoal": 50.0, "action": "STOP"}})
	if err != nil {
		return err
	}
	err = lane.remove(view.ID, true, false)
	evidence["unconfirmed_error"] = fmt.Sprint(err)
	if code := graphqlCode(err); code != "SEEDING_GOAL_UNMET" {
		lane.cleanupTorrent(view.ID, evidence)
		return fmt.Errorf("unconfirmed remove of an unmet private goal: code %q (%v)", code, err)
	}
	if still, err := lane.torrent(view.ID); err != nil || still == nil || still.State != "SEEDING" {
		return fmt.Errorf("refused removal left %s (%v)", still.summary(), err)
	}
	if err := lane.remove(view.ID, true, true); err != nil {
		return fmt.Errorf("confirmed remove: %w", err)
	}
	if gone, err := lane.torrent(view.ID); err != nil || gone != nil {
		return fmt.Errorf("confirmed removal left %s (%v)", gone.summary(), err)
	}
	return nil
}

func scenarioUnwanted(lane *torrentLane, evidence map[string]any) error {
	fixture := lane.fixture("unwanted")
	_, err := lane.submit(fixture, submitOptions{})
	evidence["error"] = fmt.Sprint(err)
	if code := graphqlCode(err); code != "REFUSED" {
		return fmt.Errorf("unwanted torrent: code %q (%v), want REFUSED", code, err)
	}
	if !strings.Contains(err.Error(), "run.cmd") {
		return fmt.Errorf("refusal %q does not name run.cmd", err)
	}
	if err := lane.assertNotListed(fixture.Infohash); err != nil {
		return err
	}
	return lane.assertNeverAnnounced(evidence, "unwanted")
}

func scenarioDuplicate(lane *torrentLane, evidence map[string]any) error {
	fixture := lane.fixture("duplicate")
	view, err := lane.submit(fixture, submitOptions{})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	_, again := lane.submit(fixture, submitOptions{})
	_, magnet := lane.submit(fixture, submitOptions{Magnet: true})
	evidence["metainfo_again"] = fmt.Sprint(again)
	evidence["magnet_again"] = fmt.Sprint(magnet)
	if graphqlCode(again) != "DUPLICATE" || graphqlCode(magnet) != "DUPLICATE" {
		return fmt.Errorf("second adds: metainfo %v, magnet %v; want DUPLICATE for both", again, magnet)
	}
	views, err := lane.torrents()
	if err != nil {
		return err
	}
	count := 0
	for _, other := range views {
		if strings.EqualFold(other.Infohash, fixture.Infohash) {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("%d torrents listed with the infohash", count)
	}
	_, err = lane.waitTorrent(view.ID, "the first add seeds", seeding)
	return err
}

func (lane *torrentLane) saveProxy(name, kind string, port int) (int, error) {
	if lane.proxyIP == "" {
		return 0, errors.New("the proxy fixture is not running")
	}
	// Every proxy listener of the fixture asks for the same credentials.
	input := map[string]any{"name": name, "kind": kind, "enabled": true, "host": lane.proxyIP, "port": port, "resolver": "PROXY",
		"username": "fixture", "password": "fixture"}
	var saved struct{ SaveProxyProfile struct{ ID int } }
	err := lane.gql(`mutation($input: ProxyProfileInput!) { saveProxyProfile(input: $input) { id } }`, map[string]any{"input": input}, &saved)
	return saved.SaveProxyProfile.ID, err
}

func (lane *torrentLane) saveTorrentRoute(kind string, id int, proxyID int) error {
	path := map[string]any{"direct": true}
	if proxyID > 0 {
		path = map[string]any{"ladder": map[string]any{"rungs": []map[string]any{{"proxy": proxyID}}, "directFallback": false}}
	}
	route := map[string]any{"failover": "HOLD", "legs": []map[string]any{{"egressId": 0, "weight": 100, "path": path}}}
	return lane.gql(`mutation($kind: NetworkConsumerKind!, $id: Int!, $input: RouteInput!) { saveNetworkRoute(kind: $kind, id: $id, input: $input) { consumer } }`,
		map[string]any{"kind": kind, "id": id, "input": route}, nil)
}

type proxyEvent struct {
	Sequence int64  `json:"sequence"`
	Kind     string `json:"kind"`
	Route    string `json:"route"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Client   string `json:"client"`
}

func (lane *torrentLane) proxyEvents(after int64) ([]proxyEvent, int64, error) {
	if lane.proxyURL == "" {
		return nil, 0, errors.New("the proxy fixture's control port is not published")
	}
	resp, err := lane.http.Get(fmt.Sprintf("%s/events?after=%d", lane.proxyURL, after))
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Sequence int64        `json:"sequence"`
		Events   []proxyEvent `json:"events"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.Events, out.Sequence, err
}

// proxiedScenario downloads a fixture whose route runs through the proxy
// fixture's `route`, and checks the tracker hears it only from the proxy.
func (lane *torrentLane) proxiedScenario(slug, route string, options submitOptions, evidence map[string]any) (*torrentView, error) {
	fixture := lane.fixture(slug)
	_, proxyMark, err := lane.proxyEvents(0)
	if err != nil {
		return nil, err
	}
	trackerMark, err := lane.trackerMark()
	if err != nil {
		return nil, err
	}
	view, err := lane.submit(fixture, options)
	if err != nil {
		return nil, err
	}
	done, err := lane.waitTorrent(view.ID, slug+" seeds through the proxy", seeding)
	if err != nil {
		return view, err
	}
	evidence["seeding"] = done
	output, err := verifyTorrentOutput(fixture, deref(done.StorageDir))
	evidence["output"] = output
	if err != nil {
		return done, err
	}
	announces, err := lane.weaverAnnounces(fixture.Infohash, trackerMark)
	if err != nil {
		return done, err
	}
	evidence["announces"] = announces
	evidence["proxy_ip"], evidence["weaver_ip"] = lane.proxyIP, lane.weaverIP
	if len(announces) == 0 {
		return done, errors.New("the tracker heard no announce")
	}
	for _, event := range announces {
		if event.IP != lane.proxyIP {
			return done, fmt.Errorf("the tracker heard %s from %s, not the proxy %s", event.Event, event.IP, lane.proxyIP)
		}
	}
	events, _, err := lane.proxyEvents(proxyMark)
	if err != nil {
		return done, err
	}
	var tracker, peers int
	for _, event := range events {
		if event.Kind != "connected" || event.Route != route {
			continue
		}
		switch event.Port {
		case 8080:
			tracker++
		case 6881:
			peers++
		}
	}
	evidence["proxy_tracker_connections"], evidence["proxy_peer_connections"] = tracker, peers
	if tracker == 0 || peers == 0 {
		return done, fmt.Errorf("the proxy's %s route carried %d tracker and %d peer connections", route, tracker, peers)
	}
	return done, nil
}

func scenarioCategory(lane *torrentLane, evidence map[string]any) error {
	var added struct{ AddCategory struct{ ID int } }
	err := lane.gql(`mutation($input: CategoryInput!) { addCategory(input: $input) { id } }`, map[string]any{"input": map[string]any{
		"name": torrentLaneCategory, "torrentRoute": true,
		"seeding": map[string]any{"ratioGoal": 3.5, "action": "STOP"},
	}}, &added)
	if err != nil {
		return fmt.Errorf("add category: %w", err)
	}
	proxyID, err := lane.saveProxy("lane-category-connect", "HTTP_CONNECT", 8083)
	if err != nil {
		return err
	}
	if err := lane.saveTorrentRoute("TORRENT_CATEGORY", added.AddCategory.ID, proxyID); err != nil {
		return fmt.Errorf("save the category's route: %w", err)
	}
	view, err := lane.proxiedScenario("category", "tertiary", submitOptions{Category: torrentLaneCategory}, evidence)
	if view != nil {
		defer lane.cleanupTorrent(view.ID, evidence)
	}
	if err != nil {
		return err
	}
	var problems []string
	if deref(view.Category) != torrentLaneCategory {
		problems = append(problems, fmt.Sprintf("category %q", deref(view.Category)))
	}
	if view.SeedingSource != "CATEGORY" || view.SeedingPolicy.RatioGoal == nil || *view.SeedingPolicy.RatioGoal != 3.5 {
		problems = append(problems, fmt.Sprintf("policy %+v from %s, want the category's 3.5", view.SeedingPolicy, view.SeedingSource))
	}
	if !strings.HasPrefix(deref(view.StorageDir), "/data/complete/"+torrentLaneCategory+"/") {
		problems = append(problems, fmt.Sprintf("storageDir %q outside the category's directory", deref(view.StorageDir)))
	}
	evidence["default_route"] = lane.defaultRoute
	if view.Route == nil || *view.Route == lane.defaultRoute {
		problems = append(problems, fmt.Sprintf("route %q is the default route", deref(view.Route)))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}

	// Taken out of the category, the torrent falls back to the global
	// policy and moves to the default torrent route.
	if err := lane.mutateBool("setTorrentCategory", "(id: $id, category: $category)",
		map[string]any{"id": view.ID, "category": nil}); err != nil {
		return fmt.Errorf("clear the torrent's category: %w", err)
	}
	moved, err := lane.waitTorrent(view.ID, "uncategorised torrent on the default route and policy", func(view *torrentView) bool {
		return view != nil && view.Category == nil && view.SeedingSource == "GLOBAL" &&
			view.Route != nil && *view.Route == lane.defaultRoute
	})
	evidence["uncategorised"] = moved
	return err
}

func scenarioSOCKS(lane *torrentLane, evidence map[string]any) error {
	proxyID, err := lane.saveProxy("lane-socks", "SOCKS5", 8082)
	if err != nil {
		return err
	}
	if err := lane.saveTorrentRoute("TORRENT_DEFAULT", 0, proxyID); err != nil {
		return fmt.Errorf("route torrents through SOCKS: %w", err)
	}
	defer func() {
		if err := lane.saveTorrentRoute("TORRENT_DEFAULT", 0, 0); err != nil {
			evidence["restore_route_error"] = err.Error()
		}
	}()
	view, err := lane.proxiedScenario("socks", "secondary", submitOptions{}, evidence)
	if view != nil {
		defer lane.cleanupTorrent(view.ID, evidence)
	}
	return err
}

func (lane *torrentLane) setDownloadCap(bps any) error {
	return lane.gql(`mutation($input: BandwidthInput!) { setBandwidth(input: $input) { downloadBps } }`,
		map[string]any{"input": map[string]any{"downloadBps": bps}}, nil)
}

func scenarioDownloadCap(lane *torrentLane, evidence map[string]any) error {
	const capBps = 2 * mib
	if err := lane.setDownloadCap(capBps); err != nil {
		return err
	}
	defer func() {
		if err := lane.setDownloadCap(nil); err != nil {
			evidence["restore_cap_error"] = err.Error()
		}
	}()
	return lane.cappedDownload(lane.fixture("capped"), capBps, evidence, nil)
}

// cappedDownload downloads fixture under a global download limit of capBps
// and checks the transfer took at least as long as the limit allows. Torrents
// never move faster than the whole limit, whatever share of it the split gives
// them, so the bound holds under any split. sample, when set, runs at every
// progress reading.
func (lane *torrentLane) cappedDownload(fixture *torrentFixture, capBps int64, evidence map[string]any, sample func() error) error {
	// Progress is read two ways: Weaver's own fetched-bytes counter, and
	// what the seeder, the only peer holding this infohash, has sent; a
	// reading is the larger of the two. The last reading at or below a
	// quarter of the whole and the first reading of the whole bracket the
	// transfer of at least the last three quarters, so the time between
	// those readings is at least as long as that transfer took. One second
	// of the cap is allowed as a burst. Starting the bracket at the submit,
	// before any byte can move, keeps it sound when no early reading is seen.
	type reading struct {
		at    time.Time
		bytes int64
	}
	last := reading{at: time.Now(), bytes: 0}
	var full *reading
	var probeErr error
	var samples []string
	defer func() { evidence["samples"] = samples }()
	submitted := last.at
	view, err := lane.submit(fixture, submitOptions{})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	done, err := lane.waitTorrent(view.ID, fixture.Slug+" torrent seeds under the cap", func(view *torrentView) bool {
		if full == nil && probeErr == nil {
			stats, err := lane.peerStats(lane.seederURL, fixture.Infohash)
			if err != nil {
				probeErr = err
				return true
			}
			now := time.Now()
			var sent, fetched int64
			if stats != nil {
				sent = stats.UploadedBytes
			}
			state := "absent"
			if view != nil {
				fetched = view.DownloadedBytes
				state = view.State
			}
			if len(samples) < 400 {
				samples = append(samples, fmt.Sprintf("+%dms %s downloaded=%d seeder_sent=%d", now.Sub(submitted).Milliseconds(), state, fetched, sent))
			}
			bytes := max(sent, fetched)
			if bytes >= fixture.TotalBytes {
				full = &reading{at: now, bytes: bytes}
			} else if bytes <= fixture.TotalBytes/4 {
				last = reading{at: now, bytes: bytes}
			}
			if sample != nil {
				if err := sample(); err != nil {
					probeErr = err
					return true
				}
			}
		}
		return seeding(view)
	})
	if err != nil {
		return err
	}
	if probeErr != nil {
		return probeErr
	}
	evidence["seeding"] = done
	if full == nil {
		return errors.New("Weaver seeds, but neither counter ever reached the whole torrent")
	}
	window := full.at.Sub(last.at)
	measured := fixture.TotalBytes - last.bytes
	floor := time.Duration(float64(max(measured-capBps, 0)) / (1.25 * float64(capBps)) * float64(time.Second))
	evidence["cap_bps"], evidence["bytes_measured"] = capBps, measured
	evidence["window_ms"], evidence["floor_ms"] = window.Milliseconds(), floor.Milliseconds()
	if window < floor {
		return fmt.Errorf("%d bytes moved within %s under a %d B/s cap; at the cap they take at least %s", measured, window, capBps, floor)
	}
	_, err = verifyTorrentOutput(fixture, deref(done.StorageDir))
	return err
}

type bandwidthSplitView struct {
	InForce *struct {
		UsenetPct int `json:"usenetPct"`
	} `json:"inForce"`
	NeedsDownloadLimit bool   `json:"needsDownloadLimit"`
	UsenetBps          *int64 `json:"usenetBps"`
	TorrentBps         *int64 `json:"torrentBps"`
}

func (view *bandwidthSplitView) summary() string {
	pct, usenet, torrent := "none", "none", "none"
	if view.InForce != nil {
		pct = fmt.Sprint(view.InForce.UsenetPct)
	}
	if view.UsenetBps != nil {
		usenet = fmt.Sprint(*view.UsenetBps)
	}
	if view.TorrentBps != nil {
		torrent = fmt.Sprint(*view.TorrentBps)
	}
	return fmt.Sprintf("usenetPct=%s needsDownloadLimit=%t usenetBps=%s torrentBps=%s", pct, view.NeedsDownloadLimit, usenet, torrent)
}

func (lane *torrentLane) bandwidthSplit() (*bandwidthSplitView, error) {
	var out struct {
		Networking struct {
			BandwidthSplit bandwidthSplitView `json:"bandwidthSplit"`
		} `json:"networking"`
	}
	err := lane.gql(`{ networking { bandwidthSplit { inForce { usenetPct } needsDownloadLimit usenetBps torrentBps } } }`, nil, &out)
	return &out.Networking.BandwidthSplit, err
}

// waitBandwidthSplit waits for the split state, which the bandwidth tick
// publishes, to satisfy want.
func (lane *torrentLane) waitBandwidthSplit(describe string, want func(*bandwidthSplitView) bool) (*bandwidthSplitView, error) {
	var seen *bandwidthSplitView
	err := waitUntil(describe, func() (bool, string, error) {
		view, err := lane.bandwidthSplit()
		if err != nil {
			return false, "", err
		}
		seen = view
		return want(view), view.summary(), nil
	})
	return seen, err
}

func scenarioBandwidthSplit(lane *torrentLane, evidence map[string]any) error {
	const capBps = 2 * mib
	const usenetPct = 80
	torrentShare := int64(capBps - capBps*usenetPct/100)
	setBandwidth := func(input map[string]any) error {
		return lane.gql(`mutation($input: BandwidthInput!) { setBandwidth(input: $input) { downloadBps split { usenetPct } } }`,
			map[string]any{"input": input}, nil)
	}
	if err := setBandwidth(map[string]any{"downloadBps": nil, "split": map[string]any{"usenetPct": usenetPct}}); err != nil {
		return err
	}
	defer func() {
		if err := setBandwidth(map[string]any{"downloadBps": nil, "split": nil}); err != nil {
			evidence["restore_error"] = err.Error()
		}
	}()
	unlimited, err := lane.waitBandwidthSplit("the split is in force with no download limit", func(view *bandwidthSplitView) bool {
		return view.InForce != nil && view.InForce.UsenetPct == usenetPct && view.NeedsDownloadLimit
	})
	if err != nil {
		return err
	}
	evidence["split_without_limit"] = unlimited.summary()
	if unlimited.UsenetBps != nil || unlimited.TorrentBps != nil {
		return fmt.Errorf("with no download limit the split holds a side to a rate: %s", unlimited.summary())
	}
	if err := setBandwidth(map[string]any{"downloadBps": capBps}); err != nil {
		return err
	}
	// Each reading of the split under the limit holds each side to at least
	// its share and neither past the whole limit. A side that lends keeps its
	// own share as its limit, so the two may add up to more than the limit.
	check := func(view *bandwidthSplitView) error {
		if view.InForce == nil || view.InForce.UsenetPct != usenetPct || view.NeedsDownloadLimit ||
			view.UsenetBps == nil || view.TorrentBps == nil {
			return fmt.Errorf("under a %d B/s limit with an %d%% usenet split: %s", capBps, usenetPct, view.summary())
		}
		usenetShare := int64(capBps) - torrentShare
		if *view.UsenetBps < usenetShare || *view.TorrentBps < torrentShare ||
			*view.UsenetBps > capBps || *view.TorrentBps > capBps {
			return fmt.Errorf("under a %d B/s limit the split must hold usenet to %d..%d B/s and torrents to %d..%d B/s: %s",
				capBps, usenetShare, capBps, torrentShare, capBps, view.summary())
		}
		return nil
	}
	limited, err := lane.waitBandwidthSplit("the split holds the download limit", func(view *bandwidthSplitView) bool {
		return !view.NeedsDownloadLimit && view.UsenetBps != nil
	})
	if err != nil {
		return err
	}
	evidence["split_with_limit"] = limited.summary()
	if err := check(limited); err != nil {
		return err
	}
	var readings []string
	maxTorrent := *limited.TorrentBps
	defer func() {
		evidence["split_readings"] = readings
		evidence["torrent_share_bps"], evidence["max_torrent_bps"] = torrentShare, maxTorrent
	}()
	return lane.cappedDownload(lane.fixture("split"), capBps, evidence, func() error {
		view, err := lane.bandwidthSplit()
		if err != nil {
			return err
		}
		if len(readings) < 400 {
			readings = append(readings, view.summary())
		}
		if err := check(view); err != nil {
			return err
		}
		maxTorrent = max(maxTorrent, *view.TorrentBps)
		return nil
	})
}

// ---- the client facade ----

type facadeClient struct {
	base string
	sid  string
	http *http.Client
}

func (client *facadeClient) do(method, action string, body io.Reader, contentType string) (int, string, *http.Response, error) {
	request, err := http.NewRequest(method, client.base+"/api/v2/"+action, body)
	if err != nil {
		return 0, "", nil, err
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if client.sid != "" {
		request.AddCookie(&http.Cookie{Name: "SID", Value: client.sid})
	}
	resp, err := client.http.Do(request)
	if err != nil {
		return 0, "", nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(data)), resp, err
}

func (client *facadeClient) form(action string, values map[string]string) (int, string, error) {
	form := make([]string, 0, len(values))
	for key, value := range values {
		form = append(form, urlEncode(key)+"="+urlEncode(value))
	}
	status, body, _, err := client.do(http.MethodPost, action, strings.NewReader(strings.Join(form, "&")), "application/x-www-form-urlencoded")
	return status, body, err
}

func (client *facadeClient) info(hash string) ([]map[string]any, error) {
	status, body, _, err := client.do(http.MethodGet, "torrents/info?hashes="+hash, nil, "")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("torrents/info %d %s", status, body)
	}
	var list []map[string]any
	err = json.Unmarshal([]byte(body), &list)
	return list, err
}

func (client *facadeClient) waitState(hash, describe string, want ...string) (string, error) {
	var state string
	err := waitUntil(describe, func() (bool, string, error) {
		list, err := client.info(hash)
		if err != nil {
			return false, "", err
		}
		if len(list) != 1 {
			return false, fmt.Sprintf("%d listed", len(list)), nil
		}
		state, _ = list[0]["state"].(string)
		return slices.Contains(want, state), state, nil
	})
	return state, err
}

func (lane *torrentLane) addFacadeFile(client *facadeClient, fixture *torrentFixture) (int, string, error) {
	data, err := readFile(fixture.TorrentFile)
	if err != nil {
		return 0, "", err
	}
	body, contentType := multipartBody(map[string]string{}, "torrents", fixture.Slug+".torrent", data)
	status, text, _, err := client.do(http.MethodPost, "torrents/add", body, contentType)
	return status, text, err
}

var facadeSeedingStates = []string{"uploading", "stalledUP", "forcedUP"}

func scenarioFacade(lane *torrentLane, evidence map[string]any) error {
	var key struct {
		CreateApiKey struct{ RawKey string } `json:"createApiKey"`
	}
	if err := lane.gql(`mutation { createApiKey(name: "torrent-lane-facade", scope: CONTROL) { rawKey } }`, nil, &key); err != nil {
		return err
	}
	client := &facadeClient{base: lane.weaverURL, http: &http.Client{}}
	status, body, resp, err := client.do(http.MethodPost, "auth/login",
		strings.NewReader("username=lane&password="+urlEncode(key.CreateApiKey.RawKey)), "application/x-www-form-urlencoded")
	if err != nil {
		return err
	}
	evidence["login"] = fmt.Sprintf("%d %s", status, body)
	if status != http.StatusOK || body != "Ok." {
		return fmt.Errorf("login: %d %q", status, body)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "SID" {
			client.sid = cookie.Value
		}
	}
	if client.sid == "" {
		return errors.New("login set no SID cookie")
	}
	status, body, _, err = client.do(http.MethodGet, "app/webapiVersion", nil, "")
	if err != nil {
		return err
	}
	evidence["webapiVersion"] = body
	if status != http.StatusOK || body != "2.11.0" {
		return fmt.Errorf("webapiVersion: %d %q", status, body)
	}

	fileFixture, magnetFixture := lane.fixture("facade-file"), lane.fixture("facade-magnet")
	status, body, err = lane.addFacadeFile(client, fileFixture)
	if err != nil {
		return err
	}
	evidence["add_file"] = fmt.Sprintf("%d %s", status, body)
	if status != http.StatusOK || body != "Ok." {
		return fmt.Errorf("add by file: %d %q", status, body)
	}
	status, body, err = client.form("torrents/add", map[string]string{"urls": magnetFixture.Magnet})
	if err != nil {
		return err
	}
	evidence["add_magnet"] = fmt.Sprintf("%d %s", status, body)
	if status != http.StatusOK || body != "Ok." {
		return fmt.Errorf("add by magnet: %d %q", status, body)
	}
	defer func() {
		_, _, _ = client.form("torrents/delete", map[string]string{"hashes": fileFixture.Infohash + "|" + magnetFixture.Infohash, "deleteFiles": "true", "confirmUnmetGoal": "true"})
	}()
	for _, fixture := range []*torrentFixture{fileFixture, magnetFixture} {
		state, err := client.waitState(fixture.Infohash, fixture.Slug+" seeds through the facade", facadeSeedingStates...)
		if err != nil {
			return err
		}
		evidence[fixture.Slug+"_state"] = state
	}
	status, body, err = lane.addFacadeFile(client, fileFixture)
	if err != nil {
		return err
	}
	evidence["add_duplicate"] = fmt.Sprintf("%d %s", status, body)
	if status != http.StatusOK || body != "Fails." {
		return fmt.Errorf("duplicate add: %d %q, want 200 Fails.", status, body)
	}
	status, body, err = lane.addFacadeFile(client, lane.fixture("facade-refused"))
	if err != nil {
		return err
	}
	evidence["add_refused"] = fmt.Sprintf("%d %s", status, body)
	if status != http.StatusConflict {
		return fmt.Errorf("unwanted add: %d %q, want 409", status, body)
	}
	status, body, err = client.form("torrents/add", map[string]string{"urls": "http://torrent-tracker:8080/lane.torrent"})
	if err != nil {
		return err
	}
	evidence["add_url"] = fmt.Sprintf("%d %s", status, body)
	if status != http.StatusUnsupportedMediaType {
		return fmt.Errorf("non-magnet url: %d %q, want 415", status, body)
	}

	hash := fileFixture.Infohash
	if status, body, err = client.form("torrents/pause", map[string]string{"hashes": hash}); err != nil || status != http.StatusOK {
		return fmt.Errorf("pause: %d %q %v", status, body, err)
	}
	if state, err := client.waitState(hash, "facade pause", "stoppedUP"); err != nil {
		return err
	} else {
		evidence["paused_state"] = state
	}
	if status, body, err = client.form("torrents/resume", map[string]string{"hashes": hash}); err != nil || status != http.StatusOK {
		return fmt.Errorf("resume: %d %q %v", status, body, err)
	}
	if state, err := client.waitState(hash, "facade resume", facadeSeedingStates...); err != nil {
		return err
	} else {
		evidence["resumed_state"] = state
	}
	if status, body, err = client.form("torrents/delete", map[string]string{"hashes": hash, "deleteFiles": "true"}); err != nil || status != http.StatusOK {
		return fmt.Errorf("delete: %d %q %v", status, body, err)
	}
	if err := waitUntil("facade delete", func() (bool, string, error) {
		list, err := client.info(hash)
		return len(list) == 0, fmt.Sprintf("%d listed", len(list)), err
	}); err != nil {
		return err
	}

	// Last: the bad logins lock this client out of logging in for a minute.
	bad := &facadeClient{base: lane.weaverURL, http: &http.Client{}}
	var answers []string
	for range 6 {
		status, body, err := bad.form("auth/login", map[string]string{"username": "lane-bad", "password": "not-a-key"})
		if err != nil {
			return err
		}
		answers = append(answers, fmt.Sprintf("%d %s", status, body))
	}
	evidence["bad_logins"] = answers
	for index, answer := range answers[:5] {
		if answer != "200 Fails." {
			return fmt.Errorf("bad login %d: %s, want 200 Fails.", index+1, answer)
		}
	}
	if !strings.HasPrefix(answers[5], "429") {
		return fmt.Errorf("sixth bad login: %s, want 429", answers[5])
	}
	return nil
}

func scenarioRestart(lane *torrentLane, evidence map[string]any) error {
	fixture := lane.fixture("restart")
	view, err := lane.downloadOnly("restart", submitOptions{})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	evidence["before"] = view
	// What the seeder has sent for this infohash: only Weaver pulls it, so
	// a growth across the restart is bytes Weaver fetched again.
	seederBefore, err := lane.peerStats(lane.seederURL, fixture.Infohash)
	if err != nil {
		return err
	}
	evidence["seeder_before"] = seederBefore
	mark, err := lane.trackerMark()
	if err != nil {
		return err
	}
	if err := dockerComposeRestart("weaver"); err != nil {
		return err
	}
	if err := waitUntil("restarted Weaver answers GraphQL", func() (bool, string, error) {
		err := lane.gql(`{ settings { torrent { enabled } } }`, nil, nil)
		return err == nil, fmt.Sprint(err), nil
	}); err != nil {
		return err
	}
	// Every state the torrent shows from the first answer after the
	// restart until it seeds again.
	var states []string
	after, err := lane.waitTorrent(view.ID, "restarted Weaver seeds the torrent", func(seen *torrentView) bool {
		if seen == nil {
			states = append(states, "absent")
			return false
		}
		if len(states) == 0 || states[len(states)-1] != seen.State {
			states = append(states, seen.State)
		}
		return seeding(seen)
	})
	evidence["states_after_restart"] = states
	if err != nil {
		return err
	}
	evidence["after"] = after
	var problems []string
	for _, state := range states {
		if state == "CHECKING_FILES" || state == "DOWNLOADING" || state == "absent" {
			problems = append(problems, "seen "+state+" after the restart")
		}
	}
	if after.CompletedAtMs == nil || view.CompletedAtMs == nil || *after.CompletedAtMs != *view.CompletedAtMs {
		problems = append(problems, "completion time changed across the restart")
	}
	if after.DownloadedBytes > view.DownloadedBytes {
		problems = append(problems, fmt.Sprintf("downloaded grew from %d to %d", view.DownloadedBytes, after.DownloadedBytes))
	}
	announce, err := lane.waitAnnounce(fixture.Infohash, mark, "restarted Weaver announces", func(event trackerEvent) bool { return event.weaver() })
	if err != nil {
		return err
	}
	evidence["announce_after_restart"] = announce
	if announce.Left != 0 {
		problems = append(problems, fmt.Sprintf("the first announce after the restart has left=%d", announce.Left))
	}
	seederAfter, err := lane.peerStats(lane.seederURL, fixture.Infohash)
	if err != nil {
		return err
	}
	evidence["seeder_after"] = seederAfter
	if seederBefore != nil && seederAfter != nil && seederAfter.UploadedBytes > seederBefore.UploadedBytes {
		problems = append(problems, fmt.Sprintf("the seeder sent %d more bytes after the restart", seederAfter.UploadedBytes-seederBefore.UploadedBytes))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return lane.leechFromWeaver(fixture, after, evidence)
}

// facadeLogin logs a new client into the facade with a fresh CONTROL key.
func (lane *torrentLane) facadeLogin(keyName string) (*facadeClient, error) {
	var key struct {
		CreateApiKey struct{ RawKey string } `json:"createApiKey"`
	}
	if err := lane.gql(`mutation($name: String!) { createApiKey(name: $name, scope: CONTROL) { rawKey } }`,
		map[string]any{"name": keyName}, &key); err != nil {
		return nil, err
	}
	client := &facadeClient{base: lane.weaverURL, http: &http.Client{}}
	status, body, resp, err := client.do(http.MethodPost, "auth/login",
		strings.NewReader("username=lane&password="+urlEncode(key.CreateApiKey.RawKey)), "application/x-www-form-urlencoded")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK || body != "Ok." {
		return nil, fmt.Errorf("facade login: %d %q", status, body)
	}
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "SID" {
			client.sid = cookie.Value
		}
	}
	if client.sid == "" {
		return nil, errors.New("facade login set no SID cookie")
	}
	return client, nil
}

func (lane *torrentLane) setUploadCap(bps any) (*int64, error) {
	var out struct {
		SetBandwidth struct {
			UploadBps *int64 `json:"uploadBps"`
		} `json:"setBandwidth"`
	}
	err := lane.gql(`mutation($input: BandwidthInput!) { setBandwidth(input: $input) { uploadBps } }`,
		map[string]any{"input": map[string]any{"uploadBps": bps}}, &out)
	return out.SetBandwidth.UploadBps, err
}

func scenarioUploadCap(lane *torrentLane, evidence map[string]any) error {
	const capBps = 1 * mib
	fixture := lane.fixture("upload-capped")
	view, err := lane.downloadOnly(fixture.Slug, submitOptions{})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(view.ID, evidence)
	readBack, err := lane.setUploadCap(capBps)
	if err != nil {
		return err
	}
	defer func() {
		if _, err := lane.setUploadCap(nil); err != nil {
			evidence["restore_cap_error"] = err.Error()
		}
	}()
	if readBack == nil || *readBack != capBps {
		return fmt.Errorf("setBandwidth uploadBps reads back %v, want %d", readBack, capBps)
	}
	client, err := lane.facadeLogin("torrent-lane-upload-cap")
	if err != nil {
		return err
	}
	status, body, _, err := client.do(http.MethodGet, "transfer/info", nil, "")
	if err != nil {
		return err
	}
	var transfer map[string]any
	if status != http.StatusOK || json.Unmarshal([]byte(body), &transfer) != nil {
		return fmt.Errorf("transfer/info: %d %q", status, body)
	}
	evidence["transfer_info"] = transfer
	if limit, ok := transfer["up_rate_limit"].(float64); !ok || int64(limit) != capBps {
		return fmt.Errorf("transfer/info up_rate_limit %v, want %d", transfer["up_rate_limit"], capBps)
	}

	// Only Weaver holds the torrent once the seeder lets go of it, so the
	// leecher's progress is what Weaver uploaded under the limit.
	if err := lane.peerAction(lane.seederURL, fixture.Infohash, "forget"); err != nil {
		return fmt.Errorf("seeder forgets %s: %w", fixture.Slug, err)
	}
	if err := waitUntil("seeder lets go of "+fixture.Slug, func() (bool, string, error) {
		stats, err := lane.peerStats(lane.seederURL, fixture.Infohash)
		return stats == nil, "held", err
	}); err != nil {
		return err
	}
	defer func() {
		if err := lane.peerAdd(lane.seederURL, fixture, "/payload/"+fixture.Slug); err != nil {
			evidence["reseed_error"] = err.Error()
		}
	}()
	baseline := view.UploadedBytes
	// The bracket as cappedDownload reads it: the last reading at or below a
	// quarter, from the add on, and the first reading of the whole. A reading
	// is the larger of the leecher's progress and Weaver's upload counter.
	type reading struct {
		at    time.Time
		bytes int64
	}
	var samples []string
	defer func() { evidence["samples"] = samples }()
	last := reading{at: time.Now()}
	added := last.at
	folder := "/leech/" + fixture.Slug
	if err := lane.peerAdd(lane.leecherURL, fixture, folder); err != nil {
		return err
	}
	defer func() { _ = lane.peerAction(lane.leecherURL, fixture.Infohash, "delete") }()
	var full *reading
	if err := waitUntil("leecher pulls "+fixture.Slug+" from Weaver under the upload limit", func() (bool, string, error) {
		stats, err := lane.peerStats(lane.leecherURL, fixture.Infohash)
		if err != nil {
			return false, "", err
		}
		seen, err := lane.torrent(view.ID)
		if err != nil {
			return false, "", err
		}
		now := time.Now()
		var progress, uploaded int64
		finished := false
		if stats != nil {
			if stats.Error != "" {
				return false, "", fmt.Errorf("leecher error: %s", stats.Error)
			}
			progress, finished = stats.ProgressBytes, stats.Finished
		}
		if seen != nil {
			uploaded = seen.UploadedBytes - baseline
		}
		if len(samples) < 400 {
			samples = append(samples, fmt.Sprintf("+%dms leecher=%d weaver_uploaded=%d", now.Sub(added).Milliseconds(), progress, uploaded))
		}
		bytes := max(progress, uploaded)
		if full == nil && bytes >= fixture.TotalBytes {
			full = &reading{at: now, bytes: bytes}
		} else if full == nil && bytes <= fixture.TotalBytes/4 {
			last = reading{at: now, bytes: bytes}
		}
		return finished && progress == fixture.TotalBytes, fmt.Sprintf("leecher %d/%d", progress, fixture.TotalBytes), nil
	}); err != nil {
		return err
	}
	if full == nil {
		return errors.New("the leecher finished, but no reading ever reached the whole torrent")
	}
	window := full.at.Sub(last.at)
	measured := fixture.TotalBytes - last.bytes
	floor := time.Duration(float64(max(measured-capBps, 0)) / (1.25 * float64(capBps)) * float64(time.Second))
	evidence["cap_bps"], evidence["bytes_measured"] = capBps, measured
	evidence["window_ms"], evidence["floor_ms"] = window.Milliseconds(), floor.Milliseconds()
	if window < floor {
		return fmt.Errorf("%d bytes uploaded within %s under a %d B/s upload limit; at the limit they take at least %s", measured, window, capBps, floor)
	}
	for _, file := range fixture.Files {
		got, err := remoteBLAKE3("torrent-leecher", folder+"/"+file.Path)
		if err != nil {
			return err
		}
		if got != file.BLAKE3 {
			return fmt.Errorf("leecher %s BLAKE3 %s, want %s", file.Path, got, file.BLAKE3)
		}
	}
	return nil
}

type queueHoldView struct {
	IsPaused       bool `json:"isPaused"`
	TorrentsPaused bool `json:"torrentsPaused"`
}

func (lane *torrentLane) queueHold() (*queueHoldView, error) {
	var out struct {
		GlobalQueueState queueHoldView `json:"globalQueueState"`
	}
	err := lane.gql(`{ globalQueueState { isPaused torrentsPaused } }`, nil, &out)
	return &out.GlobalQueueState, err
}

// waitTorrentsHold waits until globalQueueState reads the torrents' hold as
// want, and checks the usenet queue is not paused with it.
func (lane *torrentLane) waitTorrentsHold(describe string, want bool) error {
	var seen *queueHoldView
	if err := waitUntil(describe, func() (bool, string, error) {
		view, err := lane.queueHold()
		if err != nil {
			return false, "", err
		}
		seen = view
		return view.TorrentsPaused == want, fmt.Sprintf("torrentsPaused=%t isPaused=%t", view.TorrentsPaused, view.IsPaused), nil
	}); err != nil {
		return err
	}
	if seen.IsPaused {
		return fmt.Errorf("%s: the usenet queue reads paused", describe)
	}
	return nil
}

func paused(view *torrentView) bool { return view != nil && view.State == "PAUSED" }

func scenarioPauseAllTorrents(lane *torrentLane, evidence map[string]any) error {
	own, err := lane.downloadOnly("hold-own", submitOptions{})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(own.ID, evidence)
	only, err := lane.downloadOnly("hold-only", submitOptions{})
	if err != nil {
		return err
	}
	defer lane.cleanupTorrent(only.ID, evidence)
	defer func() {
		if err := lane.mutateBool("resumeTorrents", "", nil); err != nil {
			evidence["restore_hold_error"] = err.Error()
		}
	}()
	if err := lane.pause(own.ID); err != nil {
		return err
	}
	if _, err := lane.waitTorrent(own.ID, "hold-own paused on its own", paused); err != nil {
		return err
	}

	if err := lane.mutateBool("pauseTorrents", "", nil); err != nil {
		return fmt.Errorf("pauseTorrents: %w", err)
	}
	held, err := lane.waitTorrent(only.ID, "the hold pauses hold-only", paused)
	evidence["held"] = held
	if err != nil {
		return err
	}
	if err := lane.waitTorrentsHold("pauseTorrents reads torrentsPaused", true); err != nil {
		return err
	}
	if view, err := lane.torrent(own.ID); err != nil || !paused(view) {
		return fmt.Errorf("under the hold hold-own reads %s (%v)", view.summary(), err)
	}

	if err := lane.mutateBool("resumeTorrents", "", nil); err != nil {
		return fmt.Errorf("resumeTorrents: %w", err)
	}
	if _, err := lane.waitTorrent(only.ID, "resumeTorrents lets hold-only seed", seeding); err != nil {
		return err
	}
	if err := lane.waitTorrentsHold("resumeTorrents clears torrentsPaused", false); err != nil {
		return err
	}
	stillOwn, err := lane.torrent(own.ID)
	evidence["own_after_resume"] = stillOwn
	if err != nil {
		return err
	}
	if !paused(stillOwn) {
		return fmt.Errorf("resumeTorrents resumed the torrent paused on its own: %s", stillOwn.summary())
	}

	// The client facade's every-torrent stop and start are the same hold.
	ownHash, onlyHash := lane.fixture("hold-own").Infohash, lane.fixture("hold-only").Infohash
	client, err := lane.facadeLogin("torrent-lane-pause-all")
	if err != nil {
		return err
	}
	if status, body, err := client.form("torrents/stop", map[string]string{"hashes": "all"}); err != nil || status != http.StatusOK {
		return fmt.Errorf("torrents/stop hashes=all: %d %q %v", status, body, err)
	}
	for _, hash := range []string{ownHash, onlyHash} {
		state, err := client.waitState(hash, "torrents/stop hashes=all stops "+hash, "stoppedUP")
		if err != nil {
			return err
		}
		evidence["facade_stopped_"+hash] = state
	}
	if err := lane.waitTorrentsHold("torrents/stop hashes=all reads torrentsPaused", true); err != nil {
		return err
	}
	if status, body, err := client.form("torrents/start", map[string]string{"hashes": "all"}); err != nil || status != http.StatusOK {
		return fmt.Errorf("torrents/start hashes=all: %d %q %v", status, body, err)
	}
	state, err := client.waitState(onlyHash, "torrents/start hashes=all lets hold-only seed", facadeSeedingStates...)
	if err != nil {
		return err
	}
	evidence["facade_started_only"] = state
	if err := lane.waitTorrentsHold("torrents/start hashes=all clears torrentsPaused", false); err != nil {
		return err
	}
	list, err := client.info(ownHash)
	if err != nil {
		return err
	}
	if len(list) != 1 {
		return fmt.Errorf("torrents/info lists %d torrents for hold-own", len(list))
	}
	evidence["facade_started_own"] = list[0]["state"]
	if list[0]["state"] != "stoppedUP" {
		return fmt.Errorf("torrents/start hashes=all started the torrent paused on its own: %v", list[0]["state"])
	}
	return nil
}
