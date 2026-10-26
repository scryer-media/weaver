// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"errors"
	"fmt"
	"maps"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// The runtime assertions read the graph's observability lines from the weaver
// log: a line the assertion needs and the log does not carry fails the
// scenario.

// Object kinds that open an archive, as the graph's `Kind` displays them.
var graphArchiveKinds = map[string]bool{
	"rar_store":     true,
	"rar_engine":    true,
	"sevenz_store":  true,
	"sevenz_engine": true,
	"zip":           true,
	"zip_engine":    true,
}

// Object kinds that route a member's stored bytes straight through.
var graphStoreKinds = map[string]bool{
	"rar_store":    true,
	"sevenz_store": true,
	"zip":          true,
}

// Recovery kinds that describe members by a parity set: PAR2 and PAR3. An SFV
// also binds members, by name alone, and is not one of them.
var graphParityKinds = map[string]bool{
	"par2": true,
	"par3": true,
}

// graphNoRelaunch is the restart point of a log the harness never relaunched
// weaver into.
const graphNoRelaunch = -1

// archiveFileName matches a file name an archive object would have opened: a
// RAR volume in either naming, a 7z archive or split part, a zip or its split
// parts.
var archiveFileName = regexp.MustCompile(`(?i)\.(rar|r\d{2,3}|7z|7z\.\d{3}|zip|z\d{2})$`)

func isArchiveFileName(name string) bool {
	return archiveFileName.MatchString(path.Base(filepath.ToSlash(strings.TrimSpace(name))))
}

// graphDirectUnpackVerdict judges a direct-unpack assertion by the graph's
// lines. An archive is always opened in the stream, so ForbidAnyActivity has
// nothing to judge. `delivered` is the job's output directory listing.
//
// `relaunchAt` is the byte offset in `raw` where the relaunched weaver began
// writing, or graphNoRelaunch. A restored graph job rebuilds its tree without
// binding again, so a restart leaves no second `object bound`; what a restart
// mid-download must still show is the job finishing after it with its archive
// read in place: an archive object bound at the root at any time, and every
// delivery of the job written by the relaunched process.
func graphDirectUnpackVerdict(raw string, jobID int, assertion *ScenarioDirectUnpackAssertion, delivered []string, relaunchAt int) error {
	rootArchiveBindings := 0
	for _, bound := range graphLogEvents(raw, jobID, graphObjectBoundMessage) {
		if graphArchiveKinds[bound["kind"]] && bound["depth"] == "0" {
			rootArchiveBindings++
		}
	}
	deliveries := graphLogEvents(raw, jobID, graphMemberDeliveredMessage)

	if assertion.RequireArmed && rootArchiveBindings == 0 {
		return errors.New("the graph bound no archive object over a root member of this job")
	}
	if assertion.RequireConsumed {
		if len(deliveries) == 0 {
			return errors.New("the graph delivered no member of this job")
		}
		for _, delivery := range deliveries {
			if delivery["depth"] == "0" && isArchiveFileName(delivery["name"]) {
				return fmt.Errorf("the graph delivered the archive %q itself instead of its members", delivery["name"])
			}
		}
		var archives []string
		for _, name := range delivered {
			if isArchiveFileName(name) {
				archives = append(archives, name)
			}
		}
		if len(archives) > 0 {
			sort.Strings(archives)
			return fmt.Errorf("the output directory holds archive file(s): %s", strings.Join(archives, ", "))
		}
	}
	if assertion.RequireRearmAfterRestart {
		if relaunchAt < 0 || relaunchAt > len(raw) {
			return errors.New("the harness relaunched no weaver into this log, so there is no restart to judge the job against")
		}
		if rootArchiveBindings == 0 {
			return errors.New("the graph bound no archive object over a root member of this job")
		}
		if before := graphLogEvents(raw[:relaunchAt], jobID, graphMemberDeliveredMessage); len(before) > 0 {
			return fmt.Errorf("the graph delivered %q before the restart; the job must complete after it", before[0]["name"])
		}
		if len(graphLogEvents(raw[relaunchAt:], jobID, graphMemberDeliveredMessage)) == 0 {
			return errors.New("the graph delivered no member of this job after the restart")
		}
	}
	switch reason := assertion.ExpectedDemotionReason; reason {
	case "":
	case "repair_rewrote":
		// A repair rewrites the archive's bytes in the stream and nothing
		// demotes: the members come out whole from a set that ruled repaired.
		if !graphAnyRecoverySetRuled(raw, jobID, "repaired") {
			return errors.New("no recovery set of this job ruled repaired")
		}
		if len(deliveries) == 0 {
			return errors.New("the graph delivered no member of this job")
		}
		for _, delivery := range deliveries {
			if delivery["whole"] != "true" {
				return fmt.Errorf("the graph delivered %q as it stands, not whole", delivery["name"])
			}
		}
	default:
		return fmt.Errorf("direct-unpack demotion reason %q has no graph equivalent", reason)
	}
	return nil
}

// graphVolume is one archive volume a scenario posts, by every name the graph
// may report it under: the name its articles carry and the name the NZB
// declares.
type graphVolume []string

// scenarioArchiveVolumes lists the archive volumes a scenario posts, from its
// local files, shared assets and fixture assets as they are staged, each with
// the NZB name its subject override gives it.
func scenarioArchiveVolumes(scenario *Scenario, localFiles []string) ([]graphVolume, error) {
	if scenario == nil {
		return nil, nil
	}
	names := append([]string(nil), localFiles...)
	for _, asset := range scenario.SharedAssets {
		_, staged, err := parseSharedAssetSpec(asset)
		if err != nil {
			return nil, err
		}
		names = append(names, staged)
	}
	for _, asset := range scenario.FixtureAssets {
		_, staged, err := parseFixtureAssetSpec(asset)
		if err != nil {
			return nil, err
		}
		names = append(names, staged)
	}
	sort.Strings(names)
	var volumes []graphVolume
	for _, name := range names {
		declared := strings.TrimSpace(scenario.NZBSubjectFilenameOverrides[name])
		if !isArchiveFileName(name) && !isArchiveFileName(declared) {
			continue
		}
		volume := graphVolume{name}
		if declared != "" && declared != name {
			volume = append(volume, declared)
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

// graphDirectStoreVerdict judges a direct-store assertion by the graph's
// lines. Nothing demotes: a `par2_damaged` set is one whose recovery rules
// impossible, and a `member_malformed_chain` set is one the store routes by its
// headers and the recovery rebinds by content. Routed-byte materialization, and the bare
// assertion that a set routed direct, is a store object bound over every
// volume the scenario posts.
//
// ForbidVolumeRefetch has no graph line to read: the graph fetches each
// article once and keeps it, and the contract logs no fetch, so it is left to
// the job's outcome.
func graphDirectStoreVerdict(raw string, jobID int, assertion *ScenarioDirectStoreAssertion, volumes []graphVolume) error {
	routed := assertion.RequireRoutedByteMaterialization || *assertion == (ScenarioDirectStoreAssertion{})
	switch reason := assertion.ExpectedDemotionReason; {
	case reason == "":
	case reason == "par2_damaged":
		if !graphAnyRecoverySetRuled(raw, jobID, "impossible") {
			return errors.New("no recovery set of this job ruled impossible")
		}
	case reason == "member_malformed_chain":
		// Volumes posted under each other's names: a store places each
		// volume where its own headers say it belongs, so the volumes route
		// direct, and the recovery set binds the posted names to their bytes
		// by content.
		if !slices.ContainsFunc(graphLogEvents(raw, jobID, graphRecoveryBoundMessage), func(bound map[string]string) bool {
			return bound["by"] == "content" && bound["member"] != bound["as"]
		}) {
			return fmt.Errorf("no recovery set of this job bound a posted volume to another name by content (%q)", reason)
		}
		routed = true
	default:
		return fmt.Errorf("direct-store demotion reason %q has no graph equivalent", reason)
	}

	if routed {
		if len(volumes) == 0 {
			return errors.New("the scenario posts no archive volume to route")
		}
		storeBound := make(map[string]bool)
		for _, bound := range graphLogEvents(raw, jobID, graphObjectBoundMessage) {
			if graphStoreKinds[bound["kind"]] {
				storeBound[bound["over"]] = true
			}
		}
		var unrouted []string
		for _, volume := range volumes {
			if !slices.ContainsFunc(volume, func(name string) bool { return storeBound[name] }) {
				unrouted = append(unrouted, volume[0])
			}
		}
		if len(unrouted) > 0 {
			return fmt.Errorf("no store object was bound over volume(s): %s", strings.Join(unrouted, ", "))
		}
	}
	return nil
}

func graphAnyRecoverySetRuled(raw string, jobID int, outcome string) bool {
	for _, ruled := range graphLogEvents(raw, jobID, graphRecoveryRuledMessage) {
		if ruled["outcome"] == outcome {
			return true
		}
	}
	return false
}

// graphPar2Verifications maps each clean-verification mode a scenario names to
// how the ruling is reached: the in-stream grid vouching without a byte read,
// or the recovery object hashing the member's bytes. The grid is the ruling
// whenever the index is read before the volumes it places, so a clean set may
// always be vouched by it; the named modes only say whether a read ruling is
// allowed too.
var graphPar2Verifications = map[string]string{
	"grid":          "grid",
	"file_crc":      "read",
	"quick_digest":  "read",
	"strong_decode": "read",
	"authoritative": "read",
}

// graphPar2CleanSettlementVerdict judges a clean-settlement assertion by the
// graph's `recovery set ruled` lines: one per expected set, clean, with the
// expected slice size, reached the way the set's expected modes allow. A
// `grid` ruling reads no bytes by definition, so VerificationReadBytes can
// only be zero.
func graphPar2CleanSettlementVerdict(raw string, jobID int, assertion *ScenarioPar2CleanSettlementAssertion) error {
	if assertion.VerificationReadBytes != 0 {
		return fmt.Errorf(
			"clean PAR2 settlement expects %d grid verification bytes read; a graph grid ruling reads none",
			assertion.VerificationReadBytes,
		)
	}
	observed := make(map[string]bool, len(assertion.ExpectedSetSliceSizes))
	for _, ruled := range graphLogEvents(raw, jobID, graphRecoveryRuledMessage) {
		if !graphParityKinds[ruled["kind"]] {
			continue
		}
		setID := ruled["recovery_set_id"]
		wantModes, wanted := assertion.ExpectedSetVerificationModes[setID]
		if !wanted {
			return fmt.Errorf("recovery set ruled for unexpected set %s", setID)
		}
		if observed[setID] {
			return fmt.Errorf("recovery set %s ruled more than once", setID)
		}
		observed[setID] = true
		if outcome := ruled["outcome"]; outcome != "clean" {
			return fmt.Errorf("recovery set %s ruled %q, want clean", setID, outcome)
		}
		sliceSize, err := strconv.ParseUint(ruled["slice_size"], 10, 64)
		if err != nil {
			return fmt.Errorf("recovery set %s: invalid slice_size %q", setID, ruled["slice_size"])
		}
		if want := assertion.ExpectedSetSliceSizes[setID]; sliceSize != want {
			return fmt.Errorf("recovery set %s has slice size %d, want %d", setID, sliceSize, want)
		}
		verification := ruled["verification"]
		allowed := []string{"grid"}
		for _, mode := range wantModes {
			if graphMode := graphPar2Verifications[mode]; graphMode != "" && !slices.Contains(allowed, graphMode) {
				allowed = append(allowed, graphMode)
			}
		}
		if !slices.Contains(allowed, verification) {
			return fmt.Errorf("recovery set %s ruled clean by verification %q, want one of %v", setID, verification, allowed)
		}
	}
	var missing []string
	for setID := range assertion.ExpectedSetVerificationModes {
		if !observed[setID] {
			missing = append(missing, setID)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("no recovery set ruled line for set(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

// graphFileIdentityRewrite observes a file-identity assertion in the graph's
// lines. A PAR2 classification is a PAR2 or PAR3 `recovery bound` naming the
// file as the set describes it, by content when the member was posted under a name the
// scenario forbids; a declared one is the name a member was delivered from or
// an object was bound over. A forbidden name is never what a set describes a
// member as, nor what a member is delivered under. An SFV binding names a file
// by its posted name and is never a PAR2 classification.
func graphFileIdentityRewrite(raw string, jobID int, assertion *ScenarioFileIdentityRewriteAssertion) fileIdentityRewriteObservation {
	observation := newFileIdentityRewriteObservation(assertion)
	if assertion == nil || !assertion.enabled() {
		return observation
	}
	forbidden := normalizeManifestFilenames(assertion.ForbiddenCurrentFilenames)
	isForbidden := make(map[string]bool, len(forbidden))
	for _, name := range forbidden {
		isForbidden[name] = true
	}

	// name -> where the graph showed it: "par2" (a PAR2 or PAR3 set, by
	// content or name), another recovery kind such as "sfv", or "declared".
	seen := make(map[string]map[string]struct{})
	note := func(name, how string) {
		if name == "" {
			return
		}
		if seen[name] == nil {
			seen[name] = make(map[string]struct{})
		}
		seen[name][how] = struct{}{}
	}
	par2By := make(map[string][]string)
	forbiddenSeen := make(map[string]bool)
	for _, bound := range graphLogEvents(raw, jobID, graphRecoveryBoundMessage) {
		as := bound["as"]
		parity := graphParityKinds[bound["kind"]]
		if parity {
			note(as, "par2")
		} else {
			note(as, bound["kind"])
		}
		if parity && isForbidden[bound["member"]] && bound["by"] != "content" {
			par2By[as] = append(par2By[as], fmt.Sprintf("%s by=%s from %s", as, bound["by"], bound["member"]))
		}
		if isForbidden[as] {
			forbiddenSeen[as] = true
		}
	}
	for _, delivery := range graphLogEvents(raw, jobID, graphMemberDeliveredMessage) {
		note(delivery["from"], "declared")
		if isForbidden[delivery["name"]] {
			forbiddenSeen[delivery["name"]] = true
		}
	}
	for _, bound := range graphLogEvents(raw, jobID, graphObjectBoundMessage) {
		note(bound["over"], "declared")
	}

	source := observation.RequiredClassificationSource
	for _, name := range normalizeManifestFilenames(assertion.RequiredCurrentFilenames) {
		hows, ok := seen[name]
		if !ok {
			observation.MissingCurrentFilenames = append(observation.MissingCurrentFilenames, name)
			continue
		}
		if source != "" {
			if _, ok := hows[source]; !ok {
				observation.WrongClassificationSources = append(
					observation.WrongClassificationSources,
					fmt.Sprintf("%s (found: %s)", name, joinSortedSet(hows)),
				)
				continue
			}
		}
		if source == "par2" {
			observation.WrongClassificationSources = append(observation.WrongClassificationSources, par2By[name]...)
		}
	}
	for _, name := range forbidden {
		if forbiddenSeen[name] {
			observation.ForbiddenCurrentFilenames = append(observation.ForbiddenCurrentFilenames, name)
		}
	}
	for name, hows := range seen {
		observation.ObservedCurrentFilenames = append(
			observation.ObservedCurrentFilenames,
			fmt.Sprintf("%s (%s)", name, joinSortedSet(hows)),
		)
	}
	sort.Strings(observation.ObservedCurrentFilenames)
	observation.Observed = len(observation.MissingCurrentFilenames) == 0 &&
		len(observation.ForbiddenCurrentFilenames) == 0 &&
		len(observation.WrongClassificationSources) == 0
	return observation
}

// graphDirectStoreEngagementVerdict is direct-store engagement judged from the
// graph's lines: at least one store object bound in any of the phase's jobs.
// The graph has no direct-store gate and no direct-store counters; a store
// archive is always read in place by a store object, so a phase whose jobs
// bound none never exercised the store path, whatever its outputs say.
func graphDirectStoreEngagementVerdict(raw string, jobIDs []int) error {
	if len(jobIDs) == 0 {
		return errors.New("the phase submitted no job to look for a store object in")
	}
	kinds := make(map[string]int)
	for _, bound := range graphLogEventsOfJobs(raw, graphObjectBoundMessage, jobIDs) {
		kinds[bound["kind"]]++
	}
	stores := 0
	for kind := range graphStoreKinds {
		stores += kinds[kind]
	}
	if stores == 0 {
		return fmt.Errorf(
			"no job of the phase bound a store object (%s) — the store path never engaged, "+
				"so this phase proved nothing about reading archives in place",
			strings.Join(slices.Sorted(maps.Keys(graphStoreKinds)), ", "),
		)
	}
	return nil
}
