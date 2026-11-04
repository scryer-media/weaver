// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

package weaver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/scryer-media/weaver/e2e/internal/containerengine"
)

// Every compose project the harness brings up is claimed by an owner: the
// top-level run (full, scenarios, release-gate) that started it, by pid and
// run id. A run that starts sweeps every claimed project whose owner is
// gone, so stacks a crashed or killed run left behind do not pile up.
// Compose has no project-wide label, so the claim is a record under the
// owners directory, one file per project, beside the label compose puts
// on every container, network and volume.
const (
	harnessOwnerPIDEnv   = "E2E_OWNER_PID"
	harnessOwnerRunEnv   = "E2E_OWNER_RUN"
	harnessOwnersDirEnv  = "E2E_OWNERS_DIR"
	harnessImageKeepEnv  = "E2E_IMAGE_KEEP"
	defaultHarnessImages = 3
	composeProjectLabel  = "com.docker.compose.project"
	composeWorkdirLabel  = "com.docker.compose.project.working_dir"
	// An unclaimed project from before claims existed is swept only once
	// everything in it is older than this, so a run of an older harness
	// still going is left alone.
	unclaimedProjectAge = 24 * time.Hour
)

type harnessOwner struct {
	Project   string    `json:"project"`
	Run       string    `json:"run"`
	PID       int       `json:"pid"`
	ClaimedAt time.Time `json:"claimed_at"`
}

// startHarnessRun makes this process the owner of every project it and its
// children bring up. A command started by another keeps its parent as the
// owner. A top-level full, scenarios or release-gate run also sweeps what
// dead owners left.
func startHarnessRun(command string) {
	if strings.TrimSpace(os.Getenv(harnessOwnerPIDEnv)) != "" {
		return
	}
	setEnv(harnessOwnerPIDEnv, strconv.Itoa(os.Getpid()))
	setEnv(harnessOwnerRunEnv, fmt.Sprintf("%s-%s-%d", command, time.Now().UTC().Format("20060102-150405"), os.Getpid()))
	switch command {
	case "full", "scenarios", "release-gate":
	default:
		return
	}
	if err := sweepDeadHarnessProjects(processAlive, time.Now()); err != nil {
		log.Printf("warning: sweep projects of dead runs: %v", err)
	}
	if err := retireHarnessImages(); err != nil {
		log.Printf("warning: retire old harness images: %v", err)
	}
}

func harnessOwnersDir() string {
	if value := strings.TrimSpace(os.Getenv(harnessOwnersDirEnv)); value != "" {
		return absolutePath(value)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return ""
	}
	return filepath.Join(cache, "weaver-e2e", "owners")
}

var claimedProjects sync.Map

// claimComposeProject records this run as the owner of `project`, once per
// process. Outside a harness run (a unit test building compose arguments)
// there is no owner and nothing is claimed.
func claimComposeProject(project string) {
	pid, err := strconv.Atoi(strings.TrimSpace(os.Getenv(harnessOwnerPIDEnv)))
	if project == "" || err != nil || pid <= 0 {
		return
	}
	if _, done := claimedProjects.LoadOrStore(project, true); done {
		return
	}
	dir := harnessOwnersDir()
	if dir == "" {
		return
	}
	owner := harnessOwner{Project: project, Run: os.Getenv(harnessOwnerRunEnv), PID: pid, ClaimedAt: time.Now().UTC()}
	if err := os.MkdirAll(dir, 0o755); err == nil {
		err = writeWeaverReleaseJSON(filepath.Join(dir, project+".json"), owner)
	}
	if err != nil {
		log.Printf("warning: claim compose project %s: %v", project, err)
	}
}

func readHarnessOwners(dir string) map[string]harnessOwner {
	owners := map[string]harnessOwner{}
	if dir == "" {
		return owners
	}
	paths, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var owner harnessOwner
		if json.Unmarshal(body, &owner) == nil && owner.Project != "" {
			owners[owner.Project] = owner
		}
	}
	return owners
}

type engineResourceKind int

const (
	engineContainer engineResourceKind = iota
	engineNetwork
	engineVolume
)

func (kind engineResourceKind) String() string {
	return [...]string{"container", "network", "volume"}[kind]
}

type engineResource struct {
	Kind    engineResourceKind
	Name    string
	Project string
	// The compose working directory, which only containers carry.
	Workdir string
	Created time.Time
}

// A project only this harness names: its own run ids put "weaver-e2e-" in
// front. Other prefixes ("e2e", "e2e-", "adversarial-") are shared with
// other harnesses on the same engine, so an unclaimed project under one of
// them is ours only when a container of it was brought up from a Weaver e2e
// directory.
func harnessProjectName(project string) bool {
	return strings.HasPrefix(project, "weaver-e2e-")
}

func weaverE2EWorkdir(dir string) bool {
	dir = filepath.ToSlash(filepath.Clean(dir))
	return strings.HasSuffix(dir, "/e2e") && strings.Contains(dir, "/weaver/")
}

// selectDeadHarnessResources picks what a run start removes: every resource
// of a project whose owner is dead, and of an unclaimed project of ours
// whose every resource is older than unclaimedProjectAge. Containers come
// first, then networks, then volumes, the order the engine can remove them
// in.
func selectDeadHarnessResources(
	resources []engineResource,
	owners map[string]harnessOwner,
	alive func(int) bool,
	now time.Time,
) (doomed []engineResource, projects []string) {
	byProject := map[string][]engineResource{}
	for _, resource := range resources {
		if resource.Project != "" {
			byProject[resource.Project] = append(byProject[resource.Project], resource)
		}
	}
	for project, held := range byProject {
		if owner, claimed := owners[project]; claimed {
			if alive(owner.PID) {
				continue
			}
		} else {
			ours := harnessProjectName(project)
			for _, resource := range held {
				if resource.Kind == engineContainer && weaverE2EWorkdir(resource.Workdir) {
					ours = true
				}
			}
			old := true
			for _, resource := range held {
				if resource.Created.IsZero() || now.Sub(resource.Created) < unclaimedProjectAge {
					old = false
				}
			}
			if !ours || !old {
				continue
			}
		}
		doomed = append(doomed, held...)
		projects = append(projects, project)
	}
	sort.SliceStable(doomed, func(i, j int) bool {
		if doomed[i].Kind != doomed[j].Kind {
			return doomed[i].Kind < doomed[j].Kind
		}
		return doomed[i].Name < doomed[j].Name
	})
	sort.Strings(projects)
	return doomed, projects
}

func sweepDeadHarnessProjects(alive func(int) bool, now time.Time) error {
	resources, err := listComposeResources()
	if err != nil {
		return err
	}
	dir := harnessOwnersDir()
	doomed, projects := selectDeadHarnessResources(resources, readHarnessOwners(dir), alive, now)
	if len(projects) == 0 {
		return nil
	}
	log.Printf("sweeping %d projects of dead runs: %s", len(projects), strings.Join(projects, ", "))
	var errs []error
	for _, kind := range []engineResourceKind{engineContainer, engineNetwork, engineVolume} {
		var names []string
		for _, resource := range doomed {
			if resource.Kind == kind {
				names = append(names, resource.Name)
			}
		}
		if len(names) == 0 {
			continue
		}
		args := map[engineResourceKind][]string{
			engineContainer: {"rm", "-f", "-v"},
			engineNetwork:   {"network", "rm"},
			engineVolume:    {"volume", "rm", "-f"},
		}[kind]
		if output, err := containerengine.Command(append(args, names...)...).CombinedOutput(); err != nil {
			errs = append(errs, fmt.Errorf("remove %ss: %w: %s", kind, err, strings.TrimSpace(string(output))))
		}
	}
	if dir != "" {
		for _, project := range projects {
			_ = os.Remove(filepath.Join(dir, project+".json"))
		}
	}
	return errors.Join(errs...)
}

func listComposeResources() ([]engineResource, error) {
	var out []engineResource
	for _, listing := range []struct {
		kind engineResourceKind
		args []string
	}{
		{engineContainer, []string{"ps", "-a", "--filter", "label=" + composeProjectLabel, "--format", "json"}},
		{engineNetwork, []string{"network", "ls", "--filter", "label=" + composeProjectLabel, "--format", "json"}},
		{engineVolume, []string{"volume", "ls", "--filter", "label=" + composeProjectLabel, "--format", "json"}},
	} {
		output, err := containerengine.Command(listing.args...).Output()
		if err != nil {
			return nil, fmt.Errorf("list %ss: %w", listing.kind, err)
		}
		resources, err := parseEngineListing(listing.kind, output)
		if err != nil {
			return nil, fmt.Errorf("read %s listing: %w", listing.kind, err)
		}
		out = append(out, resources...)
	}
	return out, nil
}

// parseEngineListing reads a `--format json` listing as Podman (one JSON
// array) or Docker (one object a line) writes it.
func parseEngineListing(kind engineResourceKind, output []byte) ([]engineResource, error) {
	objects, err := engineJSONObjects(output)
	if err != nil {
		return nil, err
	}
	var out []engineResource
	for _, object := range objects {
		labels := engineLabels(object)
		resource := engineResource{
			Kind:    kind,
			Project: labels[composeProjectLabel],
			Workdir: labels[composeWorkdirLabel],
			Created: engineCreated(object),
		}
		switch kind {
		case engineContainer:
			resource.Name = engineString(object, "Id", "ID", "id")
		default:
			resource.Name = engineString(object, "Name", "name")
		}
		if resource.Name != "" {
			out = append(out, resource)
		}
	}
	return out, nil
}

func engineJSONObjects(output []byte) ([]map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(output)
	if len(trimmed) == 0 {
		return nil, nil
	}
	var objects []map[string]json.RawMessage
	if trimmed[0] == '[' {
		return objects, json.Unmarshal(trimmed, &objects)
	}
	for _, line := range bytes.Split(trimmed, []byte("\n")) {
		if line = bytes.TrimSpace(line); len(line) == 0 {
			continue
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(line, &object); err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	return objects, nil
}

func engineString(object map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		var value string
		if raw, ok := object[key]; ok && json.Unmarshal(raw, &value) == nil && value != "" {
			return value
		}
	}
	return ""
}

// engineLabels reads labels as a map (Podman) or as Docker's "k=v,k=v".
func engineLabels(object map[string]json.RawMessage) map[string]string {
	for _, key := range []string{"Labels", "labels"} {
		raw, ok := object[key]
		if !ok {
			continue
		}
		var labels map[string]string
		if json.Unmarshal(raw, &labels) == nil && labels != nil {
			return labels
		}
		var flat string
		if json.Unmarshal(raw, &flat) == nil {
			labels = map[string]string{}
			for _, pair := range strings.Split(flat, ",") {
				if key, value, found := strings.Cut(pair, "="); found {
					labels[key] = value
				}
			}
			return labels
		}
	}
	return map[string]string{}
}

// engineCreated reads a creation time as unix seconds or as a timestamp in
// the layouts the engines write. An unreadable one is zero, which keeps an
// unclaimed project.
func engineCreated(object map[string]json.RawMessage) time.Time {
	for _, key := range []string{"Created", "created", "CreatedAt"} {
		raw, ok := object[key]
		if !ok {
			continue
		}
		var seconds int64
		if json.Unmarshal(raw, &seconds) == nil && seconds > 0 {
			return time.Unix(seconds, 0)
		}
		var text string
		if json.Unmarshal(raw, &text) != nil {
			continue
		}
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05 -0700 MST"} {
			if parsed, err := time.Parse(layout, text); err == nil {
				return parsed
			}
		}
	}
	return time.Time{}
}

type harnessImage struct {
	ID      string
	Ref     string
	Group   string
	Created time.Time
}

// harnessImageGroup names the family a tag competes in for retention, or ""
// for an image retention never touches: the Weaver and Playwright images by
// repository, and each NNTP corpus image by its profile and role.
func harnessImageGroup(ref string) string {
	repository, tag, found := strings.Cut(ref[strings.LastIndex(ref, "/")+1:], ":")
	if !found || tag == "latest" {
		return ""
	}
	switch repository {
	case "weaver-e2e-weaver", "weaver-e2e-playwright":
		return repository
	case "weaver-e2e-nntp":
		if !strings.HasPrefix(tag, "corpus-") {
			return ""
		}
		if at := strings.LastIndex(tag, "-"); at > len("corpus-") {
			return repository + ":" + tag[:at]
		}
	}
	return ""
}

// selectRetiredHarnessImages keeps the newest `keep` tags of each family and
// picks the rest.
func selectRetiredHarnessImages(images []harnessImage, keep int) []string {
	byGroup := map[string][]harnessImage{}
	for _, image := range images {
		if image.Group != "" {
			byGroup[image.Group] = append(byGroup[image.Group], image)
		}
	}
	var retired []string
	for _, group := range byGroup {
		sort.SliceStable(group, func(i, j int) bool { return group[i].Created.After(group[j].Created) })
		for index, image := range group {
			if index >= keep {
				retired = append(retired, image.Ref)
			}
		}
	}
	sort.Strings(retired)
	return retired
}

func harnessImageKeep() int {
	if value := strings.TrimSpace(os.Getenv(harnessImageKeepEnv)); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed >= 1 {
			return parsed
		}
	}
	return defaultHarnessImages
}

// retireHarnessImages untags old fingerprint-tagged images one tag at a
// time, keeping images referenced by running or stopped containers. The
// engine also protects references acquired after the inventory was read.
func retireHarnessImages() error {
	return retireHarnessImagesUsing(harnessImageKeep(), func(args ...string) ([]byte, error) {
		command := containerengine.Command(args...)
		if args[0] == "rmi" {
			return command.CombinedOutput()
		}
		return command.Output()
	})
}

func retireHarnessImagesUsing(keep int, command func(...string) ([]byte, error)) error {
	output, err := command("images", "--no-trunc", "--format", "json")
	if err != nil {
		return err
	}
	images, err := parseHarnessImages(output)
	if err != nil {
		return err
	}
	retired := selectRetiredHarnessImages(images, keep)
	if len(retired) == 0 {
		return nil
	}
	output, err = command("ps", "-a", "-q", "--no-trunc")
	if err != nil {
		return fmt.Errorf("list containers before image retirement: %w", err)
	}
	used := map[string]bool{}
	containers := strings.Fields(string(output))
	for len(containers) > 0 {
		count := min(128, len(containers))
		output, err = command(append([]string{"container", "inspect"}, containers[:count]...)...)
		if err != nil {
			return fmt.Errorf("inspect containers before image retirement: %w", err)
		}
		objects, err := engineJSONObjects(output)
		if err != nil {
			return fmt.Errorf("read container images: %w", err)
		}
		if len(objects) != count {
			return fmt.Errorf("container image inventory returned %d of %d containers", len(objects), count)
		}
		for _, object := range objects {
			id := engineString(object, "Image", "ImageID", "ImageId")
			if id == "" {
				return errors.New("container image inventory has no image ID")
			}
			used[strings.TrimPrefix(id, "sha256:")] = true
		}
		containers = containers[count:]
	}
	ids := map[string]string{}
	for _, image := range images {
		ids[image.Ref] = image.ID
	}
	var errs []error
	for _, ref := range retired {
		if ids[ref] == "" {
			errs = append(errs, fmt.Errorf("image %s has no immutable ID; keeping it", ref))
			continue
		}
		if used[ids[ref]] {
			continue
		}
		if output, err := command("rmi", ref); err != nil && !imageRetirementInUse(output) {
			errs = append(errs, fmt.Errorf("untag %s: %w: %s", ref, err, strings.TrimSpace(string(output))))
		}
	}
	return errors.Join(errs...)
}

// Podman can also retain external build containers absent from `ps`, and
// another run can acquire an image after the inventory. Those refusals keep
// the image; unrelated engine errors still reach the caller.
func imageRetirementInUse(output []byte) bool {
	text := strings.ToLower(string(output))
	return strings.Contains(text, "image is in use by a container") ||
		strings.Contains(text, "image is being used by")
}

func parseHarnessImages(output []byte) ([]harnessImage, error) {
	objects, err := engineJSONObjects(output)
	if err != nil {
		return nil, err
	}
	var images []harnessImage
	for _, object := range objects {
		created := engineCreated(object)
		id := strings.TrimPrefix(engineString(object, "Id", "ID", "id"), "sha256:")
		var refs []string
		if raw, ok := object["Names"]; ok {
			_ = json.Unmarshal(raw, &refs)
		}
		if len(refs) == 0 {
			repository, tag := engineString(object, "Repository"), engineString(object, "Tag")
			if repository != "" && tag != "" && tag != "<none>" {
				refs = []string{repository + ":" + tag}
			}
		}
		for _, ref := range refs {
			if group := harnessImageGroup(ref); group != "" {
				images = append(images, harnessImage{ID: id, Ref: ref, Group: group, Created: created})
			}
		}
	}
	return images, nil
}
