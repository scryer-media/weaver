package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// pin records which client builds a benchmark session will measure. Every
// client is pinned by digest rather than by tag: a tag is a moving target, and
// a session whose meaning depends on when it happened to be started cannot be
// compared with the one before it.
func pin(args []string) error {
	flags := flag.NewFlagSet("pin", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var adaptersPath, templatePath, client, image, version, digest, entrypoint string
	var pullAll, noProbe bool
	flags.StringVar(&adaptersPath, "adapters", "", "adapter catalog to rewrite")
	flags.StringVar(&templatePath, "template", "", "catalog to rewrite from, leaving the other clients' pins as it declares them (defaults to the catalog itself)")
	flags.StringVar(&client, "client", "", "client whose image is being pinned")
	flags.StringVar(&image, "image", "", "image repository, without a tag or digest")
	flags.StringVar(&version, "version", "", "published tag to resolve to a digest")
	flags.StringVar(&digest, "digest", "", "digest to pin directly, bypassing tag resolution")
	flags.StringVar(&entrypoint, "entrypoint", "", "binary inside the image that reports its version")
	flags.BoolVar(&pullAll, "pull-all", true, "pre-pull every client image, so the first phase is not charged for a download")
	flags.BoolVar(&noProbe, "no-probe", false, "skip running the pinned image to report its version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	switch {
	case adaptersPath == "":
		return fmt.Errorf("--adapters is required")
	case client == "":
		return fmt.Errorf("--client is required")
	case image == "":
		return fmt.Errorf("--image is required")
	case version == "" && digest == "":
		return fmt.Errorf("one of --version or --digest is required")
	}
	if strings.ContainsAny(image, "@") || strings.Contains(filepath.Base(image), ":") {
		return fmt.Errorf("--image %q already carries a tag or digest; give the repository alone", image)
	}

	if digest == "" {
		resolved, err := resolveImageDigest(image, version)
		if err != nil {
			return err
		}
		digest = resolved
	}
	if !strings.HasPrefix(digest, "sha256:") {
		return fmt.Errorf("digest %q is not a sha256 digest", digest)
	}
	pinned := image + "@" + digest
	log := chainLogger(os.Stdout)
	log("pinning %s %s to %s", client, versionLabel(version), digest)

	if templatePath == "" {
		templatePath = adaptersPath
	}
	previous, err := adapterClientImage(adaptersPath, client)
	if err != nil {
		// A catalog that does not yet declare this client is a legitimate
		// starting point; only report what the previous pin was when there
		// actually was one.
		previous = "(none)"
	}
	backup, err := backupAdapterCatalog(adaptersPath)
	if err != nil {
		return err
	}
	if backup != "" {
		log("previous catalog saved as %s", filepath.Base(backup))
	}
	pins, err := rewriteAdapterCatalog(templatePath, adaptersPath, client, pinned)
	if err != nil {
		return err
	}
	log("previous %s pin: %s", client, previous)
	for _, line := range pins {
		log("  %s", line)
	}

	if pullAll {
		for _, entry := range pins {
			name, reference, found := strings.Cut(entry, " ")
			if !found {
				continue
			}
			if err := exec.Command("docker", "pull", "-q", reference).Run(); err != nil {
				return fmt.Errorf("pull %s image %s: %w", name, reference, err)
			}
			log("pulled %s", name)
		}
	}
	if !noProbe {
		reported, err := clientImageVersion(pinned, entrypoint, nil)
		if err != nil {
			return fmt.Errorf("probe the pinned image: %w", err)
		}
		log("%s reports %q", client, reported)
	}
	log("PIN-DONE")
	return nil
}

func versionLabel(version string) string {
	if version == "" {
		return "(by digest)"
	}
	return version
}

// resolveImageDigest turns a published tag into the digest the session will
// record. A tag that does not resolve is reported as such rather than pinned
// optimistically, because a session started against a half-published release
// measures whatever the registry happened to serve.
func resolveImageDigest(image, version string) (string, error) {
	output, err := exec.Command("docker", "buildx", "imagetools", "inspect",
		image+":"+version, "--format", "{{.Manifest.Digest}}").Output()
	if err != nil {
		return "", fmt.Errorf("resolve %s:%s: %w", image, version, err)
	}
	digest := strings.TrimSpace(string(output))
	if digest == "" {
		return "", fmt.Errorf("%s:%s resolved to an empty digest", image, version)
	}
	return digest, nil
}

// backupAdapterCatalog copies the catalog aside under a timestamped name, so
// the pin a past session ran under can always be recovered.
func backupAdapterCatalog(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read the adapter catalog: %w", err)
	}
	backup := fmt.Sprintf("%s.bak-%s", path, time.Now().UTC().Format("20060102T150405Z"))
	if err := os.WriteFile(backup, contents, 0o644); err != nil {
		return "", fmt.Errorf("back up the adapter catalog: %w", err)
	}
	return backup, nil
}

// rewriteAdapterCatalog writes the destination catalog from the template with
// one client's image replaced, and returns the resulting pins for the log. The
// catalog is decoded generically so fields this build does not know about
// survive the rewrite untouched.
func rewriteAdapterCatalog(templatePath, destination, client, pinned string) ([]string, error) {
	contents, err := os.ReadFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", templatePath, err)
	}
	var catalog map[string]any
	if err := json.Unmarshal(contents, &catalog); err != nil {
		return nil, fmt.Errorf("parse %s: %w", templatePath, err)
	}
	adapters, ok := catalog["adapters"].([]any)
	if !ok || len(adapters) == 0 {
		return nil, fmt.Errorf("%s declares no adapters", templatePath)
	}
	replaced := false
	pins := make([]string, 0, len(adapters))
	for _, entry := range adapters {
		adapter, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s contains an adapter that is not an object", templatePath)
		}
		name, _ := adapter["client"].(string)
		environment, ok := adapter["environment"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("adapter %s declares no environment", name)
		}
		if name == client {
			environment["CLIENT_IMAGE"] = pinned
			replaced = true
		}
		reference, _ := environment["CLIENT_IMAGE"].(string)
		if reference == "" {
			return nil, fmt.Errorf("adapter %s declares no CLIENT_IMAGE", name)
		}
		pins = append(pins, name+" "+reference)
	}
	if !replaced {
		return nil, fmt.Errorf("%s declares no client %q to pin", templatePath, client)
	}
	sort.Strings(pins)
	encoded, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return nil, err
	}
	// The catalog is replaced atomically: a session that started while a half
	// written catalog was on disk would fail for a reason unrelated to what it
	// measures.
	temporary := destination + ".pin-tmp"
	if err := os.WriteFile(temporary, append(encoded, '\n'), 0o644); err != nil {
		return nil, fmt.Errorf("write the adapter catalog: %w", err)
	}
	if err := os.Rename(temporary, destination); err != nil {
		os.Remove(temporary)
		return nil, fmt.Errorf("replace the adapter catalog: %w", err)
	}
	return pins, nil
}
