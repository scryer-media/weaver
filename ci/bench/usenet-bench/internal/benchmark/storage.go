package benchmark

import (
	"fmt"
	"os"
	"strconv"
)

// StorageKind separates a client whose download directories are ordinary local
// directories from one whose directories live on a shaped NFS export. It is a
// first-class plan dimension: completion behaviour over network storage is a
// different measurement, never a variant of the local one.
type StorageKind string

const (
	StorageLocal StorageKind = "local"
	StorageNFS   StorageKind = "nfs"
)

const (
	// StorageProfileLocal keeps both client directories on the benchmark
	// host's own disk. It is the default and the published headline.
	StorageProfileLocal = "local"
	// StorageProfileNFSAll places the intermediate and completion directories
	// on the export, so every intermediate write also crosses the link.
	StorageProfileNFSAll = "nfs-all"
	// StorageProfileNFSComplete keeps the intermediate directory local and
	// places only the completion directory on the export. This is the common
	// consumer NAS layout and the reason the axis exists: it separates a
	// client that assembles into its final location from one that assembles
	// locally and then copies.
	StorageProfileNFSComplete = "nfs-complete"
)

const (
	// NFSLinkNone is the only link identifier a local profile may carry.
	NFSLinkNone     = "none"
	NFSLink100Mbit  = "nas-100mbit"
	NFSLink1Gbit    = "nas-1gbit"
	NFSLink2500Mbit = "nas-2.5gbit"
)

// DefaultNFSImage is the locally built shaped-NFS image. Like the NNTP
// shaper, it is harness infrastructure built from this repository and is never
// pulled from a registry; it also doubles as the verification helper, so the
// harness introduces exactly one image for the whole storage lane.
const DefaultNFSImage = "weaver-nntp-bench-nfs:dev"

const (
	storageScopeNone    = "none"
	storageScopeNFSLink = "nfs_server_link"
	// storageShaperNone and storageShaperTBFNetem name the *declared* queueing
	// discipline. The mechanism actually used for each direction is discovered
	// at run time and recorded in the attestation, because the client-to-server
	// direction depends on whether the container can create an ifb device.
	storageShaperNone     = "none"
	storageShaperTBFNetem = "tbf+netem"
)

// nfsMountOptions are the client mount options every NFS profile uses, minus
// the run-specific server address. NFSv4.1 is pinned rather than negotiated:
// 4.0 needs a server-to-client callback port, while 4.1 carries its back
// channel on the same TCP connection, so a single shaped port is enough.
const nfsMountOptions = "nfsvers=4.1,hard,proto=tcp,timeo=600,retrans=2,rsize=1048576,wsize=1048576,noatime"

// nfsExportOptions are the export options the NFS container applies. async is
// deliberate and recorded: this benchmark measures how a client behaves over a
// slow link, not how fast the container's backing store can fsync, and sync
// would fold that unrelated latency into every write. no_root_squash and
// insecure are acceptable only because the export exists on an isolated
// benchmark network for the lifetime of one run.
const nfsExportOptions = "rw,no_subtree_check,no_root_squash,insecure,async,fsid=0"

// StorageProfile is the complete, serializable storage contract for a run. It
// is comparable on purpose: a result carries it back unchanged and is rejected
// if a single field drifted from the plan.
type StorageProfile struct {
	ID                string      `json:"id"`
	Kind              StorageKind `json:"kind"`
	NFSLinkID         string      `json:"nfs_link_id"`
	IntermediateOnNFS bool        `json:"intermediate_on_nfs"`
	CompleteOnNFS     bool        `json:"complete_on_nfs"`
	LinkBitsPerSecond uint64      `json:"link_bits_per_second"`
	LinkBurstBytes    uint64      `json:"link_burst_bytes"`
	RTTMicros         uint64      `json:"rtt_micros"`
	MountOptions      string      `json:"mount_options"`
	ExportOptions     string      `json:"export_options"`
	Shaper            string      `json:"shaper"`
	AttestationScope  string      `json:"attestation_scope"`
}

// nfsLink is one named, fixed NFS link. Like the server link profiles, a named
// identifier never silently changes its rate, burst, or delay.
type nfsLink struct {
	BitsPerSecond uint64
	BurstBytes    uint64
	// RTTMicros is the full round trip. The container applies half of it as a
	// fixed one-way delay in each direction with zero jitter.
	RTTMicros uint64
}

func namedNFSLinks() map[string]nfsLink {
	return map[string]nfsLink{
		NFSLink100Mbit:  {BitsPerSecond: 100_000_000, BurstBytes: 1 << 17, RTTMicros: 1_000},
		NFSLink1Gbit:    {BitsPerSecond: 1_000_000_000, BurstBytes: 1 << 20, RTTMicros: 1_000},
		NFSLink2500Mbit: {BitsPerSecond: 2_500_000_000, BurstBytes: 1 << 21, RTTMicros: 1_000},
	}
}

func DefaultStorageProfile() StorageProfile {
	return StorageProfile{
		ID:               StorageProfileLocal,
		Kind:             StorageLocal,
		NFSLinkID:        NFSLinkNone,
		Shaper:           storageShaperNone,
		AttestationScope: storageScopeNone,
	}
}

// ResolveStorageProfile returns the complete profile for a declared identifier.
// An NFS profile must name its link explicitly; no benchmark silently picks a
// storage speed on the operator's behalf.
func ResolveStorageProfile(id, nfsLinkID string) (StorageProfile, error) {
	switch id {
	case "", StorageProfileLocal:
		if nfsLinkID != "" && nfsLinkID != NFSLinkNone {
			return StorageProfile{}, fmt.Errorf("local storage profile cannot select NFS link %q", nfsLinkID)
		}
		return DefaultStorageProfile(), nil
	case StorageProfileNFSAll, StorageProfileNFSComplete:
		if nfsLinkID == "" || nfsLinkID == NFSLinkNone {
			return StorageProfile{}, fmt.Errorf("storage profile %q requires an explicit NFS link profile", id)
		}
		link, ok := namedNFSLinks()[nfsLinkID]
		if !ok {
			return StorageProfile{}, fmt.Errorf("unsupported NFS link profile %q", nfsLinkID)
		}
		return StorageProfile{
			ID:                id,
			Kind:              StorageNFS,
			NFSLinkID:         nfsLinkID,
			IntermediateOnNFS: id == StorageProfileNFSAll,
			CompleteOnNFS:     true,
			LinkBitsPerSecond: link.BitsPerSecond,
			LinkBurstBytes:    link.BurstBytes,
			RTTMicros:         link.RTTMicros,
			MountOptions:      nfsMountOptions,
			ExportOptions:     nfsExportOptions,
			Shaper:            storageShaperTBFNetem,
			AttestationScope:  storageScopeNFSLink,
		}, nil
	default:
		return StorageProfile{}, fmt.Errorf("unsupported storage profile %q", id)
	}
}

func (p StorageProfile) Validate() error {
	resolved, err := ResolveStorageProfile(p.ID, p.NFSLinkID)
	if err != nil {
		return err
	}
	if p != resolved {
		return fmt.Errorf("storage profile %q does not match its declared fixed values", p.ID)
	}
	return nil
}

// OneWayDelayMicros is the fixed per-direction delay the shaper applies. Both
// directions carry the same delay, so the observed round trip is RTTMicros.
func (p StorageProfile) OneWayDelayMicros() uint64 {
	return p.RTTMicros / 2
}

func (p StorageProfile) usesNFS() bool {
	return p.Kind == StorageNFS
}

// validateStorageProfileTargets keeps NFS profiles inside the Docker lane. A
// native macOS or Windows lane would need the host kernel to mount the export
// as root, which the harness will not do to an operator's workstation.
func validateStorageProfileTargets(profile StorageProfile, targets []ExecutionTarget) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	if !profile.usesNFS() {
		return nil
	}
	for _, target := range targets {
		if target != DockerLinux {
			return fmt.Errorf("storage profile %q is only supported on the %s target, not %q", profile.ID, DockerLinux, target)
		}
	}
	return nil
}

// WriteStorageLinkEnvironment produces the immutable Compose-compatible env
// file the NFS server container consumes. It is the storage counterpart of
// WriteServerLinkEnvironment and refuses to describe a local profile, which
// has no container to configure.
func WriteStorageLinkEnvironment(path string, profile StorageProfile) error {
	if err := profile.Validate(); err != nil {
		return err
	}
	if !profile.usesNFS() {
		return fmt.Errorf("storage profile %q has no NFS server to configure", profile.ID)
	}
	contents := "# nntpbench storage profile: " + profile.ID + "\n" +
		"# nfs link: " + profile.NFSLinkID + "\n" +
		"# scope: " + profile.AttestationScope + "\n" +
		"NFS_LINK_BITS_PER_SECOND=" + strconv.FormatUint(profile.LinkBitsPerSecond, 10) + "\n" +
		"NFS_LINK_BURST_BYTES=" + strconv.FormatUint(profile.LinkBurstBytes, 10) + "\n" +
		"NFS_RTT_MICROS=" + strconv.FormatUint(profile.RTTMicros, 10) + "\n" +
		"NFS_EXPORT_OPTIONS=" + profile.ExportOptions + "\n"
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create storage link environment %s: %w", path, err)
	}
	defer file.Close()
	if _, err := file.WriteString(contents); err != nil {
		return fmt.Errorf("write storage link environment %s: %w", path, err)
	}
	return nil
}
