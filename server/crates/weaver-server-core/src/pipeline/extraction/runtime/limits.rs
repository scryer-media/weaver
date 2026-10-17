// The fixed resource shape of one decoded archive namespace. Every admission
// of a namespace, live or restored, reserves from the job's budget in these
// units, so a restart rebuilds exactly what the original run held.

use crate::pipeline::direct_store::router::RouterResources;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

// Archives nest at most this deep, the posted set included, which bounds the
// session tree, the routing graph and the extraction recursion alike. Deeper
// than anything posted in earnest, so the limit only ever stops a tower built
// to exhaust it.
pub(crate) const MAX_DEPTH: u32 = 11;

// Routed bytes a namespace may hold while it waits for a member boundary.
pub(crate) const ROUTER_HOLDS_BUDGET: u64 = 4 * MIB;

// Scratch a namespace's router may spill held bytes into.
pub(crate) const ROUTER_SCRATCH_CEILING: u64 = 8 * MIB;

// Bytes a router may copy when a member migrates between destinations.
pub(crate) const ROUTER_MIGRATION_CEILING: u64 = 8 * MIB;

// Frames in flight between a decoder and its namespace bridge; the decoder
// blocks once this many are queued, which bounds the bridge's memory.
pub(crate) const FRAME_CHANNEL_DEPTH: usize = 8;

// Memory the bridge reserves for its queued frames before the decoder starts.
pub(crate) const BRIDGE_ADMISSION_BYTES: u64 = 512 * KIB;

// Memory the member catalog and the destination registry each reserve.
pub(crate) const CATALOG_ADMISSION_BYTES: u64 = 4 * MIB;

// One copy buffer for materializing or re-reading a member, reserved for
// the length of the copy.
pub(crate) const MATERIALIZATION_PERMIT_BYTES: u64 = 64 * KIB;

// Memory a restored namespace's generation reserves. A restored generation
// never carries a frame, so it holds only a token amount.
pub(crate) const RESTORED_GENERATION_ADMISSION_BYTES: u64 = KIB;

// The routing resources every decoded namespace runs with.
pub(crate) fn namespace_router_resources() -> RouterResources {
    RouterResources {
        accountant: Default::default(),
        holds_budget: ROUTER_HOLDS_BUDGET,
        scratch_ceiling: ROUTER_SCRATCH_CEILING,
        sparse: Default::default(),
        migration_ceiling_bytes: ROUTER_MIGRATION_CEILING,
    }
}
