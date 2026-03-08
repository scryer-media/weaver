//! Sub-allocator for PPMd context model nodes.
//!
//! PPMd uses a custom memory allocator (sub-allocator) for its context trie.
//! The arena is split into two regions:
//! - **Text region** (lower ~1/8): stores raw decoded symbols for model building
//! - **Unit region** (upper ~7/8): stores context nodes and state arrays
//!
//! Within the unit region, two allocation stacks grow toward each other:
//! - `lo_unit` grows upward (for state arrays via `alloc_units`)
//! - `hi_unit` grows downward (for contexts via `alloc_context`)
//!
//! Reference: 7-zip Ppmd7.c (public domain), Shkarin's original PPMd code.

/// Size of one allocation unit in bytes.
pub const UNIT_SIZE: usize = 12;

/// Index-to-units mapping for the free list bins.
static INDEX_TO_UNITS: [u8; 38] = [
    1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128,
    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
];

/// Number of distinct free-list bin sizes.
const NUM_INDEXES: usize = 24;

/// A reference to an allocated unit block in the arena.
/// Stored as a unit index (byte offset = index * UNIT_SIZE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeRef(pub u32);

impl NodeRef {
    pub const NULL: NodeRef = NodeRef(0);

    #[inline]
    pub fn is_null(self) -> bool {
        self.0 == 0
    }

    /// Byte offset into the arena.
    #[inline]
    pub fn offset(self) -> usize {
        self.0 as usize * UNIT_SIZE
    }
}

/// A free-list node stored in-place in unused arena blocks.
#[derive(Clone, Copy)]
struct FreeNode {
    next: NodeRef,
}

/// Sub-allocator for PPMd.
pub struct SubAllocator {
    /// The memory arena.
    arena: Vec<u8>,
    /// Free lists indexed by bin size.
    free_lists: [NodeRef; NUM_INDEXES],

    // --- Text region (grows upward from byte UNIT_SIZE) ---
    /// Current text write position (byte offset in arena).
    p_text: usize,
    /// Byte offset where the unit allocation region starts.
    /// Text pointers (successor refs) are byte offsets < units_start_bytes.
    units_start_bytes: usize,

    // --- Unit allocation region ---
    /// Next available unit from the low end (unit index, grows up).
    lo_unit: u32,
    /// Next available unit from the high end (unit index, grows down).
    hi_unit: u32,
    /// Total number of units in the arena.
    total_units: u32,
}

impl SubAllocator {
    /// Create a new sub-allocator with the given arena size in bytes.
    pub fn new(arena_size: usize) -> Self {
        let arena_size = arena_size.max(UNIT_SIZE * 8); // minimum viable size
        let total_units = (arena_size / UNIT_SIZE) as u32;

        // Text region takes first ~1/8 of arena (after NULL unit).
        let text_units = (arena_size / 8 / UNIT_SIZE).max(1) + 1; // +1 for NULL unit
        let units_start = text_units as u32;
        let units_start_bytes = units_start as usize * UNIT_SIZE;

        let mut alloc = Self {
            arena: vec![0u8; arena_size],
            free_lists: [NodeRef::NULL; NUM_INDEXES],
            p_text: UNIT_SIZE, // start after NULL unit
            units_start_bytes,
            lo_unit: units_start,
            hi_unit: total_units,
            total_units,
        };
        alloc.arena.fill(0);
        alloc
    }

    /// Reset the allocator, freeing all allocations.
    pub fn reset(&mut self) {
        self.free_lists = [NodeRef::NULL; NUM_INDEXES];
        self.arena.fill(0);

        let text_units = (self.arena.len() / 8 / UNIT_SIZE).max(1) + 1;
        self.lo_unit = text_units as u32;
        self.units_start_bytes = text_units * UNIT_SIZE;
        self.hi_unit = self.total_units;
        self.p_text = UNIT_SIZE;
    }

    // ---- Text region methods ----

    /// Write a byte to the text region and advance the text pointer.
    /// Returns the byte offset where the byte was stored.
    pub fn write_text_byte(&mut self, b: u8) -> usize {
        let pos = self.p_text;
        if pos < self.units_start_bytes {
            self.arena[pos] = b;
            self.p_text += 1;
        }
        pos
    }

    /// Current text write position (byte offset in arena).
    #[inline]
    pub fn text_position(&self) -> usize {
        self.p_text
    }

    /// Decrement text position (undo a write).
    pub fn text_dec(&mut self) {
        if self.p_text > UNIT_SIZE {
            self.p_text -= 1;
        }
    }

    /// Check if the text region is exhausted.
    #[inline]
    pub fn text_exhausted(&self) -> bool {
        self.p_text >= self.units_start_bytes
    }

    /// The byte offset where the unit region starts.
    /// Successor values below this are text pointers.
    #[inline]
    pub fn units_start_bytes(&self) -> usize {
        self.units_start_bytes
    }

    // ---- Unit allocation ----

    /// Map a requested number of units to a free-list bin index.
    #[allow(clippy::needless_range_loop)]
    fn units_to_index(units: usize) -> usize {
        match units {
            1..=8 => units - 1,
            _ => {
                for i in 8..NUM_INDEXES {
                    if INDEX_TO_UNITS[i] as usize >= units {
                        return i;
                    }
                }
                NUM_INDEXES - 1
            }
        }
    }

    /// Allocate a block of `units` contiguous units from lo_unit or free lists.
    /// Used for state arrays.
    pub fn alloc_units(&mut self, units: usize) -> NodeRef {
        let idx = Self::units_to_index(units);
        let bin_units = INDEX_TO_UNITS[idx] as usize;

        // Try free list for exact or larger bin.
        #[allow(clippy::needless_range_loop)]
        for i in idx..NUM_INDEXES {
            if !self.free_lists[i].is_null() {
                let node = self.free_lists[i];
                let free_node = self.read_free_node(node);
                self.free_lists[i] = free_node.next;

                let found_units = INDEX_TO_UNITS[i] as usize;
                if found_units > bin_units {
                    let remainder_ref = NodeRef(node.0 + bin_units as u32);
                    let remainder_units = found_units - bin_units;
                    self.free_units(remainder_ref, remainder_units);
                }

                return node;
            }
        }

        // Try virgin region (lo_unit).
        if self.lo_unit + bin_units as u32 <= self.hi_unit {
            let node = NodeRef(self.lo_unit);
            self.lo_unit += bin_units as u32;
            return node;
        }

        NodeRef::NULL
    }

    /// Allocate exactly 1 unit from lo_unit or free lists.
    pub fn alloc_one(&mut self) -> NodeRef {
        if !self.free_lists[0].is_null() {
            let node = self.free_lists[0];
            let free_node = self.read_free_node(node);
            self.free_lists[0] = free_node.next;
            return node;
        }
        if self.lo_unit < self.hi_unit {
            let node = NodeRef(self.lo_unit);
            self.lo_unit += 1;
            return node;
        }
        // Try splitting from a larger bin.
        self.alloc_units(1)
    }

    /// Allocate a context node from hi_unit (growing downward).
    pub fn alloc_context(&mut self) -> NodeRef {
        if self.hi_unit > self.lo_unit {
            self.hi_unit -= 1;
            return NodeRef(self.hi_unit);
        }
        // Fallback to free list.
        if !self.free_lists[0].is_null() {
            let node = self.free_lists[0];
            let free_node = self.read_free_node(node);
            self.free_lists[0] = free_node.next;
            return node;
        }
        NodeRef::NULL
    }

    /// Expand a block from `old_nu` to `old_nu + 1` units.
    /// Returns the (possibly relocated) NodeRef, or NULL if out of memory.
    pub fn expand_units(&mut self, node: NodeRef, old_nu: usize) -> NodeRef {
        let old_idx = Self::units_to_index(old_nu);
        let new_idx = Self::units_to_index(old_nu + 1);
        if old_idx == new_idx {
            return node; // already fits in the same bin
        }
        let new_node = self.alloc_units(old_nu + 1);
        if new_node.is_null() {
            return NodeRef::NULL;
        }
        // Copy data.
        let src = node.offset();
        let dst = new_node.offset();
        let len = old_nu * UNIT_SIZE;
        self.arena.copy_within(src..src + len, dst);
        self.free_units(node, old_nu);
        new_node
    }

    /// Shrink a block from `old_nu` to `new_nu` units.
    /// Returns the (possibly relocated) NodeRef.
    pub fn shrink_units(&mut self, node: NodeRef, old_nu: usize, new_nu: usize) -> NodeRef {
        let old_idx = Self::units_to_index(old_nu);
        let new_idx = Self::units_to_index(new_nu);
        if old_idx == new_idx {
            return node;
        }
        let new_node = self.alloc_units(new_nu);
        if new_node.is_null() {
            return node; // keep old allocation
        }
        let src = node.offset();
        let dst = new_node.offset();
        let len = new_nu * UNIT_SIZE;
        self.arena.copy_within(src..src + len, dst);
        self.free_units(node, old_nu);
        new_node
    }

    /// Free a block of `units` contiguous units back to the appropriate free list.
    pub fn free_units(&mut self, node: NodeRef, units: usize) {
        let idx = Self::units_to_index(units);
        self.write_free_node(
            node,
            FreeNode {
                next: self.free_lists[idx],
            },
        );
        self.free_lists[idx] = node;
    }

    /// Free exactly 1 unit.
    pub fn free_one(&mut self, node: NodeRef) {
        self.write_free_node(
            node,
            FreeNode {
                next: self.free_lists[0],
            },
        );
        self.free_lists[0] = node;
    }

    // ---- Arena access via NodeRef (unit-aligned) ----

    /// Read a u8 at a specific byte offset within a node.
    #[inline]
    pub fn read_u8(&self, node: NodeRef, field_offset: usize) -> u8 {
        self.arena[node.offset() + field_offset]
    }

    /// Write a u8 at a specific byte offset within a node.
    #[inline]
    pub fn write_u8(&mut self, node: NodeRef, field_offset: usize, val: u8) {
        self.arena[node.offset() + field_offset] = val;
    }

    /// Read a u16 LE at a specific byte offset within a node.
    #[inline]
    pub fn read_u16(&self, node: NodeRef, field_offset: usize) -> u16 {
        let off = node.offset() + field_offset;
        u16::from_le_bytes([self.arena[off], self.arena[off + 1]])
    }

    /// Write a u16 LE at a specific byte offset within a node.
    #[inline]
    pub fn write_u16(&mut self, node: NodeRef, field_offset: usize, val: u16) {
        let off = node.offset() + field_offset;
        let bytes = val.to_le_bytes();
        self.arena[off] = bytes[0];
        self.arena[off + 1] = bytes[1];
    }

    /// Read a u32 LE at a specific byte offset within a node.
    #[inline]
    pub fn read_u32(&self, node: NodeRef, field_offset: usize) -> u32 {
        let off = node.offset() + field_offset;
        u32::from_le_bytes([
            self.arena[off],
            self.arena[off + 1],
            self.arena[off + 2],
            self.arena[off + 3],
        ])
    }

    /// Write a u32 LE at a specific byte offset within a node.
    #[inline]
    pub fn write_u32(&mut self, node: NodeRef, field_offset: usize, val: u32) {
        let off = node.offset() + field_offset;
        let bytes = val.to_le_bytes();
        self.arena[off..off + 4].copy_from_slice(&bytes);
    }

    // ---- Direct byte-offset arena access (for state records) ----

    /// Read a byte at an arbitrary byte offset in the arena.
    #[inline]
    pub fn read_byte_at(&self, off: usize) -> u8 {
        self.arena[off]
    }

    /// Write a byte at an arbitrary byte offset.
    #[inline]
    pub fn write_byte_at(&mut self, off: usize, val: u8) {
        self.arena[off] = val;
    }

    /// Read u16 LE at an arbitrary byte offset.
    #[inline]
    pub fn read_u16_at(&self, off: usize) -> u16 {
        u16::from_le_bytes([self.arena[off], self.arena[off + 1]])
    }

    /// Write u16 LE at an arbitrary byte offset.
    #[inline]
    pub fn write_u16_at(&mut self, off: usize, val: u16) {
        let bytes = val.to_le_bytes();
        self.arena[off] = bytes[0];
        self.arena[off + 1] = bytes[1];
    }

    /// Read u32 LE at an arbitrary byte offset.
    #[inline]
    pub fn read_u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes([
            self.arena[off],
            self.arena[off + 1],
            self.arena[off + 2],
            self.arena[off + 3],
        ])
    }

    /// Write u32 LE at an arbitrary byte offset.
    #[inline]
    pub fn write_u32_at(&mut self, off: usize, val: u32) {
        self.arena[off..off + 4].copy_from_slice(&val.to_le_bytes());
    }

    /// Copy bytes within the arena.
    pub fn copy_within(&mut self, src: usize, dst: usize, len: usize) {
        self.arena.copy_within(src..src + len, dst);
    }

    // ---- Internal ----

    fn read_free_node(&self, node: NodeRef) -> FreeNode {
        FreeNode {
            next: NodeRef(self.read_u32(node, 0)),
        }
    }

    fn write_free_node(&mut self, node: NodeRef, free_node: FreeNode) {
        self.write_u32(node, 0, free_node.next.0);
    }

    /// Check if the allocator has available memory.
    pub fn has_memory(&self) -> bool {
        self.lo_unit < self.hi_unit || self.free_lists.iter().any(|n| !n.is_null())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_one() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_one();
        assert!(!n.is_null());
    }

    #[test]
    fn test_alloc_free_reuse() {
        let mut alloc = SubAllocator::new(4096);
        let n1 = alloc.alloc_one();
        alloc.free_one(n1);
        let n2 = alloc.alloc_one();
        assert_eq!(n1, n2);
    }

    #[test]
    fn test_alloc_multiple_units() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_units(4);
        assert!(!n.is_null());
    }

    #[test]
    fn test_alloc_context() {
        let mut alloc = SubAllocator::new(4096);
        let c1 = alloc.alloc_context();
        let c2 = alloc.alloc_context();
        assert!(!c1.is_null());
        assert!(!c2.is_null());
        // Contexts are allocated from hi_unit (descending).
        assert!(c1.0 > c2.0);
    }

    #[test]
    fn test_read_write() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_one();
        alloc.write_u8(n, 0, 0xAA);
        alloc.write_u16(n, 2, 0xBBCC);
        alloc.write_u32(n, 4, 0xDDEEFF00);
        assert_eq!(alloc.read_u8(n, 0), 0xAA);
        assert_eq!(alloc.read_u16(n, 2), 0xBBCC);
        assert_eq!(alloc.read_u32(n, 4), 0xDDEEFF00);
    }

    #[test]
    fn test_text_region() {
        let mut alloc = SubAllocator::new(4096);
        let pos = alloc.write_text_byte(0x42);
        assert_eq!(pos, UNIT_SIZE); // first text byte after NULL unit
        assert_eq!(alloc.read_byte_at(pos), 0x42);
        assert_eq!(alloc.text_position(), UNIT_SIZE + 1);
    }

    #[test]
    fn test_expand_units() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_units(1);
        alloc.write_u32(n, 0, 0xDEADBEEF);
        let expanded = alloc.expand_units(n, 1);
        assert!(!expanded.is_null());
        // Data should be preserved.
        assert_eq!(alloc.read_u32(expanded, 0), 0xDEADBEEF);
    }

    #[test]
    fn test_shrink_units() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_units(4);
        alloc.write_u32(n, 0, 0xCAFEBABE);
        let shrunk = alloc.shrink_units(n, 4, 1);
        assert!(!shrunk.is_null());
        assert_eq!(alloc.read_u32(shrunk, 0), 0xCAFEBABE);
    }

    #[test]
    fn test_reset() {
        let mut alloc = SubAllocator::new(4096);
        let _n1 = alloc.alloc_one();
        let _n2 = alloc.alloc_one();
        alloc.reset();
        assert!(alloc.has_memory());
    }

    #[test]
    fn test_direct_byte_access() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_one();
        let off = n.offset();
        alloc.write_byte_at(off + 3, 0x77);
        assert_eq!(alloc.read_byte_at(off + 3), 0x77);
        alloc.write_u32_at(off + 4, 0x12345678);
        assert_eq!(alloc.read_u32_at(off + 4), 0x12345678);
    }
}
