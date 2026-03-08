//! Sub-allocator for PPMd context model nodes.
//!
//! PPMd uses a custom memory allocator (sub-allocator) for its context trie.
//! Nodes are allocated from a contiguous arena. When memory is exhausted, the
//! model is reset and rebuilt.
//!
//! This implementation uses a Vec<u8> arena with a free-list of fixed-size units.
//! Each "unit" is 12 bytes (the size of a PPMd context or state record).
//!
//! Reference: 7-zip Ppmd7.c (public domain), Shkarin's original PPMd code.

/// Size of one allocation unit in bytes.
pub const UNIT_SIZE: usize = 12;

/// Index-to-units mapping for the free list bins.
/// Bin `i` holds blocks of `INDEX_TO_UNITS[i]` units.
static INDEX_TO_UNITS: [u8; 38] = [
    1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, 128,
    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
];

/// Number of distinct free-list bin sizes.
const NUM_INDEXES: usize = 24;

/// A reference to an allocated block in the arena.
/// Uses a u32 offset from the start of the arena (byte offset / UNIT_SIZE).
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
    /// High-water mark: next unit index to allocate from the virgin region.
    hi_unit: u32,
    /// Low-water mark: start of the virgin (unallocated) region.
    lo_unit: u32,
    /// Total number of units in the arena.
    total_units: u32,
}

impl SubAllocator {
    /// Create a new sub-allocator with the given arena size in bytes.
    pub fn new(arena_size: usize) -> Self {
        let total_units = (arena_size / UNIT_SIZE) as u32;
        // Reserve unit 0 as the NULL sentinel.
        let arena = vec![0u8; arena_size.max(UNIT_SIZE)];

        Self {
            arena,
            free_lists: [NodeRef::NULL; NUM_INDEXES],
            hi_unit: total_units,
            lo_unit: 1, // unit 0 is NULL
            total_units,
        }
    }

    /// Reset the allocator, freeing all allocations.
    pub fn reset(&mut self) {
        self.free_lists = [NodeRef::NULL; NUM_INDEXES];
        self.arena.fill(0);
        self.hi_unit = self.total_units;
        self.lo_unit = 1;
    }

    /// Map a requested number of units to a free-list bin index.
    fn units_to_index(units: usize) -> usize {
        match units {
            1..=8 => units - 1,
            _ => {
                // For larger blocks, find the bin with the smallest size >= units
                for i in 8..NUM_INDEXES {
                    if INDEX_TO_UNITS[i] as usize >= units {
                        return i;
                    }
                }
                NUM_INDEXES - 1
            }
        }
    }

    /// Allocate a block of `units` contiguous units.
    /// Returns a NodeRef to the first unit, or NodeRef::NULL if out of memory.
    pub fn alloc_units(&mut self, units: usize) -> NodeRef {
        let idx = Self::units_to_index(units);
        let bin_units = INDEX_TO_UNITS[idx] as usize;

        // Try free list for exact or larger bin.
        for i in idx..NUM_INDEXES {
            if !self.free_lists[i].is_null() {
                let node = self.free_lists[i];
                // Remove from free list.
                let free_node = self.read_free_node(node);
                self.free_lists[i] = free_node.next;

                // If the bin is larger than needed, put remainder back.
                let found_units = INDEX_TO_UNITS[i] as usize;
                if found_units > bin_units {
                    let remainder_ref = NodeRef(node.0 + bin_units as u32);
                    let remainder_units = found_units - bin_units;
                    self.free_units(remainder_ref, remainder_units);
                }

                return node;
            }
        }

        // Try virgin region.
        if self.lo_unit + bin_units as u32 <= self.hi_unit {
            let node = NodeRef(self.lo_unit);
            self.lo_unit += bin_units as u32;
            return node;
        }

        NodeRef::NULL
    }

    /// Allocate exactly 1 unit. Optimized fast path.
    pub fn alloc_one(&mut self) -> NodeRef {
        // Try free list bin 0 first.
        if !self.free_lists[0].is_null() {
            let node = self.free_lists[0];
            let free_node = self.read_free_node(node);
            self.free_lists[0] = free_node.next;
            return node;
        }

        // Try virgin region.
        if self.lo_unit < self.hi_unit {
            let node = NodeRef(self.lo_unit);
            self.lo_unit += 1;
            return node;
        }

        // Try splitting from a larger bin.
        self.alloc_units(1)
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

    /// Read a 12-byte block from the arena at the given node reference.
    pub fn read_bytes(&self, node: NodeRef) -> &[u8] {
        let off = node.offset();
        &self.arena[off..off + UNIT_SIZE]
    }

    /// Write bytes to a node location (up to UNIT_SIZE bytes).
    pub fn write_bytes(&mut self, node: NodeRef, data: &[u8]) {
        let off = node.offset();
        let len = data.len().min(UNIT_SIZE);
        self.arena[off..off + len].copy_from_slice(&data[..len]);
    }

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

    /// Read a variable-length byte slice from the arena.
    /// Useful for reading state arrays that span multiple units.
    pub fn read_slice(&self, node: NodeRef, len: usize) -> &[u8] {
        let off = node.offset();
        &self.arena[off..off + len]
    }

    /// Write a variable-length byte slice to the arena.
    pub fn write_slice(&mut self, node: NodeRef, data: &[u8]) {
        let off = node.offset();
        self.arena[off..off + data.len()].copy_from_slice(data);
    }

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
        let mut alloc = SubAllocator::new(1024);
        let n = alloc.alloc_one();
        assert!(!n.is_null());
        assert_eq!(n.0, 1); // unit 0 is reserved
    }

    #[test]
    fn test_alloc_free_reuse() {
        let mut alloc = SubAllocator::new(1024);
        let n1 = alloc.alloc_one();
        alloc.free_one(n1);
        let n2 = alloc.alloc_one();
        assert_eq!(n1, n2); // should reuse freed unit
    }

    #[test]
    fn test_alloc_multiple_units() {
        let mut alloc = SubAllocator::new(4096);
        let n = alloc.alloc_units(4);
        assert!(!n.is_null());
    }

    #[test]
    fn test_read_write() {
        let mut alloc = SubAllocator::new(1024);
        let n = alloc.alloc_one();
        alloc.write_u8(n, 0, 0xAA);
        alloc.write_u16(n, 2, 0xBBCC);
        alloc.write_u32(n, 4, 0xDDEEFF00);
        assert_eq!(alloc.read_u8(n, 0), 0xAA);
        assert_eq!(alloc.read_u16(n, 2), 0xBBCC);
        assert_eq!(alloc.read_u32(n, 4), 0xDDEEFF00);
    }

    #[test]
    fn test_exhaustion() {
        // Small arena: 3 units total (including NULL unit 0)
        let mut alloc = SubAllocator::new(UNIT_SIZE * 3);
        let n1 = alloc.alloc_one();
        assert!(!n1.is_null());
        let n2 = alloc.alloc_one();
        assert!(!n2.is_null());
        // Arena should be exhausted now
        let n3 = alloc.alloc_one();
        assert!(n3.is_null());
    }

    #[test]
    fn test_reset() {
        let mut alloc = SubAllocator::new(1024);
        let _n1 = alloc.alloc_one();
        let _n2 = alloc.alloc_one();
        alloc.reset();
        // After reset, should allocate from the beginning again
        let n = alloc.alloc_one();
        assert_eq!(n.0, 1);
    }
}
