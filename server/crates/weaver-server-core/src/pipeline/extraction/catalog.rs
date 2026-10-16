pub(crate) mod decoded;

pub(crate) const MAX_CATALOG_MEMBERS: usize = 65_536;

// Catalog facts describe an output namespace, independently of the engine
// producing it. Payload ownership and placement evidence live elsewhere.
pub(crate) struct MemberFact<'a> {
    pub(crate) id: u32,
    pub(crate) name: &'a str,
    pub(crate) routable: bool,
}

pub(crate) trait MemberCatalog {
    fn revision(&self) -> u64;
    fn members(&self) -> Vec<MemberFact<'_>>;
    fn length(&self, member: u32) -> Option<u64>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InputRange {
    pub(crate) volume: u32,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    // Offset within the completed physical write, preserved across splits.
    pub(crate) write_offset: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MemberRange {
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) write_offset: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InputProjection {
    Mapped(Vec<InputRange>),
    // A successful stop: decoded output says nothing about original
    // compressed input coverage. That input has its own retained backing.
    NotReversible,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProjectionError {
    InvalidRange,
    Unmapped,
    Overlap,
}

pub(crate) trait MemberProjection: MemberCatalog {
    fn project_member(
        &self,
        member: u32,
        range: MemberRange,
    ) -> Result<InputProjection, ProjectionError>;
}
