use super::*;

// Selection is a namespace policy, made once per member declaration. The
// decoder keeps the same streaming contract regardless of its output sink.
// In particular, a physical leaf need not cross a queue or copy its payload
// merely because another member in the same archive feeds a child engine.
pub(crate) struct SelectOutput<A, B, F> {
    pub(crate) first: A,
    pub(crate) second: B,
    pub(crate) select_first: F,
}

pub(crate) enum SelectedMember<A, B> {
    First(A),
    Second(B),
}

impl<A: Write, B: Write> Write for SelectedMember<A, B> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::First(member) => member.write(bytes),
            Self::Second(member) => member.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::First(member) => member.flush(),
            Self::Second(member) => member.flush(),
        }
    }
}

impl<A: OutputMember, B: OutputMember> OutputMember for SelectedMember<A, B> {
    fn metadata(&self, metadata: Metadata) -> io::Result<()> {
        match self {
            Self::First(member) => member.metadata(metadata),
            Self::Second(member) => member.metadata(metadata),
        }
    }

    fn finish_data(self) -> io::Result<PendingOutput> {
        match self {
            Self::First(member) => member.finish_data(),
            Self::Second(member) => member.finish_data(),
        }
    }
}

impl<A, B, F> ArchiveOutput for SelectOutput<A, B, F>
where
    A: ArchiveOutput,
    B: ArchiveOutput,
    F: Fn(usize, &Path) -> bool + Sync,
{
    type Member = SelectedMember<A::Member, B::Member>;

    fn file(
        &self,
        occurrence: usize,
        path: &Path,
        length: Option<u64>,
    ) -> io::Result<Self::Member> {
        if (self.select_first)(occurrence, path) {
            self.first
                .file(occurrence, path, length)
                .map(SelectedMember::First)
        } else {
            self.second
                .file(occurrence, path, length)
                .map(SelectedMember::Second)
        }
    }

    fn directory(&self, occurrence: usize, path: &Path) -> io::Result<()> {
        if (self.select_first)(occurrence, path) {
            self.first.directory(occurrence, path)
        } else {
            self.second.directory(occurrence, path)
        }
    }

    fn directory_metadata(
        &self,
        occurrence: usize,
        path: &Path,
        metadata: Metadata,
    ) -> io::Result<()> {
        if (self.select_first)(occurrence, path) {
            self.first.directory_metadata(occurrence, path, metadata)
        } else {
            self.second.directory_metadata(occurrence, path, metadata)
        }
    }
}
