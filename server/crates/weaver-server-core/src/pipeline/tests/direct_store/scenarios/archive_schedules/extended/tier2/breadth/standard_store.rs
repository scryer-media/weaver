// Ordinary stored posts across independent volume, payload and article sizes.
// The schedule pool adds loss, duplicates, restart, crash and demotion. Keep
// this opt-in: none of these campaign shards belongs in the default suite.
use super::*;

const CASES_PER_CELL: usize = 8_192;
const CELLS: usize = 270;
pub(super) const TOTAL: usize = CELLS * CASES_PER_CELL;
const _: () = assert!(TOTAL <= 5_000_000);

#[derive(Clone, Copy, Debug)]
pub(super) struct StandardStore {
    container: Container,
    volumes: usize,
    payload_len: usize,
    article: usize,
    recovery: Recovery,
}

pub(super) fn family() -> Family<StandardStore> {
    let mut units = Vec::new();
    for container in [Container::Rar4, Container::Rar5, Container::SevenZip] {
        for volumes in [1, 2, 3, 4, 8] {
            // Even the largest article has at least four data arrivals. Tiny
            // posts already have a separate family; empty schedule groups
            // here would inflate the advertised coverage with identical runs.
            for payload_len in [16_385, 32_769, 65_537] {
                for article in [127, 768, 4_096] {
                    for recovery in [Recovery::None, Recovery::Par2(Margin::With)] {
                        let rotation = units.len();
                        units.push((
                            StandardStore {
                                container,
                                volumes,
                                payload_len,
                                article,
                                recovery,
                            },
                            ExtractionProfile::DirectStore,
                            Pool::Combined,
                            CASES_PER_CELL,
                            rotation,
                        ));
                    }
                }
            }
        }
    }
    assert_eq!(units.len(), CELLS);
    let family = Family { units };
    assert_eq!(family.total(), TOTAL);
    family
}

impl Cell for StandardStore {
    fn requires_direct(self, interruption: Interruption) -> bool {
        matches!(interruption, Interruption::None)
    }

    fn post(self, interruption: Interruption) -> Built {
        let bytes = payload(89, self.payload_len);
        let volumes = stored(self.container, MEMBER, &bytes, self.volumes);
        let post = data_post(volumes, self.article, vec![(MEMBER.to_string(), bytes)]);
        let (post, geometry) = with(
            post,
            self.recovery,
            interruption,
            Par2Volumes::Uniform,
            self.article,
            |set| set,
        );
        Built {
            post,
            geometry,
            ruling: None,
        }
    }

    fn par2(self) -> bool {
        matches!(self.recovery, Recovery::Par2(_))
    }
}

#[test]
fn standard_store_sampling_varies_and_covers_the_pool() {
    let profile = ExtractionProfile::DirectStore;
    let pool = Pool::Combined;
    let selected = |rotation| {
        pool.sampled(profile, CASES_PER_CELL, rotation)
            .into_iter()
            .map(|(case, _)| case)
            .collect::<BTreeSet<_>>()
    };
    let first = selected(0);
    assert_eq!(first.len(), CASES_PER_CELL);
    assert_ne!(first, selected(1));
    let union: BTreeSet<_> = (0..CELLS).flat_map(selected).collect();
    assert_eq!(union.len(), pool.count(profile, usize::MAX));
}

// At most 1,006 cases per shard; replay uses the shared family case index.
mod campaign {
    use super::*;

    tier2_shards!(family, 2200;
        h00 0 h01 1 h02 2 h03 3 h04 4 h05 5 h06 6 h07 7 h08 8 h09 9
        h10 10 h11 11 h12 12 h13 13 h14 14 h15 15 h16 16 h17 17 h18 18 h19 19
        h20 20 h21 21
    );
}
