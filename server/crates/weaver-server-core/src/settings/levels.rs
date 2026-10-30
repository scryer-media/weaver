// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! One setting at the levels a torrent takes it from: the torrent itself,
//! its tracker rule, its category and the global settings. Each level holds
//! the setting as a [`Leaf`], and the first level that does not inherit
//! decides it.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A setting at one level: inherited from the level above, switched off
/// here (no goal, no cap), or set here.
///
/// Kept as JSON as `null` (or absent), `"off"`, or the value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Leaf<T> {
    #[default]
    Inherit,
    Off,
    Set(T),
}

impl<T> Leaf<T> {
    pub const fn is_inherit(&self) -> bool {
        matches!(self, Self::Inherit)
    }

    /// A setting that is set or inherited, never off: a switch, whose off is
    /// a value of its own.
    pub fn of(value: Option<T>) -> Self {
        value.map_or(Self::Inherit, Self::Set)
    }

    /// A limit at the global level, where nothing is inherited: `None` is
    /// no limit.
    pub fn limit(value: Option<T>) -> Self {
        value.map_or(Self::Off, Self::Set)
    }

    /// The value while it is set here.
    pub fn set(self) -> Option<T> {
        match self {
            Self::Set(value) => Some(value),
            Self::Inherit | Self::Off => None,
        }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Leaf<U> {
        match self {
            Self::Inherit => Leaf::Inherit,
            Self::Off => Leaf::Off,
            Self::Set(value) => Leaf::Set(f(value)),
        }
    }

    /// The leaf at the global level, where there is no level above:
    /// inheriting is off.
    pub fn at_global(self) -> Self {
        match self {
            Self::Inherit => Self::Off,
            leaf => leaf,
        }
    }
}

const OFF: &str = "off";

impl Leaf<u64> {
    /// As a column keeps it: `NULL` to inherit, `off`, or the number.
    pub fn to_text(self) -> Option<String> {
        match self {
            Self::Inherit => None,
            Self::Off => Some(OFF.to_owned()),
            Self::Set(value) => Some(value.to_string()),
        }
    }

    pub fn from_text(text: Option<&str>) -> Option<Self> {
        match text {
            None => Some(Self::Inherit),
            Some(OFF) => Some(Self::Off),
            Some(value) => value.parse().ok().map(Self::Set),
        }
    }
}

impl<T: Serialize> Serialize for Leaf<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Inherit => serializer.serialize_none(),
            Self::Off => serializer.serialize_str(OFF),
            Self::Set(value) => value.serialize(serializer),
        }
    }
}

// The word `off`, and nothing else.
struct OffWord;

impl<'de> Deserialize<'de> for OffWord {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let word = String::deserialize(deserializer)?;
        if word == OFF {
            Ok(Self)
        } else {
            Err(serde::de::Error::custom("not off"))
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Leaf<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Kept<T> {
            Off(OffWord),
            Set(T),
        }
        Ok(match Option::<Kept<T>>::deserialize(deserializer)? {
            None => Self::Inherit,
            Some(Kept::Off(_)) => Self::Off,
            Some(Kept::Set(value)) => Self::Set(value),
        })
    }
}

/// A level a torrent takes a setting from, the most specific first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Torrent,
    TrackerRule,
    Category,
    Global,
}

/// One setting at every level, for one torrent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Levels<T> {
    pub torrent: Leaf<T>,
    pub rule: Leaf<T>,
    pub category: Leaf<T>,
    pub global: Leaf<T>,
}

impl<T: Clone> Levels<T> {
    // The first level that does not inherit, and what it holds.
    fn decided(&self) -> Option<(Level, &Leaf<T>)> {
        [
            (Level::Torrent, &self.torrent),
            (Level::TrackerRule, &self.rule),
            (Level::Category, &self.category),
            (Level::Global, &self.global),
        ]
        .into_iter()
        .find(|(_, leaf)| !leaf.is_inherit())
    }

    /// The setting in force: the value of the first level that does not
    /// inherit, `None` when that level switches it off or every level
    /// inherits.
    pub fn resolve(&self) -> Option<T> {
        self.decided().and_then(|(_, leaf)| leaf.clone().set())
    }

    /// The level that decides the setting; the global level when every one
    /// inherits.
    pub fn level(&self) -> Level {
        self.decided().map_or(Level::Global, |(level, _)| level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn levels(
        torrent: Leaf<u32>,
        rule: Leaf<u32>,
        category: Leaf<u32>,
        global: Leaf<u32>,
    ) -> Levels<u32> {
        Levels {
            torrent,
            rule,
            category,
            global,
        }
    }

    #[test]
    fn the_first_level_that_does_not_inherit_decides() {
        use Leaf::{Inherit, Off, Set};
        let cases = [
            (
                levels(Inherit, Inherit, Inherit, Inherit),
                None,
                Level::Global,
            ),
            (
                levels(Inherit, Inherit, Inherit, Set(4)),
                Some(4),
                Level::Global,
            ),
            (
                levels(Inherit, Inherit, Set(3), Set(4)),
                Some(3),
                Level::Category,
            ),
            (
                levels(Inherit, Set(2), Set(3), Set(4)),
                Some(2),
                Level::TrackerRule,
            ),
            (
                levels(Set(1), Set(2), Set(3), Set(4)),
                Some(1),
                Level::Torrent,
            ),
            // Off drops what the levels above set.
            (levels(Off, Set(2), Set(3), Set(4)), None, Level::Torrent),
            (levels(Inherit, Inherit, Off, Set(4)), None, Level::Category),
            // Below an off, a level sets it again.
            (levels(Set(1), Off, Off, Off), Some(1), Level::Torrent),
        ];
        for (levels, value, level) in cases {
            assert_eq!(levels.resolve(), value, "{levels:?}");
            assert_eq!(levels.level(), level, "{levels:?}");
        }
    }

    // Over every mix of leaves, the level reported is the one whose leaf
    // gives the value resolved.
    #[test]
    fn the_level_reported_is_the_one_that_gives_the_value() {
        let leaves = |n: u32| [Leaf::Inherit, Leaf::Off, Leaf::Set(n)];
        for torrent in leaves(1) {
            for rule in leaves(2) {
                for category in leaves(3) {
                    for global in leaves(4) {
                        let levels = levels(torrent, rule, category, global);
                        let at = match levels.level() {
                            Level::Torrent => torrent,
                            Level::TrackerRule => rule,
                            Level::Category => category,
                            Level::Global => global,
                        };
                        assert_eq!(levels.resolve(), at.set(), "{levels:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn a_leaf_is_kept_as_null_off_or_its_value() {
        for (leaf, json) in [
            (Leaf::Inherit, "null"),
            (Leaf::Off, "\"off\""),
            (Leaf::Set(2.5_f32), "2.5"),
        ] {
            assert_eq!(serde_json::to_string(&leaf).unwrap(), json);
            assert_eq!(serde_json::from_str::<Leaf<f32>>(json).unwrap(), leaf);
        }
        assert!(serde_json::from_str::<Leaf<f32>>("\"on\"").is_err());
        assert_eq!(
            serde_json::from_str::<Leaf<String>>("\"any\"").unwrap(),
            Leaf::Set("any".to_owned())
        );
    }
}
