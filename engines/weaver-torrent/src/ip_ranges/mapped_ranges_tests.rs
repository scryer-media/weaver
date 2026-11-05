// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io::Cursor,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

use super::IpRanges;

fn has_both(list: &IpRanges, address: Ipv4Addr, expected: bool) {
    for representation in [IpAddr::V4(address), IpAddr::V6(address.to_ipv6_mapped())] {
        assert_eq!(list.has(representation), expected, "{representation}");
    }
}

#[test]
fn constructed_mapped_ranges_match_both_address_representations() {
    let list = IpRanges::new(
        [],
        std::iter::once(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped()..Ipv4Addr::new(127, 0, 0, 2).to_ipv6_mapped(),
        ),
    );
    has_both(&list, Ipv4Addr::LOCALHOST, true);
    has_both(&list, Ipv4Addr::new(127, 0, 0, 2), false);
    assert_eq!(list.len(), 1);
    assert!(!list.is_empty());
    assert!(IpRanges::default().is_empty());
}

#[tokio::test(start_paused = true)]
async fn parsed_mapped_ranges_match_both_address_representations() {
    let list = IpRanges::create_from_stream(&mut Cursor::new(
        b"mapped:::ffff:127.0.0.1-::ffff:127.0.0.1\n",
    ))
    .await
    .unwrap();
    has_both(&list, Ipv4Addr::LOCALHOST, true);
    has_both(&list, Ipv4Addr::new(127, 0, 0, 2), false);
    assert_eq!(list.len(), 1);
}

#[test]
fn union_keeps_explicit_mapped_ranges_separate_from_broad_ipv6_ranges() {
    let broad = IpRanges::new(
        [],
        std::iter::once(Ipv6Addr::UNSPECIFIED..Ipv6Addr::from_bits(u128::MAX)),
    );
    let mapped = IpRanges::new(
        [],
        std::iter::once(
            Ipv4Addr::LOCALHOST.to_ipv6_mapped()..Ipv4Addr::new(127, 0, 0, 2).to_ipv6_mapped(),
        ),
    );
    has_both(&broad, Ipv4Addr::LOCALHOST, false);
    assert!(broad.has(Ipv6Addr::LOCALHOST.into()));
    for union in [
        IpRanges::union([&broad, &mapped]),
        IpRanges::union([&mapped, &broad]),
    ] {
        has_both(&union, Ipv4Addr::LOCALHOST, true);
        has_both(&union, Ipv4Addr::new(127, 0, 0, 2), false);
        assert!(union.has(Ipv6Addr::LOCALHOST.into()));
        assert_eq!(union.len(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn the_mapped_range_upper_endpoint_does_not_wrap_to_ipv4_zero() {
    let after_last = Ipv6Addr::from_bits(Ipv4Addr::BROADCAST.to_ipv6_mapped().to_bits() + 1);
    let constructed = IpRanges::new(
        [],
        std::iter::once(Ipv4Addr::new(255, 255, 255, 254).to_ipv6_mapped()..after_last),
    );
    has_both(&constructed, Ipv4Addr::new(255, 255, 255, 254), true);
    has_both(&constructed, Ipv4Addr::BROADCAST, true);
    has_both(&constructed, Ipv4Addr::new(255, 255, 255, 253), false);
    has_both(&constructed, Ipv4Addr::UNSPECIFIED, false);
    let parsed = IpRanges::create_from_stream(&mut Cursor::new(
        b"last:::ffff:255.255.255.255-::ffff:255.255.255.255\n",
    ))
    .await
    .unwrap();
    has_both(&parsed, Ipv4Addr::BROADCAST, true);
    has_both(&parsed, Ipv4Addr::UNSPECIFIED, false);
    assert_eq!(parsed.len(), 1);
}
