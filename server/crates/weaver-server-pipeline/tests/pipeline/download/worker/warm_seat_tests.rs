// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A pass that hands a lease to a server's last seat leaves that server
// nothing to warm; the warm goes to the next server with a free seat.
#[test]
fn a_lease_on_the_last_seat_leaves_no_warm_for_that_server() {
    let order = [0, 1];
    let headroom = HashMap::from([(0, 1), (1, 2)]);
    let sent = HashMap::from([(0, 1)]);
    let idle = HashMap::new();

    let seats = Pipeline::unclaimed_warm_seats(&order, &headroom, &sent, &idle, 8);

    assert_eq!(seats, vec![(1, 2)], "no warm is sent to server 0");
}

// A lease served by an idle connection used the seat that connection
// held, so the server's permits are still free to warm.
#[test]
fn idle_connections_hold_their_seats_until_a_lease_takes_them() {
    let order = [0];
    let headroom = HashMap::from([(0, 3)]);
    let idle = HashMap::from([(0, 1)]);

    assert_eq!(
        Pipeline::unclaimed_warm_seats(&order, &headroom, &HashMap::new(), &idle, 8),
        vec![(0, 2)]
    );
    assert_eq!(
        Pipeline::unclaimed_warm_seats(&order, &headroom, &HashMap::new(), &idle, 1),
        vec![(0, 1)],
        "never more than the job's share"
    );
}
