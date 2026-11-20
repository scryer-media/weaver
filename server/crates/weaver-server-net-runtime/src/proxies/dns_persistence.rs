// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The saved DNS server lists and profiles, and the checks a save of either,
// or of a route, makes of the routes that use them.
//
// Both tables are keyed by id and hold at most `MAX_DNS_LISTS` and
// `MAX_DNS_PROFILES` rows, which the operator adds and deletes; nothing
// else grows them. A route names its profile in `proxy_routes.dns_profile_id`,
// indexed, and a profile its list in `dns_profiles.server_list_id`, indexed.
use std::collections::HashMap;

use super::network_persistence::{lock_network, stored_route};
use super::{
    Consumer, DnsProfile, DnsProfileKind, DnsServerList, DnsTrust, DnsWho, LegPath, NetworkRefusal,
    ProxyKind, ProxyPool, ProxyProfile, Route, Rung, SYSTEM_DNS_PROFILE_ID,
};
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database as SqlDatabase;
use weaver_server_db::persistence::sql_runtime::{
    SqlArg, SqlConn, SqlRow, SqlRuntime, SqlTx, max_rows_for_engine,
};
use weaver_tunnel::pipe::DnsChoice;

fn error(value: impl std::fmt::Display) -> StateError {
    StateError::Database(value.to_string())
}

/// A stored server list whose servers cannot be read: its id, and why.
pub type UnreadableDnsList = (u32, String);

/// Every saved list and profile as the routing configuration loads them.
#[derive(Debug, Clone, Default)]
pub struct StoredDns {
    pub lists: HashMap<u32, DnsServerList>,
    // Lists whose stored servers cannot be read. A profile that names one
    // resolves nothing, and the routes using it say why.
    pub unreadable: HashMap<u32, String>,
    pub profiles: HashMap<u32, DnsProfile>,
}

impl StoredDns {
    // The profile with `id`, the built-in System profile included.
    pub fn profile(&self, id: u32) -> Option<DnsProfile> {
        if id == SYSTEM_DNS_PROFILE_ID {
            return Some(DnsProfile::system());
        }
        self.profiles.get(&id).cloned()
    }
    // Who answers the names a route on profile `id` dials, or None when the
    // profile does not exist. A profile whose server list cannot be read
    // answers nothing.
    pub fn choice(&self, id: u32) -> Option<DnsChoice> {
        let profile = self.profile(id)?;
        if profile.who == DnsWho::Weaver
            && let Some(list) = profile.server_list_id
            && let Some(fault) = self.unreadable.get(&list)
        {
            return Some(DnsChoice::Refused(format!(
                "the stored servers of DNS server list {list} cannot be read ({fault})"
            )));
        }
        Some(profile.choice(&self.lists))
    }
}

/// What a save changes, so the route checks refuse it only for faults that
/// involve what it touched: a fault already stored among routes the save
/// leaves alone does not hold it back. A restore touches everything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Touched {
    All,
    Route(Consumer),
    Profile(u32),
    List(u32),
    Proxy(u32),
    Pool(u32),
}

impl Touched {
    // Whether `route`, held by `holder` on `profile`, is one this save
    // touches: the route saved, a route on the profile or on a profile of
    // the list saved, or a route riding the proxy or pool saved.
    fn holds(
        self,
        holder: Consumer,
        route: &Route,
        profile: &DnsProfile,
        pools: &HashMap<u32, ProxyPool>,
    ) -> bool {
        let rides = |hit: &dyn Fn(&Rung) -> bool| {
            route.legs.iter().any(|leg| match &leg.path {
                LegPath::Direct => false,
                LegPath::Ladder { rungs, .. } => rungs.iter().any(hit),
            })
        };
        match self {
            Self::All => true,
            Self::Route(consumer) => holder == consumer,
            Self::Profile(id) => profile.id == id,
            Self::List(id) => profile.server_list_id == Some(id),
            Self::Proxy(id) => rides(&|rung| match rung {
                Rung::Proxy { id: proxy } => *proxy == id,
                Rung::Chain { ids } => ids.contains(&id),
                Rung::Pool { id: pool } => pools
                    .get(pool)
                    .is_some_and(|pool| pool.member_ids.contains(&id)),
            }),
            Self::Pool(id) => rides(&|rung| matches!(rung, Rung::Pool { id: pool } if *pool == id)),
        }
    }
}

fn list_row(row: &SqlRow) -> Result<(DnsServerList, Option<String>), StateError> {
    let id = u32::try_from(row.i64("id")?).map_err(error)?;
    let name = row.text("name")?;
    let (servers, fault) = match super::read_servers(&row.text("servers")?) {
        Ok(servers) => (servers, None),
        Err(fault) => (Vec::new(), Some(fault)),
    };
    Ok((DnsServerList { id, name, servers }, fault))
}

fn profile_row(row: &SqlRow) -> Result<DnsProfile, StateError> {
    let id = u32::try_from(row.i64("id")?).map_err(error)?;
    let text = |column: &str| row.text(column);
    let kind =
        super::kind_of(&text("kind")?).ok_or_else(|| error("unknown stored DNS profile kind"))?;
    let who = super::who_of(&text("who")?).ok_or_else(|| error("unknown stored DNS answerer"))?;
    let trust =
        super::trust_of(&text("trust")?).ok_or_else(|| error("unknown stored DNS trust"))?;
    let transit = row
        .opt_text("transit")?
        .map(|transit| {
            super::transit_of(&transit).ok_or_else(|| error("unknown stored DNS transit"))
        })
        .transpose()?;
    let server_list_id = row
        .opt_i64("server_list_id")?
        .map(|id| u32::try_from(id).map_err(error))
        .transpose()?;
    Ok(DnsProfile {
        id,
        name: text("name")?,
        kind,
        who,
        transit,
        trust,
        server_list_id,
        system_fallback: row.bool("system_fallback")?,
    })
}

// A stored proxy row's profile, with the DNS profile from its own column.
pub(super) fn stored_proxy(row: &SqlRow) -> Result<ProxyProfile, StateError> {
    let mut profile: ProxyProfile = serde_json::from_str(&row.text("config")?).map_err(error)?;
    profile.id = u32::try_from(row.i64("id")?).map_err(error)?;
    profile.dns_profile_id = u32::try_from(row.i64("dns_profile_id")?).map_err(error)?;
    Ok(profile)
}

async fn load(tx: &mut SqlConn<'_>) -> Result<StoredDns, StateError> {
    let mut stored = StoredDns::default();
    for row in tx
        .fetch_all("SELECT id, name, servers FROM dns_server_lists", &[])
        .await?
    {
        let (list, fault) = list_row(&row)?;
        if let Some(fault) = fault {
            stored.unreadable.insert(list.id, fault);
        }
        stored.lists.insert(list.id, list);
    }
    for row in tx
        .fetch_all(
            "SELECT id, name, kind, who, transit, trust, server_list_id, system_fallback FROM dns_profiles",
            &[],
        )
        .await?
    {
        let profile = profile_row(&row)?;
        stored.profiles.insert(profile.id, profile);
    }
    Ok(stored)
}

// Every stored route with its holder and DNS profile.
async fn routes_with_dns(tx: &mut SqlConn<'_>) -> Result<Vec<(Consumer, Route, u32)>, StateError> {
    tx.fetch_all(
        "SELECT consumer, policy, dns_profile_id FROM proxy_routes ORDER BY consumer",
        &[],
    )
    .await?
    .into_iter()
    .filter_map(|row| {
        let read = || {
            let key = row.text("consumer")?;
            let Some(holder) = Consumer::from_key(&key) else {
                return Ok(None);
            };
            let profile = u32::try_from(row.i64("dns_profile_id")?).map_err(error)?;
            Ok(Some((holder, stored_route(&row.text("policy")?)?, profile)))
        };
        read().transpose()
    })
    .collect()
}

// Whether a leg or rung of `route` has no proxy: a direct leg, or a ladder
// that falls back to direct.
fn has_proxyless_stage(route: &Route) -> bool {
    route.legs.iter().any(|leg| match &leg.path {
        LegPath::Direct => true,
        LegPath::Ladder {
            direct_fallback, ..
        } => *direct_fallback,
    })
}

// Whether a stage of `route` answers other than at a WireGuard hop: a
// direct leg, a ladder that falls back to direct, or a rung that ends at a
// hop that is not WireGuard.
fn reaches_other_than_wireguard(
    route: &Route,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
) -> bool {
    let kind = |id: &u32| profiles.get(id).map(|profile| profile.kind);
    route.legs.iter().any(|leg| match &leg.path {
        LegPath::Direct => true,
        LegPath::Ladder {
            rungs,
            direct_fallback,
        } => {
            *direct_fallback
                || rungs.iter().any(|rung| {
                    let last = match rung {
                        Rung::Proxy { id } => kind(id),
                        Rung::Chain { ids } => ids.last().and_then(kind),
                        Rung::Pool { id } => pools.get(id).map(|pool| pool.kind),
                    };
                    last != Some(ProxyKind::WireGuard)
                })
        }
    })
}

// The WireGuard proxies `route` ends a rung at, a pool's members among
// them, that list no DNS servers of their tunnel: with the proxy answering,
// nothing would answer there.
// Each WireGuard proxy with no tunnel DNS that carries, in a chain of
// `route`, a proxy named by a host name whose own profile is the host's: an
// inner hop's address on that profile is resolved by the hop carrying it, and
// such a tunnel has no one to ask. Each with the carried proxy's profile.
fn carriers_without_dns(
    route: &Route,
    profiles: &HashMap<u32, ProxyProfile>,
    stored: &StoredDns,
) -> Vec<(u32, u32)> {
    let mut silent = Vec::new();
    for leg in &route.legs {
        let LegPath::Ladder { rungs, .. } = &leg.path else {
            continue;
        };
        for rung in rungs {
            let Rung::Chain { ids } = rung else {
                continue;
            };
            for pair in ids.windows(2) {
                let (Some(carrier), Some(carried)) =
                    (profiles.get(&pair[0]), profiles.get(&pair[1]))
                else {
                    continue;
                };
                let host = carried.host.trim().trim_matches(['[', ']']);
                let named = !host.is_empty() && host.parse::<std::net::IpAddr>().is_err();
                let on_host = stored
                    .profile(carried.dns_profile_id)
                    .is_some_and(|profile| profile.who == DnsWho::System);
                if carrier.kind == ProxyKind::WireGuard
                    && carrier.dns_servers.is_empty()
                    && named
                    && on_host
                {
                    silent.push((carrier.id, carried.dns_profile_id));
                }
            }
        }
    }
    silent
}

fn wireguard_without_dns(
    route: &Route,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
) -> Vec<u32> {
    let mut silent = Vec::new();
    for leg in &route.legs {
        let LegPath::Ladder { rungs, .. } = &leg.path else {
            continue;
        };
        for rung in rungs {
            let exits: Vec<u32> = match rung {
                Rung::Proxy { id } => vec![*id],
                Rung::Chain { ids } => ids.last().copied().into_iter().collect(),
                Rung::Pool { id } => pools
                    .get(id)
                    .map(|pool| pool.member_ids.clone())
                    .unwrap_or_default(),
            };
            for id in exits {
                if profiles.get(&id).is_some_and(|profile| {
                    profile.kind == ProxyKind::WireGuard && profile.dns_servers.is_empty()
                }) && !silent.contains(&id)
                {
                    silent.push(id);
                }
            }
        }
    }
    silent
}

// Refuse a stored state in which a route names a profile that does not
// exist, or one that cannot answer on it: the proxy answering on a route
// with a stage that has no proxy, unless the profile lets the host answer
// there; validating the proxy's answers on a route with any stage that does
// not end at WireGuard; the proxy answering at a WireGuard proxy that lists
// no DNS servers; a route on a profile that is not a route profile; or a
// proxy whose address is found by a profile that is not a proxy name
// profile. Only a fault that involves a route or proxy `touched` holds is
// refused, and each refusal names every such route at fault.
pub(super) async fn validate_route_dns(
    tx: &mut SqlConn<'_>,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
    touched: Touched,
) -> Result<(), StateError> {
    let stored = load(tx).await?;
    if let Some(list) = stored
        .profiles
        .values()
        .filter_map(|profile| profile.server_list_id)
        .find(|list| !stored.lists.contains_key(list))
    {
        return Err(StateError::Database(format!(
            "a DNS profile names server list {list}, which does not exist"
        )));
    }
    for proxy in profiles.values() {
        if !matches!(touched, Touched::All) && touched != Touched::Proxy(proxy.id) {
            continue;
        }
        let Some(profile) = stored.profile(proxy.dns_profile_id) else {
            return Err(NetworkRefusal::DnsProfileNotFound.into());
        };
        profile.select_as(DnsProfileKind::ProxyName)?;
    }
    let routes = routes_with_dns(tx).await?;
    let mut faults = RouteDnsFaults::default();
    for (holder, route, id) in &routes {
        let Some(profile) = stored.profile(*id) else {
            return Err(NetworkRefusal::DnsProfileNotFound.into());
        };
        let touches = touched.holds(*holder, route, &profile, pools);
        if touches {
            profile.select_as(DnsProfileKind::Route)?;
        }
        faults.carriers(Some(*holder), route, profiles, &stored, |carried| {
            touches || touched == Touched::Profile(carried)
        });
        if touches {
            faults.route(Some(*holder), route, &profile, profiles, pools);
        }
    }
    faults.refusal().map_err(Into::into)
}

// Refuse drafted `route` on DNS `profile` as a save of it would be refused,
// before anything is dialled through it. A draft has no holder, so each
// refusal names no route.
pub fn validate_draft_route_dns(
    route: &Route,
    profile: &DnsProfile,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
    stored: &StoredDns,
) -> Result<(), NetworkRefusal> {
    let mut faults = RouteDnsFaults::default();
    faults.carriers(None, route, profiles, stored, |_| true);
    faults.route(None, route, profile, profiles, pools);
    faults.refusal()
}

// What routes on their DNS profiles cannot answer, gathered over every route
// a check asks about. Each fault holds the routes at fault that have a
// holder; a draft is at fault with none.
#[derive(Default)]
struct RouteDnsFaults {
    needs_proxy: Option<Vec<Consumer>>,
    needs_wireguard: Option<Vec<Consumer>>,
    tunnel_proxies: Vec<u32>,
    tunnel_routes: Vec<Consumer>,
}

impl RouteDnsFaults {
    fn at_fault(list: &mut Option<Vec<Consumer>>, holder: Option<Consumer>) {
        let list = list.get_or_insert_with(Vec::new);
        if let Some(holder) = holder
            && !list.contains(&holder)
        {
            list.push(holder);
        }
    }

    fn tunnel(&mut self, proxy: u32, holder: Option<Consumer>) {
        if !self.tunnel_proxies.contains(&proxy) {
            self.tunnel_proxies.push(proxy);
        }
        if let Some(holder) = holder
            && !self.tunnel_routes.contains(&holder)
        {
            self.tunnel_routes.push(holder);
        }
    }

    // Add the WireGuard carriers with no tunnel DNS in `route`'s chains
    // whose carried proxy's profile `counts`.
    fn carriers(
        &mut self,
        holder: Option<Consumer>,
        route: &Route,
        profiles: &HashMap<u32, ProxyProfile>,
        stored: &StoredDns,
        counts: impl Fn(u32) -> bool,
    ) {
        for (carrier, carried) in carriers_without_dns(route, profiles, stored) {
            if counts(carried) {
                self.tunnel(carrier, holder);
            }
        }
    }

    // Add the faults of `route` on `profile`: the proxy answering on a stage
    // with no proxy, unless the profile lets the host answer there;
    // validating the proxy's answers on a stage that does not end at
    // WireGuard; and the proxy answering at a WireGuard proxy that lists no
    // DNS servers.
    fn route(
        &mut self,
        holder: Option<Consumer>,
        route: &Route,
        profile: &DnsProfile,
        profiles: &HashMap<u32, ProxyProfile>,
        pools: &HashMap<u32, ProxyPool>,
    ) {
        if profile.who != DnsWho::Proxy {
            return;
        }
        if !profile.system_fallback && has_proxyless_stage(route) {
            Self::at_fault(&mut self.needs_proxy, holder);
        }
        if profile.trust == DnsTrust::Validate
            && reaches_other_than_wireguard(route, profiles, pools)
        {
            Self::at_fault(&mut self.needs_wireguard, holder);
        }
        for id in wireguard_without_dns(route, profiles, pools) {
            self.tunnel(id, holder);
        }
    }

    fn refusal(self) -> Result<(), NetworkRefusal> {
        if let Some(routes) = self.needs_proxy {
            return Err(NetworkRefusal::DnsProfileNeedsProxy { routes });
        }
        if let Some(routes) = self.needs_wireguard {
            return Err(NetworkRefusal::DnsProfileNeedsWireGuard { routes });
        }
        let mut proxies = self.tunnel_proxies;
        if !proxies.is_empty() {
            proxies.sort_unstable();
            return Err(NetworkRefusal::DnsProfileNeedsTunnelDns {
                proxies,
                routes: self.tunnel_routes,
            });
        }
        Ok(())
    }
}

async fn next_id(tx: &mut SqlTx<'_>, table: &str) -> Result<u32, StateError> {
    let row = tx
        .fetch_optional(
            &format!("SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {table}"),
            &[],
        )
        .await?
        .ok_or_else(|| error("cannot allocate an id"))?;
    u32::try_from(row.i64("next_id")?).map_err(error)
}

async fn count(tx: &mut SqlTx<'_>, table: &str) -> Result<usize, StateError> {
    let row = tx
        .fetch_optional(&format!("SELECT COUNT(*) AS n FROM {table}"), &[])
        .await?
        .ok_or_else(|| error("cannot count rows"))?;
    usize::try_from(row.i64("n")?).map_err(error)
}

async fn exists(tx: &mut SqlTx<'_>, table: &str, id: u32) -> Result<bool, StateError> {
    Ok(tx
        .fetch_optional(
            &format!("SELECT id FROM {table} WHERE id = {{}}"),
            &[SqlArg::I64(i64::from(id))],
        )
        .await?
        .is_some())
}

// The names of the profiles that use list `id`, in name order.
async fn profiles_using_list(tx: &mut SqlTx<'_>, id: u32) -> Result<Vec<String>, StateError> {
    tx.fetch_all(
        "SELECT name FROM dns_profiles WHERE server_list_id = {} ORDER BY name",
        &[SqlArg::I64(i64::from(id))],
    )
    .await?
    .into_iter()
    .map(|row| row.text("name"))
    .collect()
}

pub trait ProxiesDnsPersistenceQueries {
    fn list_dns_server_lists(&self) -> Result<Vec<DnsServerList>, StateError>;
    fn list_dns_profiles(&self) -> Result<Vec<DnsProfile>, StateError>;
    fn list_dns_profiles_of(
        &self,
        kind: Option<DnsProfileKind>,
    ) -> Result<Vec<DnsProfile>, StateError>;
    fn dns_profile(&self, id: u32) -> Result<Option<DnsProfile>, StateError>;
    fn load_dns(&self) -> Result<StoredDns, StateError>;
    fn dns_list_users(&self, ids: &[u32]) -> Result<HashMap<u32, Vec<String>>, StateError>;
    fn dns_profile_proxy_counts(&self) -> Result<HashMap<u32, u32>, StateError>;
    fn dns_profile_route_counts(&self) -> Result<HashMap<u32, u32>, StateError>;
    fn save_dns_server_list(
        &self,
        list: DnsServerList,
        create: bool,
    ) -> Result<DnsServerList, StateError>;
    fn delete_dns_server_list(&self, id: u32) -> Result<(), StateError>;
    fn save_dns_profile(&self, profile: DnsProfile, create: bool)
    -> Result<DnsProfile, StateError>;
    fn delete_dns_profile(&self, id: u32) -> Result<(), StateError>;
}

impl ProxiesDnsPersistenceQueries for SqlDatabase {
    /// Every saved server list, in id order. A list whose stored servers
    /// cannot be read is given with none; saving it again repairs it.
    fn list_dns_server_lists(&self) -> Result<Vec<DnsServerList>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT id, name, servers FROM dns_server_lists ORDER BY id",
                &[],
            )
            .await?
            .iter()
            .map(|row| list_row(row).map(|(list, _)| list))
            .collect()
        })
    }

    /// Every profile, the built-in System profile first, then by id.
    fn list_dns_profiles(&self) -> Result<Vec<DnsProfile>, StateError> {
        self.list_dns_profiles_of(None)
    }

    /// The profiles something taking `kind` may select, in the order of
    /// `list_dns_profiles`: the built-in System profile, then the saved
    /// profiles of that kind. `None` is every profile.
    fn list_dns_profiles_of(
        &self,
        kind: Option<DnsProfileKind>,
    ) -> Result<Vec<DnsProfile>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut profiles = vec![DnsProfile::system()];
            let (sql, args) = match kind {
                None => (
                    "SELECT id, name, kind, who, transit, trust, server_list_id, system_fallback FROM dns_profiles ORDER BY id",
                    Vec::new(),
                ),
                Some(kind) => (
                    "SELECT id, name, kind, who, transit, trust, server_list_id, system_fallback FROM dns_profiles WHERE kind = {} ORDER BY id",
                    vec![SqlArg::Text(super::kind_text(kind).into())],
                ),
            };
            for row in SqlRuntime::fetch_all(store.read_exec(), sql, &args).await?
            {
                profiles.push(profile_row(&row)?);
            }
            Ok(profiles)
        })
    }

    /// Profile `id`, the built-in System profile for its id, or `None`
    /// when no profile has it.
    fn dns_profile(&self, id: u32) -> Result<Option<DnsProfile>, StateError> {
        if id == SYSTEM_DNS_PROFILE_ID {
            return Ok(Some(DnsProfile::system()));
        }
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT id, name, kind, who, transit, trust, server_list_id, system_fallback FROM dns_profiles WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?
            .first()
            .map(profile_row)
            .transpose()
        })
    }

    /// The lists and profiles as the routing configuration loads them.
    fn load_dns(&self) -> Result<StoredDns, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut stored = StoredDns::default();
            for row in SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT id, name, servers FROM dns_server_lists",
                &[],
            )
            .await?
            {
                let (list, fault) = list_row(&row)?;
                if let Some(fault) = fault {
                    stored.unreadable.insert(list.id, fault);
                }
                stored.lists.insert(list.id, list);
            }
            for row in SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT id, name, kind, who, transit, trust, server_list_id, system_fallback FROM dns_profiles",
                &[],
            )
            .await?
            {
                let profile = profile_row(&row)?;
                stored.profiles.insert(profile.id, profile);
            }
            Ok(stored)
        })
    }

    /// The names of the profiles using each list in `ids`.
    fn dns_list_users(&self, ids: &[u32]) -> Result<HashMap<u32, Vec<String>>, StateError> {
        let mut ids = ids.to_vec();
        ids.sort_unstable();
        ids.dedup();
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            let mut users: HashMap<u32, Vec<String>> = HashMap::new();
            // Every profile of one list lands in the same chunk, so each
            // list's names keep their order.
            for chunk in ids.chunks(max_rows_for_engine(store.engine(), 1)) {
                let placeholders = vec!["{}"; chunk.len()].join(", ");
                let args: Vec<SqlArg> = chunk.iter().map(|id| SqlArg::I64(i64::from(*id))).collect();
                for row in SqlRuntime::fetch_all(
                    store.read_exec(),
                    &format!(
                        "SELECT server_list_id, name FROM dns_profiles WHERE server_list_id IN ({placeholders}) ORDER BY name"
                    ),
                    &args,
                )
                .await?
                {
                    let id = u32::try_from(row.i64("server_list_id")?).map_err(error)?;
                    users.entry(id).or_default().push(row.text("name")?);
                }
            }
            Ok(users)
        })
    }

    /// How many proxies have their address found by each profile, counted
    /// in SQL.
    fn dns_profile_proxy_counts(&self) -> Result<HashMap<u32, u32>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT dns_profile_id, COUNT(*) AS n FROM proxy_profiles GROUP BY dns_profile_id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| {
                Ok((
                    u32::try_from(row.i64("dns_profile_id")?).map_err(error)?,
                    u32::try_from(row.i64("n")?).map_err(error)?,
                ))
            })
            .collect()
        })
    }

    /// How many routes use each profile, counted in SQL.
    fn dns_profile_route_counts(&self) -> Result<HashMap<u32, u32>, StateError> {
        let store = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT dns_profile_id, COUNT(*) AS n FROM proxy_routes GROUP BY dns_profile_id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| {
                Ok((
                    u32::try_from(row.i64("dns_profile_id")?).map_err(error)?,
                    u32::try_from(row.i64("n")?).map_err(error)?,
                ))
            })
            .collect()
        })
    }

    /// Save `list`: a new one when it has no id, else the saved one.
    fn save_dns_server_list(
        &self,
        list: DnsServerList,
        create: bool,
    ) -> Result<DnsServerList, StateError> {
        let list = list.normal();
        list.validate()?;
        let stored = super::stored_servers(&list.servers).map_err(error)?;
        if stored.len() > super::MAX_STORED_SERVERS_BYTES {
            return Err(error("DNS server list is too large to store"));
        }
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_dns_server_list", |tx| {
                let mut list = list.clone();
                let stored = stored.clone();
                Box::pin(async move {
                    super::lock_network(tx).await?;
                    if create {
                        if count(tx, "dns_server_lists").await? >= super::MAX_DNS_LISTS {
                            return Err(NetworkRefusal::DnsListLimitReached.into());
                        }
                        list.id = next_id(tx, "dns_server_lists").await?;
                    } else if !exists(tx, "dns_server_lists", list.id).await? {
                        return Err(NetworkRefusal::DnsListNotFound.into());
                    }
                    tx.execute(
                        "INSERT INTO dns_server_lists (id, name, servers) VALUES ({}, {}, {}) ON CONFLICT(id) DO UPDATE SET name = excluded.name, servers = excluded.servers",
                        &[
                            SqlArg::I64(i64::from(list.id)),
                            SqlArg::Text(list.name.clone()),
                            SqlArg::Text(stored),
                        ],
                    )
                    .await?;
                    // A list edit changes what the routes on its profiles
                    // ask.
                    super::network_persistence::validate_stored_network(
                        tx,
                        Touched::List(list.id),
                    )
                    .await?;
                    Ok(list)
                })
            })
            .await
        })
    }

    /// Delete list `id`, refused while a profile uses it.
    fn delete_dns_server_list(&self, id: u32) -> Result<(), StateError> {
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "delete_dns_server_list", |tx| {
                Box::pin(async move {
                    lock_network(tx).await?;
                    if !exists(tx, "dns_server_lists", id).await? {
                        return Err(NetworkRefusal::DnsListNotFound.into());
                    }
                    let users = profiles_using_list(tx, id).await?;
                    if !users.is_empty() {
                        return Err(NetworkRefusal::DnsListInUse { profiles: users }.into());
                    }
                    tx.execute(
                        "DELETE FROM dns_server_lists WHERE id = {}",
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    /// Save `profile`: a new one when `create`, else the saved one. The
    /// routes using it are checked against the saved profile.
    fn save_dns_profile(
        &self,
        profile: DnsProfile,
        create: bool,
    ) -> Result<DnsProfile, StateError> {
        if !create && profile.is_built_in() {
            return Err(NetworkRefusal::DnsProfileBuiltIn.into());
        }
        let profile = profile.normal();
        profile.validate()?;
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "save_dns_profile", |tx| {
                let mut profile = profile.clone();
                Box::pin(async move {
                    super::lock_network(tx).await?;
                    if create {
                        if count(tx, "dns_profiles").await? >= super::MAX_DNS_PROFILES {
                            return Err(NetworkRefusal::DnsProfileLimitReached.into());
                        }
                        profile.id = next_id(tx, "dns_profiles").await?;
                    } else {
                        let Some(row) = tx
                            .fetch_optional(
                                "SELECT kind FROM dns_profiles WHERE id = {}",
                                &[SqlArg::I64(i64::from(profile.id))],
                            )
                            .await?
                        else {
                            return Err(NetworkRefusal::DnsProfileNotFound.into());
                        };
                        if super::kind_of(&row.text("kind")?) != Some(profile.kind) {
                            return Err(NetworkRefusal::DnsProfileKindImmutable.into());
                        }
                    }
                    if let Some(list) = profile.server_list_id
                        && !exists(tx, "dns_server_lists", list).await?
                    {
                        return Err(NetworkRefusal::DnsListNotFound.into());
                    }
                    tx.execute(
                        "INSERT INTO dns_profiles (id, name, kind, who, transit, trust, server_list_id, system_fallback) VALUES ({}, {}, {}, {}, {}, {}, {}, {}) ON CONFLICT(id) DO UPDATE SET name = excluded.name, who = excluded.who, transit = excluded.transit, trust = excluded.trust, server_list_id = excluded.server_list_id, system_fallback = excluded.system_fallback",
                        &[
                            SqlArg::I64(i64::from(profile.id)),
                            SqlArg::Text(profile.name.clone()),
                            SqlArg::Text(super::kind_text(profile.kind).into()),
                            SqlArg::Text(super::who_text(profile.who).into()),
                            SqlArg::OptText(profile.transit.map(|t| super::transit_text(t).into())),
                            SqlArg::Text(super::trust_text(profile.trust).into()),
                            SqlArg::OptI64(profile.server_list_id.map(i64::from)),
                            SqlArg::Bool(profile.system_fallback),
                        ],
                    )
                    .await?;
                    super::network_persistence::validate_stored_network(
                        tx,
                        Touched::Profile(profile.id),
                    )
                    .await?;
                    Ok(profile)
                })
            })
            .await
        })
    }

    /// Delete profile `id`, refused for the built-in profile and while a
    /// route or a proxy uses it.
    fn delete_dns_profile(&self, id: u32) -> Result<(), StateError> {
        if id == SYSTEM_DNS_PROFILE_ID {
            return Err(NetworkRefusal::DnsProfileBuiltIn.into());
        }
        let store = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "delete_dns_profile", |tx| {
                Box::pin(async move {
                    lock_network(tx).await?;
                    if !exists(tx, "dns_profiles", id).await? {
                        return Err(NetworkRefusal::DnsProfileNotFound.into());
                    }
                    // The stored keys as they are, so a route whose key
                    // cannot be read is still named.
                    let routes: Vec<String> = tx
                        .fetch_all(
                            "SELECT consumer FROM proxy_routes WHERE dns_profile_id = {} ORDER BY consumer",
                            &[SqlArg::I64(i64::from(id))],
                        )
                        .await?
                        .iter()
                        .map(|row| row.text("consumer"))
                        .collect::<Result<_, _>>()?;
                    let proxies: Vec<u32> = tx
                        .fetch_all(
                            "SELECT id FROM proxy_profiles WHERE dns_profile_id = {} ORDER BY id",
                            &[SqlArg::I64(i64::from(id))],
                        )
                        .await?
                        .iter()
                        .map(|row| u32::try_from(row.i64("id")?).map_err(error))
                        .collect::<Result<_, _>>()?;
                    if !routes.is_empty() || !proxies.is_empty() {
                        return Err(NetworkRefusal::DnsProfileInUse { routes, proxies }.into());
                    }
                    tx.execute(
                        "DELETE FROM dns_profiles WHERE id = {}",
                        &[SqlArg::I64(i64::from(id))],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }
}

// A restore checks the DNS it brings as saves would have: every list can be
// read and every profile is one a save would take, and the routes are
// checked as a save checks them.
pub async fn validate_restored_dns(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    for row in conn
        .fetch_all("SELECT id, name, servers FROM dns_server_lists", &[])
        .await?
    {
        let (list, fault) = list_row(&row)?;
        if let Some(fault) = fault {
            return Err(StateError::Database(format!(
                "a restored DNS server list cannot be read: {fault}"
            )));
        }
        list.validate()?;
    }
    for row in conn
        .fetch_all(
            "SELECT id, name, kind, who, transit, trust, server_list_id, system_fallback FROM dns_profiles",
            &[],
        )
        .await?
    {
        let profile = profile_row(&row)?;
        if profile.clone().normal() != profile {
            return Err(StateError::Database(
                "a restored DNS profile is not in the form a save stores".into(),
            ));
        }
        profile.validate()?;
    }
    let mut profiles = HashMap::new();
    for row in conn
        .fetch_all("SELECT id, config, dns_profile_id FROM proxy_profiles", &[])
        .await?
    {
        let profile = stored_proxy(&row)?;
        profiles.insert(profile.id, profile);
    }
    let mut pools = HashMap::new();
    for row in conn.fetch_all("SELECT * FROM proxy_pools", &[]).await? {
        let pool = super::network_persistence::pool_row(row)?;
        pools.insert(pool.id, pool);
    }
    validate_route_dns(conn, &profiles, &pools, Touched::All).await
}
