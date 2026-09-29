use std::{
    collections::BTreeMap,
    io,
    net::IpAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::{EgressBinding, EgressInterface};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterfaceAddress {
    pub address: IpAddr,
    pub deprecated: bool,
    pub tentative: bool,
}

impl InterfaceAddress {
    pub fn usable(&self) -> bool {
        !self.deprecated
            && !self.tentative
            && match self.address {
                IpAddr::V4(ip) => !ip.is_unspecified() && !ip.is_multicast() && !ip.is_link_local(),
                IpAddr::V6(ip) => {
                    !ip.is_unspecified()
                        && !ip.is_multicast()
                        && !ip.is_unicast_link_local()
                        && ip.octets()[0] != 0xfd
                }
            }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredInterface {
    pub name: String,
    pub index: Option<u32>,
    pub up: bool,
    pub addresses: Vec<InterfaceAddress>,
}

pub trait InterfaceSource: Send + Sync {
    fn interfaces(&self) -> io::Result<Vec<DiscoveredInterface>>;
}

pub struct SystemInterfaceSource;

impl InterfaceSource for SystemInterfaceSource {
    fn interfaces(&self) -> io::Result<Vec<DiscoveredInterface>> {
        let mut interfaces = BTreeMap::<String, DiscoveredInterface>::new();
        for iface in if_addrs::get_if_addrs()? {
            let address = iface.ip();
            let up = iface.is_oper_up();
            let (deprecated, tentative) =
                weaver_tunnel::egress::address_flags(&iface.name, address);
            let entry =
                interfaces
                    .entry(iface.name.clone())
                    .or_insert_with(|| DiscoveredInterface {
                        name: iface.name,
                        index: iface.index,
                        up,
                        addresses: Vec::new(),
                    });
            entry.up |= up;
            entry.addresses.push(InterfaceAddress {
                address,
                deprecated,
                tentative,
            });
        }
        for iface in interfaces.values_mut() {
            iface.addresses.sort_by_key(|a| a.address);
            iface.addresses.dedup_by_key(|a| a.address);
        }
        weaver_tunnel::egress::cache_interface_families(
            interfaces
                .values()
                .filter(|iface| iface.up)
                .flat_map(|iface| {
                    iface
                        .addresses
                        .iter()
                        .filter(|address| address.usable())
                        .map(|address| (iface.name.clone(), address.address.is_ipv4()))
                }),
        );
        Ok(interfaces.into_values().collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "reason", rename_all = "camelCase")]
pub enum EgressHealth {
    Up,
    Down(String),
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub struct InterfaceSnapshot {
    pub interfaces: Vec<DiscoveredInterface>,
    pub error: Option<String>,
}

impl InterfaceSnapshot {
    /// Family filtering is consumer-specific; `None` reports overall egress health.
    pub fn health(&self, egress: &EgressInterface, ipv6: Option<bool>) -> EgressHealth {
        if matches!(egress.binding, EgressBinding::System) {
            return EgressHealth::Up;
        }
        if !egress.enabled {
            return EgressHealth::Down("Disabled".into());
        }
        if self.error.is_some() {
            return EgressHealth::Unknown;
        }
        let usable =
            |a: &InterfaceAddress| a.usable() && ipv6.is_none_or(|v6| a.address.is_ipv6() == v6);
        match &egress.binding {
            EgressBinding::System => EgressHealth::Up,
            EgressBinding::Interface { name } => {
                match self.interfaces.iter().find(|i| &i.name == name) {
                    None => EgressHealth::Down("Interface is missing".into()),
                    Some(i) if !i.up => EgressHealth::Down("Interface is down".into()),
                    Some(i) if !i.addresses.iter().any(usable) => {
                        EgressHealth::Down("No usable address for this destination".into())
                    }
                    Some(_) => EgressHealth::Up,
                }
            }
            EgressBinding::SourceAddress { address } => {
                if self.interfaces.iter().any(|i| {
                    i.up && i
                        .addresses
                        .iter()
                        .any(|a| a.address == *address && usable(a))
                }) {
                    EgressHealth::Up
                } else {
                    EgressHealth::Down("Source address is unavailable".into())
                }
            }
        }
    }
}

/// Owns its polling task; dropping the monitor stops all further enumeration.
pub struct InterfaceMonitor {
    snapshot: Arc<RwLock<InterfaceSnapshot>>,
    task: tokio::task::JoinHandle<()>,
}

impl InterfaceMonitor {
    pub fn start(source: Arc<dyn InterfaceSource>, handle: &tokio::runtime::Handle) -> Self {
        let sample = || match source.interfaces() {
            Ok(interfaces) => InterfaceSnapshot {
                interfaces,
                error: None,
            },
            Err(error) => InterfaceSnapshot {
                interfaces: Vec::new(),
                error: Some(error.to_string()),
            },
        };
        let snapshot = Arc::new(RwLock::new(sample()));
        let latest = snapshot.clone();
        let task = handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                interval.tick().await;
                let next = match source.interfaces() {
                    Ok(interfaces) => InterfaceSnapshot {
                        interfaces,
                        error: None,
                    },
                    Err(error) => InterfaceSnapshot {
                        interfaces: Vec::new(),
                        error: Some(error.to_string()),
                    },
                };
                *latest.write().expect("interface snapshot") = next;
            }
        });
        Self { snapshot, task }
    }

    pub fn snapshot(&self) -> InterfaceSnapshot {
        self.snapshot.read().expect("interface snapshot").clone()
    }
}

impl Drop for InterfaceMonitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
#[path = "interfaces_tests.rs"]
mod tests;
