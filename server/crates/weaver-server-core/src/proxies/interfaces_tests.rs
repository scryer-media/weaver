use super::*;

fn address(ip: &str) -> InterfaceAddress {
    InterfaceAddress {
        address: ip.parse().unwrap(),
        deprecated: false,
        tentative: false,
    }
}

fn snapshot() -> InterfaceSnapshot {
    InterfaceSnapshot {
        interfaces: vec![DiscoveredInterface {
            name: "wan0".into(),
            index: Some(1),
            up: true,
            addresses: vec![address("192.0.2.1"), address("2001:db8::1")],
        }],
        error: None,
    }
}

fn egress(binding: EgressBinding) -> EgressInterface {
    EgressInterface {
        id: 1,
        name: "Test egress".into(),
        binding,
        enabled: true,
        max_download_speed: 0,
        download_quota: Default::default(),
    }
}

#[test]
fn health_distinguishes_missing_down_family_and_address_lifetime() {
    let mut sample = snapshot();
    let route = egress(EgressBinding::Interface {
        name: "wan0".into(),
    });
    assert_eq!(sample.health(&route, Some(false)), EgressHealth::Up);
    sample.interfaces[0].addresses[0].deprecated = true;
    assert!(matches!(
        sample.health(&route, Some(false)),
        EgressHealth::Down(_)
    ));
    assert_eq!(sample.health(&route, Some(true)), EgressHealth::Up);
    sample.interfaces[0].addresses[1].tentative = true;
    assert!(matches!(sample.health(&route, None), EgressHealth::Down(_)));
    sample.interfaces[0].up = false;
    assert_eq!(
        sample.health(&route, None),
        EgressHealth::Down("Interface is down".into())
    );
    sample.interfaces.clear();
    assert_eq!(
        sample.health(&route, None),
        EgressHealth::Down("Interface is missing".into())
    );
}

#[test]
fn source_must_be_present_and_usable_and_system_survives_enumeration_failure() {
    let mut sample = snapshot();
    let route = egress(EgressBinding::SourceAddress {
        address: "192.0.2.1".parse().unwrap(),
    });
    assert_eq!(sample.health(&route, Some(false)), EgressHealth::Up);
    assert!(matches!(
        sample.health(&route, Some(true)),
        EgressHealth::Down(_)
    ));
    sample.error = Some("enumeration failed".into());
    assert_eq!(sample.health(&route, None), EgressHealth::Unknown);
    assert_eq!(
        sample.health(&EgressInterface::system(), None),
        EgressHealth::Up
    );
    for ip in [
        "0.0.0.0",
        "224.0.0.1",
        "169.254.1.1",
        "::",
        "ff02::1",
        "fe80::1",
        "fd00::1",
    ] {
        assert!(!address(ip).usable(), "{ip}");
    }
}

#[tokio::test(start_paused = true)]
async fn injected_interface_changes_are_sampled_by_the_poll_clock() {
    struct Source {
        interfaces: RwLock<Vec<DiscoveredInterface>>,
        calls: tokio::sync::watch::Sender<u32>,
    }
    impl InterfaceSource for Source {
        fn interfaces(&self) -> io::Result<Vec<DiscoveredInterface>> {
            self.calls.send_modify(|n| *n += 1);
            Ok(self.interfaces.read().unwrap().clone())
        }
    }
    let (calls, mut observed) = tokio::sync::watch::channel(0);
    let source = Arc::new(Source {
        interfaces: RwLock::new(snapshot().interfaces),
        calls,
    });
    let monitor = InterfaceMonitor::start(source.clone(), &tokio::runtime::Handle::current());
    let route = egress(EgressBinding::Interface {
        name: "wan0".into(),
    });
    assert_eq!(monitor.snapshot().health(&route, None), EgressHealth::Up);
    observed.borrow_and_update();
    source.interfaces.write().unwrap().clear();
    // Await the new sample, allowing Tokio's paused clock to advance to the poll.
    observed.changed().await.unwrap();
    assert!(matches!(
        monitor.snapshot().health(&route, None),
        EgressHealth::Down(_)
    ));
    source
        .interfaces
        .write()
        .unwrap()
        .extend(snapshot().interfaces);
    observed.borrow_and_update();
    observed.changed().await.unwrap();
    assert_eq!(monitor.snapshot().health(&route, None), EgressHealth::Up);
}
