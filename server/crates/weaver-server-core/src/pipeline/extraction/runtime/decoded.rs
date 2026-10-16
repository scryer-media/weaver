use super::*;
use crate::pipeline::direct_store::nested::catalog::DecodedNamespace;
use crate::pipeline::direct_store::nested::{NestedGraph, NodeSpan, ROOT_NODE};
use crate::pipeline::direct_store::plan::DirectSetPlan;
use crate::pipeline::direct_store::router::{DirectDestination, RoutedSpan, RouterResources};
use crate::pipeline::extraction::catalog::decoded::DecodedCatalog;
use crate::pipeline::extraction::output::channel::{Event, Frame, Generation};
use crate::pipeline::extraction::storage::{Destination, DestinationRegistry, WriteTicket};
use crate::pipeline::extraction::{ExtractionRoot, JobExtractionBudget};
use std::io::{Seek, SeekFrom, Write};

pub(crate) struct NamespaceProgress {
    pub(crate) producers: Vec<(u32, Option<u32>, u64, bool)>,
    pub(crate) children: Vec<crate::pipeline::direct_store::set::LiveChildDescription>,
}

pub(crate) struct DecodedState {
    pub(crate) graph: NestedGraph,
    pub(crate) catalog: DecodedCatalog,
    pub(crate) registry: DestinationRegistry,
    pub(crate) plan: DirectSetPlan,
    pub(crate) resources: RouterResources,
    pub(crate) destinations: BTreeMap<u32, PathBuf>,
    pub(crate) generation: Generation,
    pub(super) sessions: ArchiveSessions,
    pub(super) physical: BTreeMap<u32, (Destination, Option<WriteTicket>)>,
    pub(super) base: PathBuf,
    pub(super) root: Arc<ExtractionRoot>,
    pub(super) budget: Arc<JobExtractionBudget>,
    pub(super) delegated: HashSet<u32>,
}

impl DecodedState {
    pub(crate) fn new(
        generation: Generation,
        sessions: ArchiveSessions,
        plan: DirectSetPlan,
        resources: RouterResources,
        budget: Arc<JobExtractionBudget>,
        base: PathBuf,
    ) -> Result<Self, RuntimeError> {
        let root = Arc::new(
            ExtractionRoot::open(&base).map_err(RuntimeError::refused("decoded namespace root"))?,
        );
        let registry = DestinationRegistry::new(
            budget
                .try_reserve_memory(limits::CATALOG_ADMISSION_BYTES)
                .ok_or(RuntimeError::Admission("archive registry"))?,
        )
        .map_err(RuntimeError::session("registry admission"))?;
        let catalog = DecodedCatalog::new(
            generation.clone(),
            budget
                .try_reserve_memory(limits::CATALOG_ADMISSION_BYTES)
                .ok_or(RuntimeError::Admission("archive catalog"))?,
        );
        let mut graph = NestedGraph::default();
        graph
            .set_namespace_depth(
                sessions
                    .depth(generation.owner())
                    .ok_or(RuntimeError::Invariant("decoded owner"))?,
            )
            .map_err(RuntimeError::routing("decoded namespace depth"))?;
        Ok(Self {
            graph,
            catalog,
            registry,
            plan,
            resources,
            destinations: BTreeMap::new(),
            generation,
            sessions,
            physical: BTreeMap::new(),
            base,
            root,
            budget,
            delegated: HashSet::new(),
        })
    }

    pub(crate) fn namespace(&self) -> DecodedNamespace<'_> {
        DecodedNamespace {
            catalog: &self.catalog,
            plan: &self.plan,
            resources: &self.resources,
            destinations: &self.destinations,
        }
    }

    pub(crate) async fn publish_progress(
        &mut self,
        job_id: crate::jobs::ids::JobId,
        sender: &tokio::sync::mpsc::Sender<crate::pipeline::ExtractionDone>,
        lease: &SessionLease,
    ) -> Result<(), RuntimeError> {
        let NamespaceProgress {
            producers,
            children,
        } = self.progress_snapshot();
        if children.is_empty() {
            return Ok(());
        }
        let nodes: Vec<_> = children.iter().map(|child| child.node).collect();
        let owner = self.generation.owner();
        let event = crate::pipeline::ExtractionDone::ScopedNamespace {
            job_id,
            owner,
            namespace: format!("decoded-{}-{}", owner.node, owner.generation),
            producers,
            children,
        };
        tokio::select! {
            biased;
            _ = lease.cancelled() => Err(RuntimeError::Revoked),
            result = sender.send(event) => result.map_err(|_| RuntimeError::CoordinatorClosed),
        }?;
        self.delegated.extend(nodes);
        Ok(())
    }

    pub(crate) fn progress_snapshot(&self) -> NamespaceProgress {
        NamespaceProgress {
            producers: self.graph.direct_producer_states(),
            children: self.graph.live_child_descriptions(&self.namespace()),
        }
    }

    pub(crate) async fn accept(&mut self, frame: Frame) -> Result<(), RuntimeError> {
        if self.generation.is_revoked() || frame.lease.is_revoked() {
            return Err(RuntimeError::Revoked);
        }
        let member = self
            .catalog
            .accept(&frame)
            .map_err(RuntimeError::io("decoded catalog"))?;
        match &frame.event {
            Event::Directory { .. } => {
                let relative = PathBuf::from(format!("directory-{member}"));
                self.root.create_dir(&relative, &self.budget)?;
                self.destinations.insert(member, self.base.join(relative));
                return Ok(());
            }
            Event::Metadata(metadata) => {
                // A routed archive wrapper may have no physical backing. Its
                // metadata does not describe any of the published leaf files.
                if self.graph.binding(ROOT_NODE, member).is_some() {
                    return Ok(());
                }
                let path = self
                    .destinations
                    .get(&member)
                    .ok_or(RuntimeError::Invariant("metadata backing"))?;
                let relative = path
                    .strip_prefix(&self.base)
                    .map_err(|_| RuntimeError::Invariant("metadata escaped namespace"))?;
                // Stream declarations can describe an empty file, which never
                // produces a byte placement to create its backing.
                if !path.exists() {
                    self.root
                        .open_partial(relative)
                        .map_err(RuntimeError::io("metadata backing"))?;
                }
                self.root
                    .set_directory_metadata(
                        relative,
                        metadata.modified,
                        metadata.accessed,
                        metadata.unix_mode,
                    )
                    .map_err(RuntimeError::io("decoded metadata"))?;
                return Ok(());
            }
            Event::PhysicalFile { length, .. } => {
                let relative = PathBuf::from(format!("member-{}", frame.member.occurrence));
                self.destinations.insert(member, self.base.join(&relative));
                let destination = Destination {
                    namespace: frame.member.owner,
                    owner: crate::pipeline::direct_store::router::SpanOwner {
                        generation: frame.member.owner.generation,
                        ..Default::default()
                    },
                    destination: DirectDestination::Member { member_id: member },
                };
                let length = length.ok_or(RuntimeError::Invariant(
                    "physical output requires declared length",
                ))?;
                self.registry
                    .register(&self.sessions, destination, &relative, Some(length))
                    .map_err(RuntimeError::session("physical registration"))?;
                let ticket = self
                    .registry
                    .begin_write(&self.sessions, destination, 0, length)
                    .map_err(RuntimeError::session("physical ticket"))?;
                self.physical.insert(member, (destination, Some(ticket)));
                return Ok(());
            }
            Event::File { .. } => {
                self.destinations
                    .insert(member, self.base.join(format!("stream-{member}")));
            }
            _ => {}
        }
        if let Some((destination, ticket)) = self.physical.get_mut(&member) {
            if let Event::DataComplete { length } = frame.event {
                self.registry
                    .placed(
                        &self.sessions,
                        ticket
                            .take()
                            .ok_or(RuntimeError::Invariant("duplicate physical completion"))?,
                    )
                    .map_err(RuntimeError::session("physical placement"))?;
                self.registry
                    .seal(&self.sessions, *destination, length)
                    .map_err(RuntimeError::session("physical seal"))?;
            }
            return Ok(());
        }
        let spans = match frame.event {
            Event::Bytes { offset, bytes } => vec![RoutedSpan {
                owner: Default::default(),
                destination: DirectDestination::Member { member_id: member },
                destination_offset: offset,
                volume_index: 0,
                source_offset: offset,
                bytes: vec![bytes],
            }],
            _ => Vec::new(),
        };
        let namespace = DecodedNamespace {
            catalog: &self.catalog,
            plan: &self.plan,
            resources: &self.resources,
            destinations: &self.destinations,
        };
        let writes = self
            .graph
            .forward(&namespace, ROOT_NODE, spans)
            .map_err(RuntimeError::routing("decoded forwarding"))?;
        // Routing retention owns its own holds/scratch admission. Release the
        // bridge frame only after that transfer; emitted writes still own Bytes.
        drop(frame.lease);
        self.place(writes).await?;
        self.materialize_pending().await
    }

    fn create_dataless_outputs(&mut self) -> Result<(), RuntimeError> {
        use crate::pipeline::direct_store::nested::NodeMode;
        let claims = self
            .registry
            .checkpoint_claims()
            .map_err(RuntimeError::session("dataless inventory"))?;
        for (index, node) in self.graph.nodes.iter().enumerate() {
            if node.mode != NodeMode::Direct || !node.router.all_members_verified() {
                continue;
            }
            let mut ancestor = node.parent;
            while ancestor != ROOT_NODE && self.graph.node(ancestor).mode == NodeMode::Direct {
                ancestor = self.graph.node(ancestor).parent;
            }
            if ancestor != ROOT_NODE {
                continue;
            }
            for (entry_index, entry) in node.router.dataless_entries().into_iter().enumerate() {
                let path = node
                    .router
                    .plan()
                    .destination_dir
                    .join(format!("dataless-{entry_index}"));
                let relative = path
                    .strip_prefix(&self.base)
                    .map_err(|_| RuntimeError::Invariant("dataless output escaped namespace"))?;
                if entry.is_directory {
                    if self.root.probe_directory(relative).is_ok() {
                        continue;
                    }
                    self.root.create_dir(relative, &self.budget)?;
                } else {
                    let destination = Destination {
                        namespace: self.generation.owner(),
                        owner: crate::pipeline::direct_store::router::SpanOwner {
                            node: index as u32 + 1,
                            generation: node.generation,
                            raw_child: None,
                        },
                        destination: DirectDestination::Member {
                            member_id: u32::MAX - entry_index as u32,
                        },
                    };
                    if claims.iter().any(|(selected, path, length, ranges)| {
                        *selected == destination
                            && path == relative
                            && *length == 0
                            && ranges.is_empty()
                    }) {
                        continue;
                    }
                    let mut output = self.root.create_file(relative, &self.budget)?;
                    output
                        .flush()
                        .map_err(RuntimeError::io("dataless output"))?;
                    drop(output);
                    self.registry
                        .register(&self.sessions, destination, relative, Some(0))
                        .map_err(RuntimeError::session("dataless registration"))?;
                    self.registry
                        .seal(&self.sessions, destination, 0)
                        .map_err(RuntimeError::session("dataless seal"))?;
                }
                let time = |ticks: u64| {
                    std::time::SystemTime::UNIX_EPOCH
                        .checked_add(std::time::Duration::new(
                            ticks / 10_000_000,
                            ((ticks % 10_000_000) * 100) as u32,
                        ))
                        .and_then(|time| {
                            time.checked_sub(std::time::Duration::from_secs(11_644_473_600))
                        })
                };
                self.root
                    .set_directory_metadata(
                        relative,
                        entry.modified.and_then(time),
                        entry.accessed.and_then(time),
                        None,
                    )
                    .map_err(RuntimeError::io("dataless metadata"))?;
            }
        }
        Ok(())
    }

    async fn place(&mut self, writes: Vec<NodeSpan>) -> Result<(), RuntimeError> {
        let mut writes: std::collections::VecDeque<_> = writes.into();
        while let Some(write) = writes.pop_front() {
            if self.generation.is_revoked() {
                return Err(RuntimeError::Revoked);
            }
            let receipt = write.placement();
            let path = if write.owner == ROOT_NODE {
                let DirectDestination::Member { member_id } = receipt.destination else {
                    return Err(RuntimeError::Invariant(
                        "decoded root has no volume envelope",
                    ));
                };
                self.destinations
                    .get(&member_id)
                    .ok_or(RuntimeError::Invariant("decoded destination"))?
                    .clone()
            } else {
                let router = &self.graph.node(write.owner).router;
                match receipt.destination {
                    DirectDestination::Envelope { volume_index } => {
                        router.plan().envelope_path(volume_index)
                    }
                    DirectDestination::Member { member_id } => {
                        let partial = router
                            .member_partials()
                            .into_iter()
                            .find(|(id, _, _)| *id == member_id)
                            .ok_or(RuntimeError::Invariant("nested destination"))?
                            .2;
                        router.plan().destination_path(partial)
                    }
                }
            };
            let relative = path
                .strip_prefix(&self.base)
                .map_err(|_| RuntimeError::Invariant("decoded output escaped private root"))?
                .to_owned();
            let destination = Destination {
                namespace: self.generation.owner(),
                owner: receipt.owner,
                destination: receipt.destination,
            };
            self.registry
                .register(&self.sessions, destination, &relative, None)
                .map_err(RuntimeError::session("decoded registration"))?;
            let ticket = self
                .registry
                .begin_write(
                    &self.sessions,
                    destination,
                    receipt.destination_offset,
                    receipt.len,
                )
                .map_err(RuntimeError::session("decoded write admission"))?;
            let root = Arc::clone(&self.root);
            let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
                let mut file = root.open_partial(&relative)?;
                file.seek(SeekFrom::Start(write.span.destination_offset))?;
                for bytes in write.span.bytes {
                    file.write_all(&bytes)?;
                }
                file.flush()
            })
            .await
            .map_err(RuntimeError::join("decoded write"))?;
            if let Err(error) = result {
                let _ = self.registry.failed(ticket);
                return Err(RuntimeError::io("decoded write")(error));
            }
            self.registry
                .placed(&self.sessions, ticket)
                .map_err(RuntimeError::session("decoded placement"))?;
            let namespace = DecodedNamespace {
                catalog: &self.catalog,
                plan: &self.plan,
                resources: &self.resources,
                destinations: &self.destinations,
            };
            self.graph
                .record_placement(&namespace, receipt)
                .map_err(RuntimeError::routing("decoded receipt"))?;
        }
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), RuntimeError> {
        self.catalog
            .close()
            .map_err(RuntimeError::io("decoded catalog close"))?;
        let namespace = DecodedNamespace {
            catalog: &self.catalog,
            plan: &self.plan,
            resources: &self.resources,
            destinations: &self.destinations,
        };
        let writes = self
            .graph
            .close_parent_roster(&namespace, ROOT_NODE)
            .map_err(RuntimeError::routing("decoded roster"))?;
        self.place(writes).await?;
        self.materialize_pending().await?;
        self.create_dataless_outputs()
    }

    async fn materialize_pending(&mut self) -> Result<(), RuntimeError> {
        for id in 1..=self.graph.nodes.len() as u32 {
            let namespace = self.namespace();
            let Some(work) = self.graph.prepare_materialization(&namespace, id) else {
                continue;
            };
            let mut tickets = Vec::new();
            for (path, receipt) in work.writes() {
                let destination = Destination {
                    namespace: self.generation.owner(),
                    owner: receipt.owner,
                    destination: DirectDestination::Member {
                        member_id: receipt.member,
                    },
                };
                let relative = path
                    .strip_prefix(&self.base)
                    .map_err(|_| RuntimeError::Invariant("materialization escaped namespace"))?;
                self.registry
                    .register(&self.sessions, destination, relative, None)
                    .map_err(RuntimeError::session("materialization registration"))?;
                tickets.push(
                    self.registry
                        .begin_write(&self.sessions, destination, receipt.offset, receipt.len)
                        .map_err(RuntimeError::session("materialization admission"))?,
                );
            }
            let root = Arc::clone(&self.root);
            let base = self.base.clone();
            let budget = Arc::clone(&self.budget);
            let memory = budget
                .try_reserve_memory(limits::MATERIALIZATION_PERMIT_BYTES)
                .ok_or(RuntimeError::Admission("materialization"))?;
            let result = tokio::task::spawn_blocking(move || {
                let _memory = memory;
                crate::pipeline::direct_store::nested::materialize::copy_inputs_to_checked(
                    work,
                    root,
                    base,
                    Default::default(),
                    || budget.check_active_io(),
                )
            })
            .await
            .map_err(RuntimeError::join("materialization"))?
            .map_err(RuntimeError::io("materialization"))?;
            if self.generation.is_revoked() {
                return Err(RuntimeError::Revoked);
            }
            let namespace = DecodedNamespace {
                catalog: &self.catalog,
                plan: &self.plan,
                resources: &self.resources,
                destinations: &self.destinations,
            };
            self.graph
                .apply_materialization(&namespace, result)
                .map_err(RuntimeError::routing("materialization apply"))?;
            for ticket in tickets {
                self.registry
                    .placed(&self.sessions, ticket)
                    .map_err(RuntimeError::session("materialization placement"))?;
            }
        }
        Ok(())
    }

    pub(crate) fn publication(&self) -> Result<BTreeMap<String, PathBuf>, RuntimeError> {
        let mut output = BTreeMap::new();
        self.collect_publication(ROOT_NODE, &mut output)?;
        Ok(output)
    }

    fn collect_publication(
        &self,
        owner: u32,
        output: &mut BTreeMap<String, PathBuf>,
    ) -> Result<(), RuntimeError> {
        let facts: Vec<(u32, String, PathBuf)> =
            if owner == ROOT_NODE {
                use crate::pipeline::extraction::catalog::MemberCatalog;
                self.catalog
                    .members()
                    .into_iter()
                    .map(|fact| {
                        Ok((
                            fact.id,
                            fact.name.to_owned(),
                            self.destinations
                                .get(&fact.id)
                                .ok_or(RuntimeError::Invariant("decoded backing"))?
                                .clone(),
                        ))
                    })
                    .collect::<Result<_, RuntimeError>>()?
            } else {
                let router = &self.graph.node(owner).router;
                if !router.all_members_verified() {
                    return Err(RuntimeError::Invariant(
                        "nested archive integrity remains unresolved",
                    ));
                }
                router
                    .member_partials()
                    .into_iter()
                    .map(|(id, name, partial)| {
                        (id, name.to_owned(), router.plan().destination_path(partial))
                    })
                    .chain(router.dataless_entries().into_iter().enumerate().map(
                        |(index, entry)| {
                            (
                                u32::MAX - index as u32,
                                entry.name,
                                router
                                    .plan()
                                    .destination_dir
                                    .join(format!("dataless-{index}")),
                            )
                        },
                    ))
                    .collect()
            };
        for (id, name, path) in facts {
            if self
                .graph
                .binding(owner, id)
                .is_some_and(|(child, _)| self.delegated.contains(&child))
            {
                continue;
            }
            if let Some((child, _)) = self.graph.binding(owner, id)
                && self.graph.node(child).mode
                    != crate::pipeline::direct_store::nested::NodeMode::Materialized
            {
                // A multi-volume child is visited exactly once.
                if self
                    .graph
                    .node(child)
                    .inputs
                    .values()
                    .next()
                    .is_some_and(|input| input.parent_member == id)
                {
                    self.collect_publication(child, output)?;
                }
                continue;
            }
            if output.insert(name.clone(), path).is_some() {
                return Err(RuntimeError::OutputCollision(name));
            }
        }
        Ok(())
    }
}
