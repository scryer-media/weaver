use super::*;
use crate::pipeline::direct_store::nested::catalog::DecodedNamespace;
use crate::pipeline::direct_store::nested::materialize::Materialization;
use crate::pipeline::direct_store::nested::{
    NestedGraph, NodeMode, NodePlacement, NodeSpan, ROOT_NODE,
};
use crate::pipeline::direct_store::plan::DirectSetPlan;
use crate::pipeline::direct_store::router::{DirectDestination, RoutedSpan, RouterResources};
use crate::pipeline::extraction::catalog::decoded::DecodedCatalog;
use crate::pipeline::extraction::output::channel::{Event, Frame, Generation};
use crate::pipeline::extraction::storage::{Destination, DestinationRegistry, WriteTicket};
use crate::pipeline::extraction::{ExtractionRoot, JobExtractionBudget};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

// Spans one blocking write task places. Each holds a pending registry ticket
// until the task returns, so a batch's tickets are a small share of the
// registry's pending write cap and its metadata admission.
const PLACEMENT_BATCH: usize = 256;

// Partials one write task keeps open. A frame's spans usually land in one or
// two partials; the bound holds a frame that touches many members to a fixed
// number of handles.
pub(super) const OPEN_PARTIALS: usize = 16;

// A namespace's private backings live under the job's direct-store
// directory, named by the stream that owns them so no two generations share
// a backing.
pub(crate) fn private_root(working: &Path, owner: StreamOwner) -> PathBuf {
    working
        .join(crate::pipeline::direct_store::manifest::DIRECTORY)
        .join(format!(
            "namespace-{}-{}-{}",
            owner.root, owner.node, owner.generation
        ))
}

// The name the coordinator knows a decoded namespace by.
pub(crate) fn namespace_name(owner: StreamOwner) -> String {
    format!("decoded-{}-{}", owner.node, owner.generation)
}

pub(crate) struct NamespaceProgress {
    pub(crate) producers: Vec<crate::pipeline::direct_store::set::ProducerState>,
    pub(crate) children: Vec<crate::pipeline::direct_store::set::LiveChildDescription>,
}

pub(crate) struct DecodedState {
    pub(crate) job_id: crate::jobs::ids::JobId,
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
        job_id: crate::jobs::ids::JobId,
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
            job_id,
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

    // The graph and the namespace it routes under, borrowed apart so the
    // graph can change while it reads the namespace.
    fn graph_and_namespace(&mut self) -> (&mut NestedGraph, DecodedNamespace<'_>) {
        (
            &mut self.graph,
            DecodedNamespace {
                catalog: &self.catalog,
                plan: &self.plan,
                resources: &self.resources,
                destinations: &self.destinations,
            },
        )
    }

    pub(crate) async fn publish_progress(
        &mut self,
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
            job_id: self.job_id,
            owner,
            namespace: namespace_name(owner),
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
                self.registry
                    .register(&self.sessions, destination, &relative, *length)
                    .map_err(RuntimeError::session("physical registration"))?;
                // A decompressed stream states its length only by ending, so
                // its write is ticketed when it completes.
                let ticket = length
                    .map(|length| {
                        self.registry
                            .begin_write(&self.sessions, destination, 0, length)
                    })
                    .transpose()
                    .map_err(RuntimeError::session("physical ticket"))?;
                self.physical.insert(member, (destination, ticket));
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
                // A second completion finds the file sealed by the first, and
                // the registry refuses to write it again.
                let ticket = match ticket.take() {
                    Some(ticket) => ticket,
                    None => self
                        .registry
                        .begin_write(&self.sessions, *destination, 0, length)
                        .map_err(RuntimeError::session("physical ticket"))?,
                };
                self.registry
                    .placed(&self.sessions, ticket)
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
        let (graph, namespace) = self.graph_and_namespace();
        let writes = graph
            .forward(&namespace, ROOT_NODE, spans)
            .map_err(RuntimeError::routing("decoded forwarding"))?;
        // Nothing under a decoder can be read out of order: a child that
        // asks for bytes ahead of the stream is refused into an engine here,
        // and the seeks themselves have nowhere to go.
        graph.refuse_unseekable(&namespace);
        // Routing retention owns its own holds/scratch admission. Release the
        // bridge frame only after that transfer; emitted writes still own Bytes.
        drop(frame.lease);
        self.place(writes).await?;
        self.materialize_pending().await
    }

    fn create_dataless_outputs(&mut self) -> Result<(), RuntimeError> {
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

    // Places the spans the graph emitted. A frame's spans share one blocking
    // write task, so the actor waits on the file system once per frame, not
    // once per span. Spans before the first refusal land and settle, as they
    // would one at a time; a batch holds its tickets together, so a registry
    // at its admission edge refuses a span one at a time would have admitted.
    pub(super) async fn place(&mut self, writes: Vec<NodeSpan>) -> Result<(), RuntimeError> {
        let mut writes = writes.into_iter().peekable();
        while writes.peek().is_some() {
            if self.generation.is_revoked() {
                return Err(RuntimeError::Revoked);
            }
            let mut batch = PlacementBatch::default();
            let mut refused = None;
            for write in writes.by_ref().take(PLACEMENT_BATCH) {
                if let Err(error) = self.admit(write, &mut batch) {
                    refused = Some(error);
                    break;
                }
            }
            self.write_batch(batch).await?;
            if let Some(error) = refused {
                return Err(error);
            }
        }
        Ok(())
    }

    // Registers the span's partial and takes the ticket for its range.
    fn admit(&mut self, write: NodeSpan, batch: &mut PlacementBatch) -> Result<(), RuntimeError> {
        let receipt = write.placement();
        let relative = self.partial_of(&write, receipt)?;
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
        batch.writes.push(SpanWrite {
            relative,
            offset: write.span.destination_offset,
            bytes: write.span.bytes,
        });
        batch.admitted.push((receipt, ticket));
        Ok(())
    }

    // The span's partial, relative to the namespace's private root.
    fn partial_of(
        &self,
        write: &NodeSpan,
        receipt: NodePlacement,
    ) -> Result<PathBuf, RuntimeError> {
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
        path.strip_prefix(&self.base)
            .map(Path::to_owned)
            .map_err(|_| RuntimeError::Invariant("decoded output escaped private root"))
    }

    // Writes one batch on a blocking task, then settles each span the task
    // landed. Every ticket it did not land is released: the task has stopped,
    // so no released range can still be receiving bytes.
    async fn write_batch(&mut self, batch: PlacementBatch) -> Result<(), RuntimeError> {
        let PlacementBatch { writes, admitted } = batch;
        if writes.is_empty() {
            return Ok(());
        }
        let root = Arc::clone(&self.root);
        let generation = self.generation.clone();
        let (landed, failure) = match tokio::task::spawn_blocking(move || {
            write_spans(&root, &generation, writes)
        })
        .await
        {
            Ok(written) => written,
            Err(error) => (0, Some(RuntimeError::join("decoded write")(error))),
        };
        let mut admitted = admitted.into_iter();
        let settled = admitted
            .by_ref()
            .take(landed)
            .try_for_each(|(receipt, ticket)| self.settle(receipt, ticket));
        for (_, ticket) in admitted {
            let _ = self.registry.failed(ticket);
        }
        settled?;
        failure.map_or(Ok(()), Err)
    }

    fn settle(&mut self, receipt: NodePlacement, ticket: WriteTicket) -> Result<(), RuntimeError> {
        self.registry
            .placed(&self.sessions, ticket)
            .map_err(RuntimeError::session("decoded placement"))?;
        let (graph, namespace) = self.graph_and_namespace();
        graph
            .record_placement(&namespace, receipt)
            .map_err(RuntimeError::routing("decoded receipt"))?;
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<(), RuntimeError> {
        self.catalog
            .close()
            .map_err(RuntimeError::io("decoded catalog close"))?;
        let (graph, namespace) = self.graph_and_namespace();
        let writes = graph
            .close_parent_roster(&namespace, ROOT_NODE)
            .map_err(RuntimeError::routing("decoded roster"))?;
        // See [`Self::accept`].
        graph.refuse_unseekable(&namespace);
        self.place(writes).await?;
        self.materialize_pending().await?;
        self.create_dataless_outputs()
    }

    // Routing and the unseekable refusal that runs just before this call are
    // what move a node out of direct mode, so the nodes that owe their parent
    // member bytes now are all the work this call has. Reading their modes
    // spares preparing every node of the graph per frame.
    async fn materialize_pending(&mut self) -> Result<(), RuntimeError> {
        let pending: Vec<_> = self
            .graph
            .nodes
            .iter()
            .zip(1..)
            .filter_map(|(node, id)| {
                matches!(
                    node.mode,
                    NodeMode::MaterializationPending(_) | NodeMode::Engine
                )
                .then_some(id)
            })
            .collect();
        for id in pending {
            let namespace = self.namespace();
            let Some(work) = self.graph.prepare_materialization(&namespace, id) else {
                continue;
            };
            let reason = work.materializes();
            self.materialize(work).await?;
            // A streamed child and a materialized one deliver identical bytes,
            // so this line is the only evidence outside the process of which
            // it was. It follows the copy: a refused one never happened.
            if let Some(reason) = reason {
                tracing::info!(
                    job_id = self.job_id.0,
                    set_name = %self.plan.set_name,
                    node = id,
                    reason = reason.metric(),
                    "direct-store child archive materialized as a file"
                );
            }
        }
        Ok(())
    }

    // Copies a pending node's input out as its parent's member and settles
    // the tickets for it. Every ticket the copy admitted is placed or
    // released before this returns: one left pending would hold its range
    // against every later write and keep the registry from an inventory.
    async fn materialize(&mut self, work: Materialization) -> Result<(), RuntimeError> {
        let mut tickets = Vec::new();
        let copied = self.copy_materialization(work, &mut tickets).await;
        let mut tickets = tickets.into_iter();
        let placed = copied.and_then(|()| {
            tickets.by_ref().try_for_each(|ticket| {
                self.registry
                    .placed(&self.sessions, ticket)
                    .map(drop)
                    .map_err(RuntimeError::session("materialization placement"))
            })
        });
        // The copy task has stopped, so no released range is still being
        // written.
        for ticket in tickets {
            let _ = self.registry.failed(ticket);
        }
        placed
    }

    // Admits the copy's writes into `tickets`, runs the copy on a blocking
    // task and applies its result to the graph.
    async fn copy_materialization(
        &mut self,
        work: Materialization,
        tickets: &mut Vec<WriteTicket>,
    ) -> Result<(), RuntimeError> {
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
        let (graph, namespace) = self.graph_and_namespace();
        graph
            .apply_materialization(&namespace, result)
            .map_err(RuntimeError::routing("materialization apply"))?;
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
            if let Some((child, _)) = self.graph.binding(owner, id) {
                match self.graph.node(child).mode {
                    // The file it landed as is this member.
                    NodeMode::Materialized => {}
                    // Its engine publishes what it holds.
                    NodeMode::Engine => continue,
                    NodeMode::Direct | NodeMode::MaterializationPending(_) => {
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
                }
            }
            if output.insert(name.clone(), path).is_some() {
                return Err(RuntimeError::OutputCollision(name));
            }
        }
        Ok(())
    }
}

// Spans admitted for one write task: what the task writes, and the receipts
// and tickets the actor settles afterwards, in the same order.
#[derive(Default)]
struct PlacementBatch {
    writes: Vec<SpanWrite>,
    admitted: Vec<(NodePlacement, WriteTicket)>,
}

pub(super) struct SpanWrite {
    pub(super) relative: PathBuf,
    pub(super) offset: u64,
    pub(super) bytes: Vec<bytes::Bytes>,
}

// Writes spans in order, stopping at the first failure or once the generation
// is revoked, and returns how many landed so the actor settles exactly those.
pub(super) fn write_spans(
    root: &ExtractionRoot,
    generation: &Generation,
    writes: Vec<SpanWrite>,
) -> (usize, Option<RuntimeError>) {
    let count = writes.len();
    let mut partials = OpenPartials::default();
    for (landed, write) in writes.into_iter().enumerate() {
        if generation.is_revoked() {
            return (landed, Some(RuntimeError::Revoked));
        }
        if let Err(error) = partials.write(root, write) {
            return (landed, Some(RuntimeError::io("decoded write")(error)));
        }
    }
    (count, None)
}

// Write handles on member partials, least recently used first. Opening a
// partial walks the private root one directory at a time, so a span that
// reuses a handle saves that walk.
#[derive(Default)]
pub(super) struct OpenPartials {
    files: Vec<(PathBuf, cap_std::fs::File)>,
}

impl OpenPartials {
    pub(super) fn write(&mut self, root: &ExtractionRoot, write: SpanWrite) -> io::Result<()> {
        let file = self.open(root, &write.relative)?;
        file.seek(SeekFrom::Start(write.offset))?;
        for bytes in write.bytes {
            file.write_all(&bytes)?;
        }
        file.flush()
    }

    // Opening past the bound closes the least recently used handle first.
    fn open(
        &mut self,
        root: &ExtractionRoot,
        relative: &Path,
    ) -> io::Result<&mut cap_std::fs::File> {
        match self.files.iter().position(|(path, _)| path == relative) {
            Some(index) => self.files[index..].rotate_left(1),
            None => {
                let file = root.open_partial(relative)?;
                if self.files.len() == OPEN_PARTIALS {
                    self.files.remove(0);
                }
                self.files.push((relative.to_owned(), file));
            }
        }
        let newest = self.files.len() - 1;
        Ok(&mut self.files[newest].1)
    }

    #[cfg(test)]
    pub(super) fn holds(&self, relative: &Path) -> bool {
        self.files.iter().any(|(path, _)| path == relative)
    }
}
