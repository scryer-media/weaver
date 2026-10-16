use crate::pipeline::direct_store::plan::{DirectSetPlan, SetFormat};
use crate::pipeline::extraction::output::channel::StreamOwner;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

impl DirectSetPlan {
    // The plan a decoded namespace routes under. A namespace is not a posted
    // set, but the router only runs under a set plan, so the namespace poses
    // as a 7z set of one volume carried by one file: volume 0 is file 0,
    // there is no identity and nothing unposted. The format and the volume
    // are placeholders; the namespace's real topology lives in its catalog
    // and graph. Both directories are the namespace's private root, so
    // nothing it writes lands in the job's shared directories.
    pub(crate) fn namespace(owner: StreamOwner, private_root: &Path) -> Self {
        Self::restored_namespace(format!("namespace-{}", owner.node), private_root)
    }

    // The same plan under the set name a checkpoint recorded for it.
    pub(crate) fn restored_namespace(set_name: String, private_root: &Path) -> Self {
        Self {
            set_name,
            format: SetFormat::SevenZip,
            volumes: BTreeMap::from([(0, 0)]),
            files: HashMap::from([(0, 0)]),
            identity: None,
            unposted: Default::default(),
            working_dir: private_root.to_owned(),
            destination_dir: private_root.to_owned(),
        }
    }
}
