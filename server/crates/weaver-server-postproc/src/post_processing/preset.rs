// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::post_processing::database::PostprocDatabase;
// What a script's header offers as a starting point for an instance.
//
// A preset fills a form and seeds the instances a header asks for. It is read
// from the script each time it is wanted and is never saved: once an instance
// exists, editing the header changes nothing about it.

use crate::post_processing::instances::PostProcessingInstancesQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use std::collections::{BTreeMap, BTreeSet};

use super::instances::{
    InstanceInputDraft, InstanceSchedule, InstanceTrigger, ScriptInstance, ScriptInstanceDraft,
    ScriptInstanceError,
};
use super::listing::{DiscoveredScript, ListingError, resolve_script};
use super::model::{OptionValue, ScriptKind, ScriptManifest, ScriptName, ScriptTaskTime};
use super::runner::option_value_text;
use weaver_server_db::persistence::StateError;

// One input a header declares.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PresetInput {
    pub name: String,
    // The header's default. Always empty for a secret.
    pub value: String,
    // The slot takes a secret rather than a plain value.
    pub secret: bool,
}

// The triggers, run times and inputs a script's header declares.
#[derive(Debug, Clone, Default)]
pub struct ScriptPreset {
    // One per instance the header asks for.
    pub triggers: Vec<InstanceTrigger>,
    // When a schedule instance is meant to run.
    pub task_times: Vec<ScriptTaskTime>,
    // Every declared input at its default. A secret is never pre-filled.
    pub inputs: Vec<PresetInput>,
}

// Secret links already saved for a script, by upper-cased input name, so a
// new instance of it starts linked to the same secrets.
pub type SecretLinks = BTreeMap<String, String>;

impl ScriptPreset {
    pub fn of(manifest: &ScriptManifest) -> Self {
        let mut triggers = Vec::new();
        for kind in manifest.kinds() {
            match kind {
                ScriptKind::PostProcessing => triggers.push(InstanceTrigger::PostProcessing),
                ScriptKind::Queue => triggers.extend(
                    manifest
                        .queue_events()
                        .iter()
                        .copied()
                        .map(InstanceTrigger::Queue),
                ),
                ScriptKind::Scan => triggers.push(InstanceTrigger::Scan),
                ScriptKind::Scheduler => triggers.push(InstanceTrigger::Schedule),
                ScriptKind::Feed => triggers.push(InstanceTrigger::Feed),
            }
        }
        let inputs = manifest
            .options()
            .iter()
            .map(|option| {
                let secret = option.is_secret();
                PresetInput {
                    name: option.name().as_str().to_string(),
                    value: match option.default() {
                        _ if secret => String::new(),
                        Some(OptionValue::Secret(_)) | None => String::new(),
                        Some(value) => option_value_text(value),
                    },
                    secret,
                }
            })
            .collect();
        Self {
            triggers,
            task_times: manifest.task_times().to_vec(),
            inputs,
        }
    }

    // Whether the header declares nothing an instance could be filled from.
    pub fn is_empty(&self) -> bool {
        self.triggers.is_empty() && self.inputs.is_empty()
    }

    // Whether the header's inputs no longer line up with what `instance` has
    // saved: an input it declares is missing, one the instance carries is no
    // longer declared, or one changed between secret and plain.
    //
    // Values are the operator's and never count. Nor does a secret that has
    // not been given a value yet, since there is nothing a header could fill
    // in for it. A secret of the instance's own counts as a secret, the same
    // as a linked one. The trigger is left out as well: any script may be
    // wired to any trigger, so one the header does not name is a choice, not
    // drift.
    pub fn drifted_from(&self, instance: &ScriptInstance) -> bool {
        let saved = |name: &str| {
            instance
                .inputs
                .iter()
                .find(|input| input.name.as_str().eq_ignore_ascii_case(name))
        };
        let declared = |name: &str| {
            self.inputs
                .iter()
                .any(|input| input.name.eq_ignore_ascii_case(name))
        };
        self.inputs.iter().any(|input| match saved(&input.name) {
            Some(saved) => saved.is_secret() != input.secret,
            None => !input.secret,
        }) || instance
            .inputs
            .iter()
            .any(|input| !declared(input.name.as_str()))
    }

    // `instance` with its inputs brought back in line with the header: every
    // declared input, at the value already saved under that name or else its
    // default, and nothing the header no longer declares.
    //
    // A secret slot keeps the secret it is linked to. One with no link yet
    // is left out until the operator picks a secret for it. A plain value
    // saved for an input the header now calls secret is kept as it is, so the
    // run still gets it and the drift stays visible. An input that went from
    // a linked secret to plain starts again from the header's default,
    // because a stored secret is never turned back into plain text.
    //
    // A secret of the instance's own is kept, sealed as it is, under any
    // name the header still declares: it exists nowhere else, so dropping it
    // would lose it. Where the header calls that input plain, the drift
    // stays visible.
    pub fn reapplied_to(&self, instance: &ScriptInstance) -> ScriptInstanceDraft {
        let mut draft = ScriptInstanceDraft::from_instance(instance);
        draft.inputs = self
            .inputs
            .iter()
            .filter_map(|declared| {
                let saved = instance
                    .inputs
                    .iter()
                    .find(|input| input.name.as_str().eq_ignore_ascii_case(&declared.name));
                match (saved, declared.secret) {
                    (Some(saved), _) if saved.sealed => {
                        Some(InstanceInputDraft::kept_sealed(&declared.name))
                    }
                    (Some(saved), true) => match saved.secret.as_ref() {
                        Some(secret) => {
                            Some(InstanceInputDraft::secret(&declared.name, &secret.id))
                        }
                        None => Some(InstanceInputDraft::plain(&declared.name, &saved.value)),
                    },
                    (None, true) => None,
                    (Some(saved), false) if !saved.is_secret() => {
                        Some(InstanceInputDraft::plain(&declared.name, &saved.value))
                    }
                    (_, false) => Some(InstanceInputDraft::plain(&declared.name, &declared.value)),
                }
            })
            .collect();
        draft
    }

    // A new instance of `script` on `trigger`, filled from the header. A
    // secret slot is linked when `links` has a secret for it, and left out
    // otherwise. A schedule job takes the run times the header declares.
    pub fn draft(
        &self,
        script: &ScriptName,
        trigger: InstanceTrigger,
        links: &SecretLinks,
    ) -> ScriptInstanceDraft {
        let mut draft = ScriptInstanceDraft::new(script.clone(), trigger);
        draft.inputs = self
            .inputs
            .iter()
            .filter_map(|input| {
                if input.secret {
                    links
                        .get(&input.name.to_ascii_uppercase())
                        .map(|id| InstanceInputDraft::secret(&input.name, id))
                } else {
                    Some(InstanceInputDraft::plain(&input.name, &input.value))
                }
            })
            .collect();
        if trigger == InstanceTrigger::Schedule {
            draft.schedule = InstanceSchedule::from_task_times(&self.task_times);
        }
        draft
    }
}

// The secrets the instances of one script already link, by input name. The
// first instance in run order wins where two differ.
pub fn secret_links_of<'a>(instances: impl IntoIterator<Item = &'a ScriptInstance>) -> SecretLinks {
    let mut links = SecretLinks::new();
    for instance in instances {
        for input in &instance.inputs {
            if let Some(secret) = &input.secret {
                links
                    .entry(input.name.as_str().to_ascii_uppercase())
                    .or_insert_with(|| secret.id.clone());
            }
        }
    }
    links
}

// What setting a script up from its header added.
#[derive(Debug, Clone, Default)]
pub struct HeaderSetup {
    pub instances: Vec<ScriptInstance>,
}

#[derive(Debug, thiserror::Error)]
pub enum HeaderSetupError {
    #[error("{0}")]
    Script(ListingError),
    #[error("the script's header declares nothing to set up")]
    NothingDeclared,
    #[error(transparent)]
    Instance(#[from] ScriptInstanceError),
    #[error(transparent)]
    Storage(#[from] StateError),
}

trait PostProcessingPresetLocalQueries {
    fn discovered_script(&self, script: &ScriptName) -> Result<DiscoveredScript, HeaderSetupError>;
}

impl<D: PostprocDatabase> PostProcessingPresetLocalQueries for D {
    fn discovered_script(&self, script: &ScriptName) -> Result<DiscoveredScript, HeaderSetupError> {
        resolve_script(&self.post_processing_script_directory()?, script)
            .map_err(HeaderSetupError::Script)
    }
}

pub trait PostProcessingPresetQueries {
    fn set_up_script_from_header(
        &self,
        script: &ScriptName,
    ) -> Result<HeaderSetup, HeaderSetupError>;
    fn reapply_script_header(&self, id: &str) -> Result<ScriptInstance, HeaderSetupError>;
}

impl<D: PostprocDatabase> PostProcessingPresetQueries for D {
    // Create every instance a script's header asks for and does not have
    // yet: one per declared trigger, each filled from the header. A new
    // schedule job runs at the times the header declares.
    //
    // Running it again adds only what is still missing, so a trigger the
    // header gained later can be picked up without touching what is saved.
    fn set_up_script_from_header(
        &self,
        script: &ScriptName,
    ) -> Result<HeaderSetup, HeaderSetupError> {
        let discovered = self.discovered_script(script)?;
        let preset = ScriptPreset::of(&discovered.manifest);
        if preset.triggers.is_empty() {
            return Err(HeaderSetupError::NothingDeclared);
        }
        let siblings = self
            .script_instances()?
            .into_iter()
            .filter(|instance| &instance.script == script)
            .collect::<Vec<_>>();
        let wired = siblings
            .iter()
            .map(|instance| instance.trigger)
            .collect::<BTreeSet<_>>();
        let links = secret_links_of(&siblings);
        let mut setup = HeaderSetup::default();
        for trigger in preset.triggers.iter().copied() {
            if wired.contains(&trigger) {
                continue;
            }
            let instance = self.create_script_instance(preset.draft(script, trigger, &links))?;
            setup.instances.push(instance);
        }
        Ok(setup)
    }

    // Bring one instance's inputs back in line with its script's header.
    fn reapply_script_header(&self, id: &str) -> Result<ScriptInstance, HeaderSetupError> {
        let instance = self
            .script_instance(id)?
            .ok_or(ScriptInstanceError::NotFound)?;
        let discovered = self.discovered_script(&instance.script)?;
        let preset = ScriptPreset::of(&discovered.manifest);
        Ok(self.update_script_instance(id, preset.reapplied_to(&instance))?)
    }
}
