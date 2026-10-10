// What a script's header offers as a starting point for an instance.
//
// A preset fills a form and seeds the instances a header asks for. It is read
// from the script each time it is wanted and is never saved: once an instance
// exists, editing the header changes nothing about it.

use std::collections::{BTreeMap, BTreeSet};

use super::instances::{
    InstanceInputDraft, InstanceSchedule, InstanceTrigger, ScriptInstance, ScriptInstanceDraft,
    ScriptInstanceError,
};
use super::listing::{DiscoveredScript, resolve_script};
use super::model::{OptionValue, ScriptKind, ScriptManifest, ScriptName, ScriptTaskTime};
use super::runner::option_value_text;
use crate::persistence::{Database, StateError};

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
    Script(String),
    #[error("the script's header declares nothing to set up")]
    NothingDeclared,
    #[error(transparent)]
    Instance(#[from] ScriptInstanceError),
    #[error(transparent)]
    Storage(#[from] StateError),
}

impl Database {
    fn discovered_script(&self, script: &ScriptName) -> Result<DiscoveredScript, HeaderSetupError> {
        resolve_script(&self.post_processing_script_directory()?, script)
            .map_err(|error| HeaderSetupError::Script(error.to_string()))
    }

    // Create every instance a script's header asks for and does not have
    // yet: one per declared trigger, each filled from the header. A new
    // schedule job runs at the times the header declares.
    //
    // Running it again adds only what is still missing, so a trigger the
    // header gained later can be picked up without touching what is saved.
    pub fn set_up_script_from_header(
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
    pub fn reapply_script_header(&self, id: &str) -> Result<ScriptInstance, HeaderSetupError> {
        let instance = self
            .script_instance(id)?
            .ok_or(ScriptInstanceError::NotFound)?;
        let discovered = self.discovered_script(&instance.script)?;
        let preset = ScriptPreset::of(&discovered.manifest);
        Ok(self.update_script_instance(id, preset.reapplied_to(&instance))?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::instances::InstanceInput;
    use super::super::model::OptionName;
    use super::super::secrets::SecretRef;
    use super::*;

    fn declared(name: &str, value: Option<&str>, secret: bool) -> PresetInput {
        PresetInput {
            name: name.into(),
            value: value.unwrap_or_default().into(),
            secret,
        }
    }

    // A saved input; a secret one is linked to the secret `value` names.
    fn saved(name: &str, value: &str, secret: bool) -> InstanceInput {
        InstanceInput {
            name: OptionName::new(name).unwrap(),
            value: if secret { String::new() } else { value.into() },
            secret: secret.then(|| SecretRef {
                id: value.into(),
                name: value.into(),
            }),
            sealed: false,
        }
    }

    // A saved input that is a secret of the instance's own.
    fn sealed(name: &str) -> InstanceInput {
        InstanceInput {
            name: OptionName::new(name).unwrap(),
            value: String::new(),
            secret: None,
            sealed: true,
        }
    }

    fn instance(inputs: Vec<InstanceInput>) -> ScriptInstance {
        ScriptInstance {
            id: "one".into(),
            name: "one".into(),
            script: ScriptName::new("notify.sh").unwrap(),
            trigger: InstanceTrigger::Scan,
            inputs,
            categories: Vec::new(),
            enabled: true,
            blocking: true,
            timeout_seconds: None,
            run_order: 0,
            schedule: InstanceSchedule::default(),
        }
    }

    fn preset(inputs: Vec<PresetInput>) -> ScriptPreset {
        ScriptPreset {
            triggers: vec![InstanceTrigger::PostProcessing],
            task_times: Vec::new(),
            inputs,
        }
    }

    #[test]
    fn a_changed_value_or_a_secret_not_yet_given_is_not_drift() {
        let header = preset(vec![
            declared("Server", Some("localhost"), false),
            declared("Token", None, true),
        ]);
        assert!(!header.drifted_from(&instance(vec![saved("server", "example.test", false)])));
        assert!(!header.drifted_from(&instance(vec![
            saved("Server", "example.test", false),
            saved("Token", "token-secret", true),
        ])));
    }

    #[test]
    fn a_missing_extra_or_reclassified_input_is_drift() {
        let header = preset(vec![declared("Server", Some("localhost"), false)]);
        assert!(header.drifted_from(&instance(Vec::new())));
        assert!(header.drifted_from(&instance(vec![
            saved("Server", "localhost", false),
            saved("Port", "80", false),
        ])));
        assert!(header.drifted_from(&instance(vec![saved("Server", "", true)])));
        // A trigger the header does not name is a choice the operator made.
        assert!(!header.drifted_from(&instance(vec![saved("Server", "localhost", false)])));
    }

    #[test]
    fn reapplying_keeps_saved_values_and_drops_what_the_header_dropped() {
        let header = preset(vec![
            declared("Server", Some("localhost"), false),
            declared("Port", Some("80"), false),
            declared("Token", None, true),
            declared("Mode", Some("fast"), false),
            declared("Password", None, true),
            declared("ApiKey", None, true),
        ]);
        let before = instance(vec![
            saved("server", "example.test", false),
            saved("Token", "token-secret", true),
            saved("Mode", "mode-secret", true),
            saved("ApiKey", "typed in clear", false),
            saved("Gone", "x", false),
        ]);
        let inputs = header.reapplied_to(&before).inputs;
        assert_eq!(
            inputs,
            [
                InstanceInputDraft::plain("Server", "example.test"),
                InstanceInputDraft::plain("Port", "80"),
                // The link is kept as it was.
                InstanceInputDraft::secret("Token", "token-secret"),
                // Was a secret, is plain now: starts again from the header,
                // and the secret's value is never what fills it.
                InstanceInputDraft::plain("Mode", "fast"),
                // A secret slot with no link yet is left for the operator to
                // link; one whose saved value was plain keeps that value.
                InstanceInputDraft::plain("ApiKey", "typed in clear"),
            ]
        );
    }

    #[test]
    fn reapplying_keeps_a_plain_value_the_header_now_calls_secret_and_still_shows_drift() {
        let header = preset(vec![declared("ApiKey", None, true)]);
        let before = instance(vec![saved("apikey", "typed in clear", false)]);
        let inputs = header.reapplied_to(&before).inputs;
        assert_eq!(
            inputs,
            [InstanceInputDraft::plain("ApiKey", "typed in clear")]
        );

        // Saved back, the input is still plain where the header wants a
        // secret, so the drift stays visible until the operator links one.
        let after = instance(vec![saved("ApiKey", "typed in clear", false)]);
        assert!(header.drifted_from(&after));

        // A linked secret is never turned into a plain value, whichever way
        // the header classifies the option.
        let linked = instance(vec![saved("ApiKey", "stored", true)]);
        assert_eq!(
            header.reapplied_to(&linked).inputs,
            [InstanceInputDraft::secret("ApiKey", "stored")]
        );
        let now_plain = preset(vec![declared("ApiKey", Some("default"), false)]);
        assert_eq!(
            now_plain.reapplied_to(&linked).inputs,
            [InstanceInputDraft::plain("ApiKey", "default")]
        );
    }

    #[test]
    fn a_secret_of_the_instances_own_drifts_exactly_as_a_linked_one_does() {
        let header = preset(vec![
            declared("Server", Some("localhost"), false),
            declared("Token", None, true),
        ]);
        // On a secret slot it is what the header asks for.
        assert!(!header.drifted_from(&instance(vec![
            saved("Server", "example.test", false),
            sealed("token"),
        ])));
        // On an input the header calls plain, the two no longer line up.
        assert!(header.drifted_from(&instance(vec![sealed("Server")])));
        assert_eq!(
            header.drifted_from(&instance(vec![sealed("Server")])),
            header.drifted_from(&instance(vec![saved("Server", "linked", true)])),
        );
        // Under a name the header does not declare it is one more extra.
        assert!(header.drifted_from(&instance(vec![
            saved("Server", "example.test", false),
            sealed("Extra"),
        ])));
    }

    #[test]
    fn reapplying_keeps_a_secret_of_the_instances_own_under_a_declared_name_only() {
        let header = preset(vec![
            declared("Server", Some("localhost"), false),
            declared("Token", None, true),
            declared("Mode", Some("fast"), false),
        ]);
        let before = instance(vec![sealed("token"), sealed("MODE"), sealed("Gone")]);
        let inputs = header.reapplied_to(&before).inputs;
        assert_eq!(
            inputs,
            [
                InstanceInputDraft::plain("Server", "localhost"),
                // Kept as it is stored: no value is carried, so none is needed.
                InstanceInputDraft::kept_sealed("Token"),
                // Kept under a plain slot too, where the drift stays visible.
                InstanceInputDraft::kept_sealed("Mode"),
            ]
        );
        assert!(
            inputs
                .iter()
                .all(|input| !input.sealed || input.value.is_none())
        );
        let after = instance(vec![
            saved("Server", "localhost", false),
            sealed("Token"),
            sealed("Mode"),
        ]);
        assert!(header.drifted_from(&after));

        // A new instance of the same script is never handed a sibling's own
        // secret: only links are shared.
        let links = secret_links_of([&before]);
        assert!(links.is_empty());
        assert_eq!(
            header
                .draft(
                    &ScriptName::new("notify.sh").unwrap(),
                    InstanceTrigger::PostProcessing,
                    &links,
                )
                .inputs,
            [
                InstanceInputDraft::plain("Server", "localhost"),
                InstanceInputDraft::plain("Mode", "fast"),
            ]
        );
    }

    #[test]
    fn a_new_instance_starts_linked_to_the_secrets_its_siblings_use() {
        let header = preset(vec![
            declared("Server", Some("localhost"), false),
            declared("Token", None, true),
            declared("Password", None, true),
        ]);
        let sibling = instance(vec![saved("token", "shared", true)]);
        let links = secret_links_of([&sibling]);
        let draft = header.draft(
            &ScriptName::new("notify.sh").unwrap(),
            InstanceTrigger::PostProcessing,
            &links,
        );
        assert_eq!(
            draft.inputs,
            [
                InstanceInputDraft::plain("Server", "localhost"),
                InstanceInputDraft::secret("Token", "shared"),
            ]
        );
    }

    #[test]
    fn header_setup_and_reapply_keep_a_secret_link() {
        let db = Database::open_in_memory().unwrap();
        let root = tempfile::tempdir().unwrap();
        let scripts = crate::post_processing::settings::normalize_script_directory(
            &root.path().join("scripts"),
        )
        .unwrap();
        std::fs::create_dir_all(&scripts).unwrap();
        std::fs::write(
            scripts.join("notify.sh"),
            "#!/bin/sh\n\
             ### NZBGET POST-PROCESSING/QUEUE SCRIPT ###\n\
             ### QUEUE EVENTS: NZB_ADDED ###\n\
             ### OPTIONS ###\n\
             # Where to send it.\n\
             #Server=localhost\n\
             # The account's API token.\n\
             #ApiToken=do-not-use\n\
             ### NZBGET POST-PROCESSING/QUEUE SCRIPT ###\n",
        )
        .unwrap();
        db.replace_post_processing_script_directory(&scripts)
            .unwrap();
        let script = ScriptName::new("notify.sh").unwrap();

        let token = db.create_secret("Notify token", "first").unwrap();
        let first = db
            .create_script_instance(
                ScriptInstanceDraft::new(script.clone(), InstanceTrigger::PostProcessing)
                    .input("Server", "example.test")
                    .secret_input("ApiToken", &token.id),
            )
            .unwrap();

        // The queue instance is new, and starts linked to the same secret.
        let setup = db.set_up_script_from_header(&script).unwrap();
        assert_eq!(setup.instances.len(), 1);
        let added = &setup.instances[0];
        assert_eq!(
            added.inputs[1]
                .secret
                .as_ref()
                .map(|secret| secret.id.as_str()),
            Some(token.id.as_str())
        );
        // The header's default for the token is never offered.
        assert_eq!(added.inputs[0].value, "localhost");

        let reapplied = db.reapply_script_header(&first.id).unwrap();
        assert_eq!(reapplied.inputs, first.inputs);
    }

    #[test]
    fn reapplying_a_header_carries_a_secret_of_the_instances_own_across_sealed() {
        use super::super::model::OptionValue;

        let db = Database::open_in_memory().unwrap();
        let root = tempfile::tempdir().unwrap();
        let scripts = crate::post_processing::settings::normalize_script_directory(
            &root.path().join("scripts"),
        )
        .unwrap();
        std::fs::create_dir_all(&scripts).unwrap();
        std::fs::write(
            scripts.join("notify.sh"),
            "#!/bin/sh\n\
             ### NZBGET POST-PROCESSING SCRIPT ###\n\
             ### OPTIONS ###\n\
             # Where to send it.\n\
             #Server=localhost\n\
             # The account's API token.\n\
             #ApiToken=do-not-use\n\
             ### NZBGET POST-PROCESSING SCRIPT ###\n",
        )
        .unwrap();
        db.replace_post_processing_script_directory(&scripts)
            .unwrap();
        let script = ScriptName::new("notify.sh").unwrap();
        let saved = db
            .create_script_instance(
                ScriptInstanceDraft::new(script, InstanceTrigger::PostProcessing)
                    .sealed_input("apitoken", "own-token")
                    .sealed_input("Extra", "own-extra"),
            )
            .unwrap();

        let reapplied = db.reapply_script_header(&saved.id).unwrap();
        assert_eq!(
            reapplied
                .inputs
                .iter()
                .map(|input| (input.name.as_str(), input.value.as_str(), input.sealed))
                .collect::<Vec<_>>(),
            // The declared input keeps its secret, under the header's casing;
            // the one the header does not declare goes like any other.
            [("Server", "localhost", false), ("ApiToken", "", true)]
        );
        let run = db.script_instance_run_inputs(&saved.id).unwrap().unwrap();
        assert_eq!(run.len(), 2);
        assert!(matches!(
            run[1].value(),
            OptionValue::Secret(value) if value.expose_for_execution() == "own-token"
        ));
        // Nothing of it became a named secret along the way.
        assert!(db.secrets().unwrap().is_empty());
    }
}
