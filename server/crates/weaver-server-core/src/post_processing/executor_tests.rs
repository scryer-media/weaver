use super::executor::{PostProcessingExecutor, execution_refusal};
use super::instances::{
    InstanceInputDraft, InstanceTrigger, ScriptInstance, ScriptInstanceDraft, ScriptInstanceError,
};
use super::model::{
    GlobalScriptsRun, OptionValue, PostProcessingSettings, PostProcessingSummary, QueueEvent,
    ScriptAdapter, ScriptEventLabel, ScriptName, ScriptResult, ScriptStatus,
};
use super::secrets::SecretError;
use super::settings::normalize_script_directory;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

fn script(name: &str) -> ScriptName {
    ScriptName::new(name).unwrap()
}

fn draft(name: &str, trigger: InstanceTrigger) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(script(name), trigger)
}

fn names(instances: &[ScriptInstance]) -> Vec<&str> {
    instances
        .iter()
        .map(|instance| instance.name.as_str())
        .collect()
}

/// The stored value of one input, exactly as the row holds it.
fn stored_input(db: &Database, instance: &str, name: &str) -> Option<String> {
    let datastore = db.datastore();
    let (instance, name) = (instance.to_string(), name.to_string());
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_optional(
            datastore.read_exec(),
            "SELECT value FROM script_instance_inputs WHERE instance_id = {} AND name = {}",
            &[SqlArg::Text(instance), SqlArg::Text(name)],
        )
        .await?
        .map(|row| row.text("value"))
        .transpose()
    })
    .unwrap()
}

fn refused(db: &Database, draft: ScriptInstanceDraft) -> &'static str {
    match db.create_script_instance(draft) {
        Err(ScriptInstanceError::Invalid(reason)) => reason,
        other => panic!("expected the instance to be refused, got {other:?}"),
    }
}

#[test]
fn execution_is_refused_while_disabled_and_under_strict_security() {
    let mut settings = PostProcessingSettings::default();
    assert_eq!(
        execution_refusal(&settings, false),
        Some("post-processing script execution is disabled")
    );
    settings.execution_enabled = true;
    assert_eq!(execution_refusal(&settings, false), None);
    // Strict security refuses even when the operator turned execution on, and
    // does so at run time rather than as a startup time bomb.
    assert_eq!(
        execution_refusal(&settings, true),
        Some("WEAVER_STRICT_SECURITY=1 refuses post-processing script execution")
    );
    settings.execution_enabled = false;
    assert_eq!(
        execution_refusal(&settings, true),
        Some("WEAVER_STRICT_SECURITY=1 refuses post-processing script execution")
    );
}

#[test]
fn settings_round_trip_through_the_settings_kv() {
    let db = Database::open_in_memory().unwrap();

    assert_eq!(
        db.post_processing_settings().unwrap(),
        PostProcessingSettings::default()
    );
    assert_eq!(
        PostProcessingSettings::default().global_scripts_run,
        GlobalScriptsRun::Always
    );
    let settings = PostProcessingSettings {
        execution_enabled: true,
        concurrency: 4,
        termination_grace_seconds: 15,
        python_interpreter: Some("/usr/bin/python3".into()),
        global_scripts_run: GlobalScriptsRun::OnlyWithoutCategoryScripts,
        ..PostProcessingSettings::default()
    };
    db.save_post_processing_settings(&settings).unwrap();
    assert_eq!(db.post_processing_settings().unwrap(), settings);
}

/// What each input of an instance shows: its name, its plain value, and the
/// name of the secret it links.
fn shown(instance: &ScriptInstance) -> Vec<(&str, &str, Option<&str>)> {
    instance
        .inputs
        .iter()
        .map(|input| {
            (
                input.name.as_str(),
                input.value.as_str(),
                input.secret.as_ref().map(|secret| secret.name.as_str()),
            )
        })
        .collect()
}

/// The value a run of `instance` is handed for input `name`, secrets included.
fn run_value(db: &Database, instance: &str, name: &str) -> String {
    let inputs = db.script_instance_run_inputs(instance).unwrap().unwrap();
    match inputs
        .iter()
        .find(|input| input.name().as_str() == name)
        .unwrap()
        .value()
    {
        OptionValue::Secret(value) => value.expose_for_execution().to_string(),
        OptionValue::String(value) => value.clone(),
        other => panic!("unexpected input value {other:?}"),
    }
}

/// The stored value of one secret, exactly as the row holds it.
fn stored_secret(db: &Database, id: &str) -> String {
    let datastore = db.datastore();
    let id = id.to_string();
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_optional(
            datastore.read_exec(),
            "SELECT value FROM secrets WHERE id = {}",
            &[SqlArg::Text(id)],
        )
        .await?
        .map(|row| row.text("value"))
        .transpose()
    })
    .unwrap()
    .unwrap()
}

#[test]
fn an_instance_reads_back_as_it_was_saved() {
    let db = Database::open_in_memory().unwrap();
    let token = db.create_secret("Family token", "hunter2").unwrap();
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::Queue(QueueEvent::NzbAdded))
                .named("  Tell the family  ")
                .input("Host", "mail.example.invalid")
                .secret_input("Token", &token.id)
                .input("Empty", "")
                .category(" Movies ")
                .category("movies")
                .category("TV")
                .fire_and_forget()
                .timeout(90),
        )
        .unwrap();

    assert_eq!(saved.name, "Tell the family");
    assert_eq!(saved.script, script("notify.sh"));
    assert_eq!(saved.trigger, InstanceTrigger::Queue(QueueEvent::NzbAdded));
    // The same category under another casing is one category.
    assert_eq!(saved.categories, ["Movies", "TV"]);
    assert!(saved.enabled);
    assert!(!saved.blocking);
    assert_eq!(saved.timeout_seconds, Some(90));
    assert_eq!(
        shown(&saved),
        // A secret's value is never handed back, only its name.
        [
            ("Host", "mail.example.invalid", None),
            ("Token", "", Some("Family token")),
            ("Empty", "", None),
        ]
    );
    assert_eq!(db.script_instances().unwrap(), std::slice::from_ref(&saved));
    assert_eq!(db.script_instance(&saved.id).unwrap(), Some(saved.clone()));
    assert_eq!(db.script_instance("missing").unwrap(), None);

    // The input row holds only the link; the secret holds the value, sealed.
    assert_eq!(stored_input(&db, &saved.id, "Token").unwrap(), "");
    assert!(
        !stored_secret(&db, &token.id).contains("hunter2"),
        "a secret must not be stored in clear"
    );
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter2");
    let inputs = db.script_instance_run_inputs(&saved.id).unwrap().unwrap();
    assert_eq!(
        inputs
            .iter()
            .map(|input| (input.name().as_str(), input.value().is_secret()))
            .collect::<Vec<_>>(),
        [("Host", false), ("Token", true), ("Empty", false)]
    );
    assert!(db.script_instance_run_inputs("missing").unwrap().is_none());

    // An instance with no name of its own takes its script's.
    let unnamed = db
        .create_script_instance(draft("cleanup.py", InstanceTrigger::PostProcessing))
        .unwrap();
    assert_eq!(unnamed.name, "cleanup.py");
    assert!(unnamed.blocking);
    assert_eq!(unnamed.timeout_seconds, None);
    assert!(unnamed.categories.is_empty());
}

#[test]
fn an_update_stores_the_links_it_is_sent() {
    let db = Database::open_in_memory().unwrap();
    let token = db.create_secret("Token", "hunter2").unwrap();
    let other = db.create_secret("Other", "second").unwrap();
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::PostProcessing)
                .input("Host", "old.example.invalid")
                .secret_input("Token", &token.id)
                .secret_input("Other", &other.id),
        )
        .unwrap();

    // What the editor sends back: plain values as shown, links as shown.
    let mut edit = ScriptInstanceDraft::from_instance(&saved).named("Renamed");
    edit.inputs[0].value = Some("new.example.invalid".into());
    edit.inputs.retain(|input| input.name != "Other");
    let updated = db.update_script_instance(&saved.id, edit).unwrap();

    assert_eq!(updated.id, saved.id);
    assert_eq!(updated.name, "Renamed");
    assert_eq!(updated.run_order, saved.run_order);
    assert_eq!(
        shown(&updated),
        [
            ("Host", "new.example.invalid", None),
            ("Token", "", Some("Token"))
        ]
    );
    assert_eq!(stored_input(&db, &saved.id, "Other"), None);
    // The secret the instance stopped linking is still there.
    assert!(db.secret(&other.id).unwrap().unwrap().used_by.is_empty());

    // Linking another secret is just storing the other link.
    let mut edit = ScriptInstanceDraft::from_instance(&updated);
    edit.inputs[1] = InstanceInputDraft::secret("Token", &other.id);
    db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(run_value(&db, &saved.id, "Token"), "second");

    // An input is a value or a link, never both or neither, and a link must
    // name a secret that exists.
    for (input, reason) in [
        (
            InstanceInputDraft {
                name: "Token".into(),
                value: Some("x".into()),
                secret_id: Some(token.id.clone()),
                sealed: false,
            },
            "an input holds either a value or a secret",
        ),
        (
            InstanceInputDraft {
                name: "Token".into(),
                value: None,
                secret_id: None,
                sealed: false,
            },
            "an input holds either a value or a secret",
        ),
        (
            InstanceInputDraft::secret("Token", "missing"),
            "a linked secret does not exist",
        ),
    ] {
        let mut edit = ScriptInstanceDraft::from_instance(&updated);
        edit.inputs[1] = input;
        assert!(matches!(
            db.update_script_instance(&saved.id, edit),
            Err(ScriptInstanceError::Invalid(found)) if found == reason
        ));
    }

    assert!(matches!(
        db.update_script_instance("missing", draft("notify.sh", InstanceTrigger::Scan)),
        Err(ScriptInstanceError::NotFound)
    ));
}

#[test]
fn a_linked_secret_stays_with_the_instance_and_a_run_resolves_its_current_value() {
    let db = Database::open_in_memory().unwrap();
    let token = db.create_secret("Token", "hunter2").unwrap();
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::PostProcessing).secret_input("Token", &token.id),
        )
        .unwrap();

    // Pointed at another script, the link goes along without being chosen
    // again.
    let mut moved = ScriptInstanceDraft::from_instance(&saved);
    moved.script = ScriptName::new("other.sh").unwrap();
    assert!(moved.inputs.iter().all(|input| input.value.is_none()));
    let updated = db.update_script_instance(&saved.id, moved).unwrap();
    assert_eq!(updated.script.as_str(), "other.sh");
    let run = db.script_instance_run_inputs(&saved.id).unwrap().unwrap();
    assert_eq!(run.len(), 1);
    assert_eq!(run[0].name().as_str(), "Token");
    assert!(matches!(
        run[0].value(),
        OptionValue::Secret(value) if value.expose_for_execution() == "hunter2"
    ));

    // Rotating the secret changes what the next run is handed.
    db.update_secret(&token.id, None, Some("hunter3")).unwrap();
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter3");
    // Renaming it changes what the input shows, not what the script sees.
    db.update_secret(&token.id, Some("Renamed token"), None)
        .unwrap();
    assert_eq!(
        shown(&db.script_instance(&saved.id).unwrap().unwrap()),
        [("Token", "", Some("Renamed token"))]
    );
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter3");
}

/// The sealed value of one input, exactly as the row holds it. `None` when
/// there is no such row, `Some(None)` when the row holds no sealed value.
fn stored_sealed(db: &Database, instance: &str, name: &str) -> Option<Option<String>> {
    let datastore = db.datastore();
    let (instance, name) = (instance.to_string(), name.to_string());
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_optional(
            datastore.read_exec(),
            "SELECT sealed_value FROM script_instance_inputs
              WHERE instance_id = {} AND name = {}",
            &[SqlArg::Text(instance), SqlArg::Text(name)],
        )
        .await?
        .map(|row| row.opt_text("sealed_value"))
        .transpose()
    })
    .unwrap()
}

/// What each input of an instance shows, with whether it is a secret of the
/// instance's own.
fn shown_sealed(instance: &ScriptInstance) -> Vec<(&str, &str, Option<&str>, bool)> {
    instance
        .inputs
        .iter()
        .map(|input| {
            (
                input.name.as_str(),
                input.value.as_str(),
                input.secret.as_ref().map(|secret| secret.name.as_str()),
                input.sealed,
            )
        })
        .collect()
}

#[test]
fn a_secret_of_the_instances_own_is_sealed_in_its_row_and_never_read_back() {
    let db = Database::open_in_memory().unwrap();
    assert!(!db.has_encrypted_credentials().unwrap());
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::PostProcessing)
                .input("Host", "mail.example.invalid")
                .sealed_input("Token", "hunter2")
                .sealed_input("Blank", ""),
        )
        .unwrap();

    assert_eq!(
        shown_sealed(&saved),
        [
            ("Host", "mail.example.invalid", None, false),
            ("Token", "", None, true),
            ("Blank", "", None, true),
        ]
    );
    assert!(saved.inputs[1].is_secret());
    assert_eq!(db.script_instance(&saved.id).unwrap(), Some(saved.clone()));

    // The row holds the value sealed, beside an empty plain value.
    assert_eq!(stored_input(&db, &saved.id, "Token").unwrap(), "");
    let sealed = stored_sealed(&db, &saved.id, "Token").unwrap().unwrap();
    assert!(crate::persistence::encryption::is_encrypted(&sealed));
    assert!(!sealed.contains("hunter2"));
    assert_eq!(stored_sealed(&db, &saved.id, "Host"), Some(None));
    // It is the instance's alone: no named secret was made for it.
    assert!(db.secrets().unwrap().is_empty());
    assert!(db.secret_usages().unwrap().is_empty());

    // Nothing an instance is shown as carries the value, clear or sealed,
    // and neither does what it was sent as when that is printed.
    for text in [
        format!("{saved:?}"),
        serde_json::to_string(&saved).unwrap(),
        format!("{:?}", ScriptInstanceDraft::from_instance(&saved)),
        format!(
            "{:?}",
            draft("notify.sh", InstanceTrigger::PostProcessing).sealed_input("Token", "hunter2")
        ),
    ] {
        assert!(!text.contains("hunter2"), "{text}");
        assert!(!text.contains(&sealed), "{text}");
    }

    // A run is handed it exactly as it is handed a linked secret.
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter2");
    assert_eq!(run_value(&db, &saved.id, "Blank"), "");
    let inputs = db.script_instance_run_inputs(&saved.id).unwrap().unwrap();
    assert_eq!(
        inputs
            .iter()
            .map(|input| (input.name().as_str(), input.value().is_secret()))
            .collect::<Vec<_>>(),
        [("Host", false), ("Token", true), ("Blank", true)]
    );
    assert!(!format!("{inputs:?}").contains("hunter2"));

    // It counts among the credentials the encryption key answers for.
    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(db.encryption_key().unwrap())
        .unwrap();
    let error = db
        .validate_encrypted_credentials(&crate::persistence::encryption::EncryptionKey::generate())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!(
            "cannot decrypt secret input Token of script instance {}",
            saved.id
        )),
        "{error}"
    );
    assert!(!error.contains(&sealed), "{error}");

    // Deleting the instance takes its secrets with it.
    assert!(db.delete_script_instance(&saved.id).unwrap());
    assert_eq!(stored_sealed(&db, &saved.id, "Token"), None);
    assert!(!db.has_encrypted_credentials().unwrap());
}

#[test]
fn an_update_keeps_replaces_or_drops_a_secret_of_the_instances_own() {
    let db = Database::open_in_memory().unwrap();
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::PostProcessing)
                .input("Host", "mail.example.invalid")
                .sealed_input("Token", "hunter2"),
        )
        .unwrap();
    let first = stored_sealed(&db, &saved.id, "Token").unwrap().unwrap();

    // Saved back as it was read, the secret is carried across as it is
    // stored: the draft holds no value for it, and none is needed.
    let edit = ScriptInstanceDraft::from_instance(&saved).named("Renamed");
    assert_eq!(edit.inputs[1], InstanceInputDraft::kept_sealed("Token"));
    let updated = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(updated.name, "Renamed");
    assert_eq!(updated.inputs, saved.inputs);
    assert_eq!(
        stored_sealed(&db, &saved.id, "Token"),
        Some(Some(first.clone()))
    );

    // So do the changes that leave an instance's inputs alone.
    db.disable_script_instances().unwrap();
    db.reorder_script_instances(std::slice::from_ref(&saved.id))
        .unwrap();
    let mut moved = ScriptInstanceDraft::from_instance(&updated);
    moved.script = ScriptName::new("other.sh").unwrap();
    moved.trigger = InstanceTrigger::Scan;
    db.update_script_instance(&saved.id, moved).unwrap();
    assert_eq!(
        stored_sealed(&db, &saved.id, "Token"),
        Some(Some(first.clone()))
    );
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter2");

    // The name it is kept under is matched without regard to case, as input
    // names are when they are checked for being unique.
    let mut edit = ScriptInstanceDraft::from_instance(&updated);
    edit.inputs[1] = InstanceInputDraft::kept_sealed("TOKEN");
    let recased = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(recased.inputs[1].name.as_str(), "TOKEN");
    assert_eq!(stored_sealed(&db, &saved.id, "Token"), None);
    assert_eq!(
        stored_sealed(&db, &saved.id, "TOKEN"),
        Some(Some(first.clone()))
    );

    // A new value replaces it.
    let mut edit = ScriptInstanceDraft::from_instance(&recased);
    edit.inputs[1] = InstanceInputDraft::sealed("Token", "hunter3");
    let replaced = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(
        shown_sealed(&replaced),
        [
            ("Host", "mail.example.invalid", None, false),
            ("Token", "", None, true),
        ]
    );
    let second = stored_sealed(&db, &saved.id, "Token").unwrap().unwrap();
    assert_ne!(second, first);
    assert!(!second.contains("hunter3"));
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter3");

    // It can become a plain value or a link, and a secret of its own again.
    let named = db.create_secret("Shared", "linked").unwrap();
    let mut edit = ScriptInstanceDraft::from_instance(&replaced);
    edit.inputs[1] = InstanceInputDraft::plain("Token", "in clear");
    let plain = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(plain.inputs[1].value, "in clear");
    assert!(!plain.inputs[1].sealed);
    assert_eq!(stored_sealed(&db, &saved.id, "Token"), Some(None));
    let mut edit = ScriptInstanceDraft::from_instance(&plain);
    edit.inputs[1] = InstanceInputDraft::secret("Token", &named.id);
    let linked = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(
        shown_sealed(&linked)[1],
        ("Token", "", Some("Shared"), false)
    );
    assert_eq!(stored_sealed(&db, &saved.id, "Token"), Some(None));
    assert_eq!(run_value(&db, &saved.id, "Token"), "linked");
    let mut edit = ScriptInstanceDraft::from_instance(&linked);
    edit.inputs[1] = InstanceInputDraft::sealed("Token", "hunter4");
    let own = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(shown_sealed(&own)[1], ("Token", "", None, true));
    assert!(db.secret(&named.id).unwrap().unwrap().used_by.is_empty());
    assert_eq!(run_value(&db, &saved.id, "Token"), "hunter4");

    // Left out of what is sent, it goes with every other input left out.
    let mut edit = ScriptInstanceDraft::from_instance(&own);
    edit.inputs.retain(|input| input.name != "Token");
    let dropped = db.update_script_instance(&saved.id, edit).unwrap();
    assert_eq!(
        shown_sealed(&dropped),
        [("Host", "mail.example.invalid", None, false)]
    );
    assert_eq!(stored_sealed(&db, &saved.id, "Token"), None);
    // With the named secret gone as well, nothing sealed is left behind.
    assert!(db.has_encrypted_credentials().unwrap());
    db.delete_secret(&named.id).unwrap();
    assert!(!db.has_encrypted_credentials().unwrap());
}

#[test]
fn a_secret_of_the_instances_own_that_cannot_be_saved_as_asked_is_refused() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;
    let named = db.create_secret("Shared", "linked").unwrap();

    // A new instance holds nothing to keep, and the refusal names the input.
    let error = db
        .create_script_instance(
            draft("a.sh", pp)
                .input("Host", "a")
                .sealed_input("Key", "k"),
        )
        .map(|_| ())
        .and_then(|()| {
            let mut new = draft("a.sh", pp).input("Host", "a");
            new.inputs.push(InstanceInputDraft::kept_sealed("Token"));
            db.create_script_instance(new).map(|_| ())
        })
        .unwrap_err();
    assert!(
        matches!(&error, ScriptInstanceError::NoOwnSecret(name) if name == "Token"),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "input \"Token\" is marked secret but was given no value, and none is saved for it"
    );
    let saved = db.script_instances().unwrap().remove(0);
    assert_eq!(db.script_instances().unwrap().len(), 1);
    let sealed = stored_sealed(&db, &saved.id, "Key").unwrap().unwrap();

    // Nor is there one to keep under a name the instance holds as a plain
    // value, or does not hold at all.
    for name in ["Host", "Other"] {
        let mut edit = ScriptInstanceDraft::from_instance(&saved);
        edit.inputs.retain(|input| input.name != name);
        edit.inputs.push(InstanceInputDraft::kept_sealed(name));
        let error = db.update_script_instance(&saved.id, edit).unwrap_err();
        assert!(
            matches!(&error, ScriptInstanceError::NoOwnSecret(found) if found == name),
            "{error:?}"
        );
    }
    // An instance that is not there is reported as that.
    let mut gone = draft("a.sh", pp);
    gone.inputs.push(InstanceInputDraft::kept_sealed("Key"));
    assert!(matches!(
        db.update_script_instance("missing", gone),
        Err(ScriptInstanceError::NotFound)
    ));

    // A secret of its own is never also a link, with or without a value.
    for value in [None, Some("x".to_string())] {
        let mut edit = ScriptInstanceDraft::from_instance(&saved);
        edit.inputs[1] = InstanceInputDraft {
            name: "Key".into(),
            value,
            secret_id: Some(named.id.clone()),
            sealed: true,
        };
        assert!(matches!(
            db.update_script_instance(&saved.id, edit),
            Err(ScriptInstanceError::Invalid(
                "an input is a secret of its own or a link to a named secret, not both"
            ))
        ));
    }
    // Its value is held to the limits of a plain one.
    for value in ["nul\0byte".to_string(), "x".repeat(64 * 1024 + 1)] {
        assert_eq!(
            refused(&db, draft("a.sh", pp).sealed_input("Key", value)),
            "input value is invalid"
        );
    }
    db.create_script_instance(draft("a.sh", pp).sealed_input("Key", "x".repeat(64 * 1024)))
        .unwrap();

    // Every refusal left what was saved as it was.
    assert_eq!(db.script_instance(&saved.id).unwrap(), Some(saved.clone()));
    assert_eq!(stored_sealed(&db, &saved.id, "Key"), Some(Some(sealed)));
    assert_eq!(run_value(&db, &saved.id, "Key"), "k");
}

#[test]
fn a_secret_of_the_instances_own_needs_an_encryption_key_as_a_named_secret_does() {
    let directory = tempfile::tempdir().unwrap();
    let db = Database::open(&directory.path().join("weaver.db")).unwrap();
    let pp = InstanceTrigger::PostProcessing;

    let named = match db.create_secret("Shared", "hunter2") {
        Err(SecretError::Invalid(reason)) => reason,
        other => panic!("expected the secret to be refused, got {other:?}"),
    };
    assert_eq!(named, "an encryption key is required to store a secret");
    assert_eq!(
        refused(&db, draft("a.sh", pp).sealed_input("Token", "hunter2")),
        named
    );
    assert!(db.script_instances().unwrap().is_empty());

    // Plain inputs need no key.
    db.create_script_instance(draft("a.sh", pp).input("Host", "a"))
        .unwrap();
}

#[test]
fn secrets_are_created_renamed_rotated_and_deleted_only_when_unused() {
    let db = Database::open_in_memory().unwrap();
    let token = db.create_secret("  Mail token ", "first").unwrap();
    assert_eq!(token.name, "Mail token");
    assert!(token.used_by.is_empty());
    let sealed = stored_secret(&db, &token.id);
    assert!(!sealed.contains("first"));

    // Names are unique without regard to case.
    assert!(matches!(
        db.create_secret("MAIL TOKEN", "x"),
        Err(SecretError::NameTaken)
    ));
    for name in ["", "   ", "two\nlines", &"x".repeat(129)] {
        assert!(matches!(
            db.create_secret(name, "x"),
            Err(SecretError::Invalid(_))
        ));
    }
    assert!(matches!(
        db.create_secret("Nul", "a\0b"),
        Err(SecretError::Invalid(_))
    ));
    db.create_secret(&"x".repeat(128), "x").unwrap();

    let spare = db.create_secret("Spare", "spare").unwrap();
    assert!(matches!(
        db.update_secret(&spare.id, Some("mail Token"), None),
        Err(SecretError::NameTaken)
    ));
    // Its own name under another casing is still its own.
    let renamed = db
        .update_secret(&token.id, Some("MAIL TOKEN"), None)
        .unwrap();
    assert_eq!(renamed.name, "MAIL TOKEN");
    assert_eq!(stored_secret(&db, &token.id), sealed);
    let rotated = db.update_secret(&token.id, None, Some("second")).unwrap();
    assert_ne!(stored_secret(&db, &token.id), sealed);
    assert_eq!(rotated.created_at_ms, token.created_at_ms);
    assert!(matches!(
        db.update_secret("missing", None, Some("x")),
        Err(SecretError::NotFound)
    ));

    let first = db
        .create_script_instance(
            draft("a.sh", InstanceTrigger::PostProcessing)
                .named("Zeta")
                .secret_input("Token", &token.id),
        )
        .unwrap();
    let second = db
        .create_script_instance(
            draft("b.sh", InstanceTrigger::Scan)
                .named("alpha")
                .secret_input("Token", &token.id)
                .secret_input("Again", &token.id),
        )
        .unwrap();
    let listed = db.secret(&token.id).unwrap().unwrap();
    assert_eq!(
        listed
            .used_by
            .iter()
            .map(|usage| (usage.instance_id.as_str(), usage.instance_name.as_str()))
            .collect::<Vec<_>>(),
        [(second.id.as_str(), "alpha"), (first.id.as_str(), "Zeta")]
    );
    assert_eq!(db.secret_usages().unwrap().len(), 1);

    // Deleting a secret in use is refused, naming who uses it.
    let error = db.delete_secret(&token.id).unwrap_err();
    assert!(matches!(&error, SecretError::InUse(usages) if usages.len() == 2));
    let message = error.to_string();
    assert!(
        message.contains("alpha") && message.contains("Zeta"),
        "{message}"
    );
    assert!(!message.contains("second"), "{message}");

    db.delete_script_instance(&first.id).unwrap();
    db.delete_script_instance(&second.id).unwrap();
    db.delete_secret(&token.id).unwrap();
    assert!(matches!(
        db.delete_secret(&token.id),
        Err(SecretError::NotFound)
    ));
    assert_eq!(
        db.secrets()
            .unwrap()
            .iter()
            .map(|secret| secret.name.as_str())
            .collect::<Vec<_>>(),
        ["Spare", "x".repeat(128).as_str()]
    );
}

#[test]
fn linking_a_deleted_secrets_stale_id_is_refused_as_invalid() {
    let db = Database::open_in_memory().unwrap();
    let gone = db.create_secret("Gone", "x").unwrap();
    db.delete_secret(&gone.id).unwrap();

    let error = db
        .create_script_instance(
            draft("a.sh", InstanceTrigger::PostProcessing).secret_input("Token", &gone.id),
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            ScriptInstanceError::Invalid("a linked secret does not exist")
        ),
        "{error:?}"
    );
    assert!(db.script_instances().unwrap().is_empty());

    let kept = db.create_secret("Kept", "y").unwrap();
    let saved = db
        .create_script_instance(
            draft("a.sh", InstanceTrigger::PostProcessing).secret_input("Token", &kept.id),
        )
        .unwrap();
    let error = db
        .update_script_instance(
            &saved.id,
            draft("a.sh", InstanceTrigger::PostProcessing).secret_input("Token", &gone.id),
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            ScriptInstanceError::Invalid("a linked secret does not exist")
        ),
        "{error:?}"
    );
    // The refused update left the saved link as it was.
    let after = db.script_instance(&saved.id).unwrap().unwrap();
    assert_eq!(
        after.inputs[0]
            .secret
            .as_ref()
            .map(|secret| secret.id.as_str()),
        Some(kept.id.as_str())
    );
}

#[test]
fn an_instance_that_could_not_run_as_written_is_refused() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;

    assert_eq!(
        refused(&db, draft("a.sh", pp).named("x".repeat(129))),
        "instance name is too long or holds a control character"
    );
    assert_eq!(
        refused(&db, draft("a.sh", pp).named("two\nlines")),
        "instance name is too long or holds a control character"
    );
    for seconds in [0, 7 * 24 * 60 * 60 + 1] {
        assert_eq!(
            refused(&db, draft("a.sh", pp).timeout(seconds)),
            "timeout must be between one second and seven days"
        );
    }
    assert_eq!(
        refused(&db, draft("a.sh", pp).category("  ")),
        "category name is invalid"
    );
    // A category only means something where there is a download to have one.
    for trigger in [
        InstanceTrigger::Scan,
        InstanceTrigger::Schedule,
        InstanceTrigger::Feed,
    ] {
        assert_eq!(
            refused(&db, draft("a.sh", trigger).category("movies")),
            "only post-processing and queue instances can be narrowed to categories"
        );
    }
    assert_eq!(
        refused(&db, draft("a.sh", pp).input("Host", "a").input("HOST", "b")),
        "input names must be unique"
    );
    assert_eq!(
        refused(&db, draft("a.sh", pp).input("", "a")),
        "input name is invalid"
    );
    assert_eq!(
        refused(&db, draft("a.sh", pp).input("Host", "nul\0byte")),
        "input value is invalid"
    );
    assert_eq!(
        refused(
            &db,
            draft("a.sh", pp).input("Host", "x".repeat(64 * 1024 + 1))
        ),
        "input value is invalid"
    );
    assert!(db.script_instances().unwrap().is_empty());

    // The longest name and timeout that are allowed.
    db.create_script_instance(
        draft("a.sh", pp)
            .named("x".repeat(128))
            .timeout(7 * 24 * 60 * 60),
    )
    .unwrap();
}

#[test]
fn a_new_instance_goes_last_and_a_reorder_moves_only_what_it_names() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;
    let ids = ["first", "second", "third", "fourth"]
        .map(|name| {
            db.create_script_instance(draft("a.sh", pp).named(name))
                .unwrap()
                .id
        })
        .to_vec();
    let saved = db.script_instances().unwrap();
    assert_eq!(names(&saved), ["first", "second", "third", "fourth"]);
    assert_eq!(
        saved
            .iter()
            .map(|instance| instance.run_order)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );

    // The ones named come first in the order given; the rest keep theirs. An
    // id that no longer exists is passed over.
    db.reorder_script_instances(&[ids[2].clone(), "gone".into(), ids[0].clone()])
        .unwrap();
    let saved = db.script_instances().unwrap();
    assert_eq!(names(&saved), ["third", "first", "second", "fourth"]);
    assert_eq!(
        saved
            .iter()
            .map(|instance| instance.run_order)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );

    assert!(db.delete_script_instance(&ids[0]).unwrap());
    assert!(!db.delete_script_instance(&ids[0]).unwrap());
    let last = db
        .create_script_instance(draft("a.sh", pp).named("fifth"))
        .unwrap();
    assert_eq!(
        names(&db.script_instances().unwrap()),
        ["third", "second", "fourth", "fifth"]
    );
    assert_eq!(last.run_order, 4);
}

#[test]
fn the_global_scripts_setting_decides_whether_a_category_adds_or_replaces() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;
    for instance in [
        draft("a.sh", pp).named("movies").category("Movies"),
        draft("a.sh", pp).named("everything"),
        draft("a.sh", pp).named("off").disabled(),
        draft("a.sh", InstanceTrigger::Scan).named("scan"),
    ] {
        db.create_script_instance(instance).unwrap();
    }
    let event = ScriptEventLabel::PostProcessing;
    let resolved = |category| names_owned(&db.script_instances_for(&event, category).unwrap());

    assert_eq!(resolved(Some("movies")), ["everything", "movies"]);
    assert_eq!(resolved(Some("tv")), ["everything"]);
    assert_eq!(resolved(None), ["everything"]);

    db.save_post_processing_settings(&PostProcessingSettings {
        global_scripts_run: GlobalScriptsRun::OnlyWithoutCategoryScripts,
        ..PostProcessingSettings::default()
    })
    .unwrap();
    assert_eq!(resolved(Some("MOVIES")), ["movies"]);
    assert_eq!(resolved(Some("tv")), ["everything"]);
    assert_eq!(resolved(None), ["everything"]);
}

fn names_owned(instances: &[ScriptInstance]) -> Vec<String> {
    instances
        .iter()
        .map(|instance| instance.name.clone())
        .collect()
}

#[test]
fn a_feed_runs_only_feed_instances_and_loses_one_that_stops_being_one() {
    let db = Database::open_in_memory().unwrap();
    for id in [7, 9] {
        db.insert_rss_feed(&crate::RssFeedRow {
            id,
            name: "fixture".into(),
            url: "https://feed.invalid".into(),
            enabled: true,
            poll_interval_secs: 900,
            scripts: Vec::new(),
            username: None,
            password: None,
            default_category: None,
            default_metadata: Vec::new(),
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        })
        .unwrap();
    }
    let first = db
        .create_script_instance(draft("feed.sh", InstanceTrigger::Feed).named("first"))
        .unwrap();
    let second = db
        .create_script_instance(draft("feed.sh", InstanceTrigger::Feed).named("second"))
        .unwrap();
    let scan = db
        .create_script_instance(draft("scan.sh", InstanceTrigger::Scan))
        .unwrap();

    assert!(matches!(
        db.set_feed_script_instances(7, std::slice::from_ref(&scan.id)),
        Err(ScriptInstanceError::Invalid(
            "only a feed script job can be attached to a feed"
        ))
    ));
    assert!(matches!(
        db.set_feed_script_instances(7, &[first.id.clone(), first.id.clone()]),
        Err(ScriptInstanceError::Invalid(
            "a script job can be attached to a feed once"
        ))
    ));
    assert!(matches!(
        db.set_feed_script_instances(7, &["missing".into()]),
        Err(ScriptInstanceError::NotFound)
    ));
    assert!(db.feed_script_instance_ids(7).unwrap().is_empty());

    // A feed runs its instances in the order it was given them.
    db.set_feed_script_instances(7, &[second.id.clone(), first.id.clone()])
        .unwrap();
    db.set_feed_script_instances(9, std::slice::from_ref(&first.id))
        .unwrap();
    assert_eq!(
        db.feed_script_instance_ids(7).unwrap(),
        [second.id.clone(), first.id.clone()]
    );
    let by_feed = db.feed_script_instance_ids_by_feed().unwrap();
    assert_eq!(by_feed.len(), 2);
    assert_eq!(by_feed[&9], std::slice::from_ref(&first.id));

    // Editing an instance leaves it attached while it is still a feed one.
    db.update_script_instance(
        &first.id,
        ScriptInstanceDraft::from_instance(&first).named("renamed"),
    )
    .unwrap();
    assert_eq!(
        db.feed_script_instance_ids(9).unwrap(),
        std::slice::from_ref(&first.id)
    );

    let mut moved = ScriptInstanceDraft::from_instance(&first);
    moved.trigger = InstanceTrigger::Scan;
    db.update_script_instance(&first.id, moved).unwrap();
    assert_eq!(
        db.feed_script_instance_ids(7).unwrap(),
        std::slice::from_ref(&second.id)
    );
    assert!(db.feed_script_instance_ids(9).unwrap().is_empty());

    assert!(db.delete_script_instance(&second.id).unwrap());
    assert!(db.feed_script_instance_ids_by_feed().unwrap().is_empty());

    db.set_feed_script_instances(7, &[]).unwrap();
    db.delete_feed_script_instances(7).unwrap();
}

#[test]
fn a_job_keeps_the_instances_it_was_admitted_with_through_a_directory_change() {
    let db = Database::open_in_memory().unwrap();
    let root = tempfile::tempdir().unwrap();
    let old_root = normalize_script_directory(&root.path().join("old")).unwrap();
    let new_root = normalize_script_directory(&root.path().join("new")).unwrap();
    db.replace_post_processing_script_directory(&old_root)
        .unwrap();
    let executor = PostProcessingExecutor::new(db.clone(), old_root.clone());

    // Nothing is captured while scripts are turned off.
    db.create_script_instance(draft("notify.sh", InstanceTrigger::PostProcessing))
        .unwrap();
    assert!(executor.admit_job_scripts(None).unwrap().is_none());

    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..PostProcessingSettings::default()
    })
    .unwrap();
    let (_, admitted_root) = db.post_processing_script_admission().unwrap();
    let admission = executor.admit_job_scripts(None).unwrap().unwrap();
    assert!(admission.has_enabled_entries());

    db.replace_post_processing_script_directory(&new_root)
        .unwrap();

    assert_eq!(admitted_root, old_root);
    assert!(admission.has_enabled_entries());
    // A job admitted now would run nothing: the new directory has not been
    // looked over by the operator yet.
    assert!(
        db.script_instances()
            .unwrap()
            .iter()
            .all(|instance| !instance.enabled)
    );
    assert!(
        !executor
            .admit_job_scripts(None)
            .unwrap()
            .unwrap()
            .has_enabled_entries()
    );
}

#[test]
fn job_results_and_summary_are_stored_on_the_job_and_read_back() {
    let db = Database::open_in_memory().unwrap();
    let results = vec![ScriptResult {
        script: script("notify.sh"),
        instance_id: Some("one".into()),
        instance_name: Some("Tell the family".into()),
        event: Default::default(),
        adapter: ScriptAdapter::Sabnzbd,
        status: ScriptStatus::Warning,
        exit_code: Some(3),
        duration_ms: 12,
        output_tail: "tail".into(),
        output_id: None,
        background: false,
        output_truncated: false,
        error_message: Some("exited 3".into()),
        finished_at_epoch_ms: 1_000,
    }];
    // No row for the job yet, so the write is a no-op rather than an error.
    db.save_job_post_processing_results(7, PostProcessingSummary::Warning, &results)
        .unwrap();
    assert!(db.job_post_processing_results(7).unwrap().is_empty());
}

#[tokio::test]
async fn a_waited_script_that_cannot_start_is_listed_among_the_jobs_runs() {
    let db = Database::open_in_memory().unwrap();
    let root = tempfile::tempdir().unwrap();
    let scripts = normalize_script_directory(&root.path().join("scripts")).unwrap();
    db.replace_post_processing_script_directory(&scripts)
        .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..PostProcessingSettings::default()
    })
    .unwrap();
    db.insert_job_history(&crate::JobHistoryRow {
        job_id: 51,
        job_hash: None,
        name: "unstarted".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 1,
        metadata: None,
        server_attribution: None,
    })
    .unwrap();
    // The job waits for a script the scripts directory does not hold.
    db.create_script_instance(draft("missing.sh", InstanceTrigger::PostProcessing))
        .unwrap();
    let executor = PostProcessingExecutor::new(db.clone(), scripts.clone());
    let admission = executor.admit_job_scripts(None).unwrap().unwrap();
    let context = super::runner::JobExecutionContext {
        job_id: 51,
        name: "unstarted".into(),
        nzb_filename: "unstarted.nzb".into(),
        category: None,
        group: None,
        source_url: None,
        working_directory: root.path().to_path_buf(),
        final_directory: root.path().to_path_buf(),
        pipeline_outcome: super::model::PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: Default::default(),
    };
    let report = executor
        .execute_admitted_job(51, admission, context, None, None)
        .await
        .unwrap();
    assert_eq!(report.results.len(), 1);
    assert_eq!(report.results[0].status, ScriptStatus::Warning);

    let runs = db.script_runs(Default::default(), None, 10).unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].job_id, Some(51));
    assert_eq!(runs[0].job_name.as_deref(), Some("unstarted"));
    assert_eq!(runs[0].result.script.as_str(), "missing.sh");
    assert_eq!(runs[0].result.status, ScriptStatus::Warning);
    assert!(!runs[0].result.background);
    assert!(runs[0].result.error_message.is_some());
}

// A job in post-processing with five waited scripts, none of which the
// scripts directory holds, so a script that runs leaves a Warning run in the
// Runs list and a script that does not leaves nothing.
fn interrupted_job(
    job_id: u64,
) -> (
    Database,
    tempfile::TempDir,
    PostProcessingExecutor,
    Vec<ScriptInstance>,
) {
    let db = Database::open_in_memory().unwrap();
    let root = tempfile::tempdir().unwrap();
    let scripts = normalize_script_directory(&root.path().join("scripts")).unwrap();
    db.replace_post_processing_script_directory(&scripts)
        .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..PostProcessingSettings::default()
    })
    .unwrap();
    db.create_active_job(&crate::ActiveJob {
        job_id: crate::JobId(job_id),
        nzb_hash: [job_id as u8; 32],
        nzb_path: "fixture.nzb".into(),
        nzb_zstd: Vec::new(),
        output_dir: "fixture".into(),
        created_at: 1,
        category: None,
        metadata: Vec::new(),
        status: "post_processing",
        download_state: "complete",
        post_state: "post_processing",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
    })
    .unwrap();
    let instances = ["one.sh", "two.sh", "three.sh", "four.sh", "five.sh"]
        .into_iter()
        .map(|name| {
            db.create_script_instance(draft(name, InstanceTrigger::PostProcessing))
                .unwrap()
        })
        .collect();
    let executor = PostProcessingExecutor::new(db.clone(), scripts);
    (db, root, executor, instances)
}

fn started(instance: &ScriptInstance) -> super::model::StartedScript {
    super::model::StartedScript {
        id: instance.id.clone(),
        name: instance.name.clone(),
        script: instance.script.clone(),
        waited: true,
    }
}

fn finished(instance: &ScriptInstance) -> ScriptResult {
    ScriptResult {
        script: instance.script.clone(),
        instance_id: Some(instance.id.clone()),
        instance_name: Some(instance.name.clone()),
        event: Default::default(),
        adapter: ScriptAdapter::Sabnzbd,
        status: ScriptStatus::Succeeded,
        exit_code: Some(0),
        duration_ms: 5,
        output_tail: String::new(),
        output_id: None,
        background: false,
        output_truncated: false,
        error_message: None,
        finished_at_epoch_ms: 1_000,
    }
}

fn job_context(job_id: u64, root: &tempfile::TempDir) -> super::runner::JobExecutionContext {
    super::runner::JobExecutionContext {
        job_id,
        name: "resumed".into(),
        nzb_filename: "resumed.nzb".into(),
        category: None,
        group: None,
        source_url: None,
        working_directory: root.path().to_path_buf(),
        final_directory: root.path().to_path_buf(),
        pipeline_outcome: super::model::PipelineOutcome::Succeeded,
        par_status: 0,
        unpack_status: 0,
        compatibility: Default::default(),
    }
}

// What weaver left on the job row when it stopped, read back as a restart
// reads it.
fn stopped_with(
    db: &Database,
    job_id: u64,
    resume: &super::model::PostProcessingResume,
) -> super::model::PostProcessingResume {
    db.mark_job_post_processing_resumed(job_id, resume).unwrap();
    assert_eq!(db.recover_interrupted_post_processing().unwrap(), 1);
    let stored = db.job_post_processing_resume(job_id).unwrap().unwrap();
    assert_eq!(&stored, resume);
    stored
}

fn run_scripts(db: &Database) -> Vec<String> {
    db.script_runs(Default::default(), None, 10)
        .unwrap()
        .into_iter()
        .map(|run| run.result.script.as_str().to_string())
        .collect()
}

#[tokio::test]
async fn a_resumed_job_runs_only_the_scripts_that_had_not_started() {
    let (db, root, executor, instances) = interrupted_job(71);
    // Two finished, the third was running when weaver stopped, and the last
    // two never had their turn.
    let resume = stopped_with(
        &db,
        71,
        &super::model::PostProcessingResume {
            started: instances[..3].iter().map(started).collect(),
            results: instances[..2].iter().map(finished).collect(),
        },
    );
    let admission = executor.admit_job_scripts(None).unwrap().unwrap();

    let report = executor
        .resume_admitted_job(71, admission, job_context(71, &root), None, None, resume)
        .await
        .unwrap();

    let mut runs = run_scripts(&db);
    runs.sort();
    assert_eq!(runs, ["five.sh", "four.sh"]);
    assert_eq!(
        report
            .results
            .iter()
            .map(|result| (result.script.as_str(), result.status))
            .collect::<Vec<_>>(),
        [
            ("one.sh", ScriptStatus::Succeeded),
            ("two.sh", ScriptStatus::Succeeded),
            ("three.sh", ScriptStatus::Interrupted),
            ("four.sh", ScriptStatus::Warning),
            ("five.sh", ScriptStatus::Warning),
        ]
    );
    assert_eq!(report.results[2].error_message, None);
    assert_eq!(report.summary, PostProcessingSummary::Interrupted);
    assert_eq!(db.job_post_processing_results(71).unwrap(), report.results);
    assert_eq!(
        db.job_post_processing_summary(71).unwrap(),
        Some(PostProcessingSummary::Interrupted)
    );
    // Every entry has started now, so a second restart would run none.
    let after = db.job_post_processing_resume(71).unwrap().unwrap();
    assert_eq!(
        after
            .started
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>(),
        instances
            .iter()
            .map(|instance| instance.id.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_resumed_job_whose_scripts_had_all_started_runs_none_of_them() {
    let (db, root, executor, instances) = interrupted_job(72);
    // Every entry had its turn; the last was running when weaver stopped.
    let resume = stopped_with(
        &db,
        72,
        &super::model::PostProcessingResume {
            started: instances.iter().map(started).collect(),
            results: instances[..4].iter().map(finished).collect(),
        },
    );
    let admission = executor.admit_job_scripts(None).unwrap().unwrap();

    let report = executor
        .resume_admitted_job(72, admission, job_context(72, &root), None, None, resume)
        .await
        .unwrap();

    assert!(run_scripts(&db).is_empty());
    assert_eq!(report.results.len(), 5);
    assert_eq!(report.results[4].status, ScriptStatus::Interrupted);
    assert_eq!(report.results[4].error_message, None);
    assert_eq!(report.summary, PostProcessingSummary::Interrupted);
    assert_eq!(db.job_post_processing_results(72).unwrap(), report.results);
}
