import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  CREATE_SCRIPT_INSTANCE_MUTATION,
  SECRETS_QUERY,
  UPDATE_SCRIPT_INSTANCE_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { CheckBox, SecondaryButton, TextField } from "../../../components/controls";
import { Icon } from "../../../components/icons";
import {
  MAX_TIMEOUT_SECONDS,
  QUEUE_EVENTS,
  SCRIPT_KINDS,
  SCRIPT_KIND_LABELS,
  booleanInputOn,
  booleanInputValue,
  categoryScoped,
  declaredOption,
  formFromInstance,
  inputFromForm,
  inputNameProblem,
  jobSchedule,
  newInstanceForm,
  triggerTitle,
  unwiredTriggers,
  withScript,
  withLinkedSecret,
  withSecret,
  type DiscoveredScript,
  type InstanceForm,
  type InstanceInputForm,
  type JobScheduleForm,
  type QueueEvent,
  type ScriptInstance,
  type ScriptKind,
} from "../../../data/script-instances";
import { SCHEDULE_DAYS } from "../../../data/schedule-options";
import { sortedSecrets, type Secret, type SecretRef } from "../../../data/secrets";
import { FieldControlView, type FieldSpec } from "../framework";
import { SecretEditor } from "./SecretEditor";

/**
 * The editor of one script instance: which script, what starts it, what it is
 * given, and how it is run.
 *
 * A new instance starts with no script. Choosing one fills the form from that
 * script's header, and from then on the instance belongs to the operator;
 * nothing here reads the header back into a saved instance. A secret input
 * links a named secret, chosen from those there are or created here; its value
 * is never shown. An input added here may instead be a secret of the job's
 * own: typed once, stored encrypted, and never shown again.
 *
 * An instance on the schedule keeps when it runs: its times, its days and
 * whether it also runs at startup. A new one starts from the times the header
 * asks for. Scripts never appear among the schedules.
 */

/** What the editor was opened on: a saved instance, or a new one. */
export type InstanceEditorTarget = { mode: "new" } | { mode: "edit"; instance: ScriptInstance };

/** The secret picker's entry that opens the editor of a new secret. */
const CREATE_SECRET = "\u0000create";

const CHIP = "flex h-8 cursor-pointer items-center justify-center border px-[11px] font-wv-mono text-[11.5px]";
const CHIP_ON = "border-wv-accent bg-wv-segment-active font-medium text-wv-strong";
const CHIP_OFF = "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover";

export function ScriptInstanceEditor({
  target,
  scripts,
  problems,
  instances,
  categories,
  onSaved,
  onDismiss,
  onDelete,
  onReapply,
  onSetUp,
}: {
  target: InstanceEditorTarget;
  scripts: readonly DiscoveredScript[];
  /** The files in the scripts directory that could not be read as scripts, and why. */
  problems: readonly { name: string; message: string }[];
  instances: readonly ScriptInstance[];
  /** Every category's name, for narrowing an instance to some of them. */
  categories: readonly string[];
  /**
   * The instance was saved; `status` says so in the panel's own words, and
   * `problem` what did not go with it.
   */
  onSaved: (status: string, problem?: string) => void;
  onDismiss: () => void;
  onDelete: (instance: ScriptInstance) => void;
  onReapply: (instance: ScriptInstance) => void;
  /** Sets `script` up from its header; resolves to what went wrong, or null. */
  onSetUp: (script: DiscoveredScript) => Promise<string | null>;
}) {
  const t = useTranslate();
  const [, createInstance] = useMutation(CREATE_SCRIPT_INSTANCE_MUTATION);
  const [, updateInstance] = useMutation(UPDATE_SCRIPT_INSTANCE_MUTATION);

  const editing = target.mode === "edit" ? target.instance : null;
  const byName = useMemo(() => new Map(scripts.map((entry) => [entry.name, entry])), [scripts]);
  const [form, setForm] = useState<InstanceForm>(() =>
    target.mode === "edit"
      ? formFromInstance(target.instance, byName.get(target.instance.script))
      : newInstanceForm(undefined),
  );
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // The input being added: a name and its value, kept as the job's own secret when the box is ticked.
  const [newName, setNewName] = useState("");
  const [newValue, setNewValue] = useState("");
  const [newSecret, setNewSecret] = useState(false);
  const [newProblem, setNewProblem] = useState<string | null>(null);
  const [{ data: secretsData }] = useQuery<{ secrets: Secret[] }>({ query: SECRETS_QUERY });
  // Secrets created from this editor, until the list is read again.
  const [created, setCreated] = useState<Secret[]>([]);
  // The input a new secret is being created for.
  const [creatingFor, setCreatingFor] = useState<number | null>(null);
  const secrets = useMemo(() => {
    const listed = secretsData?.secrets ?? [];
    return sortedSecrets([...listed, ...created.filter((entry) => !listed.some((own) => own.id === entry.id))]);
  }, [secretsData?.secrets, created]);
  // What a saved input links, for a secret the list does not hold.
  const linked = useMemo(
    () => new Map<string, SecretRef>((editing?.inputs ?? []).flatMap((input) => (input.secret ? [[input.secret.id, input.secret]] : []))),
    [editing],
  );

  const script = byName.get(form.script);
  const patch = (next: Partial<InstanceForm>) => {
    setError(null);
    setForm((current) => ({ ...current, ...next }));
  };

  const scheduled = form.trigger === "SCHEDULER";
  const schedule = form.schedule;
  const patchSchedule = (next: Partial<JobScheduleForm>) => {
    setError(null);
    setForm((current) => ({ ...current, schedule: { ...current.schedule, ...next } }));
  };

  const save = async () => {
    // What is wrong with the times is said before anything is saved. A job
    // that is turned off may wait without any.
    const times = scheduled ? jobSchedule(schedule) : null;
    if (times && "problem" in times && (times.problem === "invalid" || form.enabled)) {
      setError(
        times.problem === "invalid"
          ? t("next.postProcessing.runTimeInvalid", { time: times.time })
          : t("next.postProcessing.runTimeNeeded"),
      );
      return;
    }
    setBusy(true);
    const input = inputFromForm(form);
    const result = editing
      ? await updateInstance({ id: editing.id, input })
      : await createInstance({ input });
    if (result.error) {
      setBusy(false);
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    const name = input.name || input.script;
    const status = t(editing ? "next.postProcessing.instanceSaved" : "next.postProcessing.instanceCreated", { name });
    setBusy(false);
    onSaved(status);
  };

  const setUp = async () => {
    if (!script) {
      return;
    }
    setBusy(true);
    const problem = await onSetUp(script);
    setBusy(false);
    setError(problem);
  };

  /* ------------------------------------------------------------- the script */

  const scriptOptions = [
    ...(form.script === "" ? [{ value: "", label: t("next.postProcessing.chooseScript") }] : []),
    ...scripts.map((entry) => ({
      value: entry.name,
      label: entry.displayName === entry.name ? entry.name : `${entry.displayName} · ${entry.name}`,
    })),
    // A saved instance keeps naming its script after the file has gone.
    ...(form.script !== "" && !script
      ? [{ value: form.script, label: `${form.script} · ${t("next.postProcessing.missing")}` }]
      : []),
  ];

  const queueEvents: string[] = QUEUE_EVENTS.includes(form.queueEvent)
    ? [...QUEUE_EVENTS]
    : [...QUEUE_EVENTS, form.queueEvent];

  const scriptFields: FieldSpec[] = [
    {
      id: "script",
      label: t("next.postProcessing.script"),
      help: t("next.postProcessing.scriptHelp"),
      control: {
        kind: "select",
        value: form.script,
        options: scriptOptions,
        onChange: (next) => {
          setError(null);
          setForm((current) => withScript(current, byName.get(next), editing === null));
        },
      },
    },
    {
      id: "trigger",
      label: t("next.scriptRuns.trigger"),
      help: t("next.postProcessing.triggerHelp"),
      control: {
        kind: "select",
        value: form.trigger,
        options: SCRIPT_KINDS.map((kind) => ({
          value: kind,
          label: t(SCRIPT_KIND_LABELS[kind]),
        })),
        onChange: (next) => patch({ trigger: next as ScriptKind }),
      },
    },
    ...(form.trigger === "QUEUE"
      ? [
          {
            id: "queueEvent",
            label: t("next.postProcessing.queueEvent"),
            help: t("next.postProcessing.queueEventHelp"),
            control: {
              kind: "select" as const,
              value: form.queueEvent,
              options: queueEvents.map((event) => ({ value: event, label: event })),
              onChange: (next: string) => patch({ queueEvent: next as QueueEvent }),
            },
          },
        ]
      : []),
    {
      id: "name",
      label: t("next.postProcessing.instanceName"),
      help: t("next.postProcessing.instanceNameHelp"),
      control: {
        kind: "text",
        mono: false,
        value: form.name,
        placeholder: form.script,
        onChange: (next) => patch({ name: next }),
      },
    },
  ];

  /* ----------------------------------------------------------- when it runs */

  const scheduleFields: FieldSpec[] = [
        {
          id: "runTimes",
          label: t("next.postProcessing.runTimes"),
          help: t("next.schedules.scriptTimeHelp"),
          control: {
            kind: "text",
            value: schedule.times,
            placeholder: "04:00, *:30",
            onChange: (next) => patchSchedule({ times: next }),
          },
        },
        {
          id: "runDays",
          label: t("next.schedules.days"),
          help: t("next.schedules.daysHelp"),
          control: {
            kind: "custom",
            control: (
              <div role="group" aria-label={t("next.schedules.days")} className="flex flex-wrap justify-end gap-1.5">
                {SCHEDULE_DAYS.map((day) => {
                  const active = schedule.days.includes(day.key);
                  return (
                    <button
                      key={day.key}
                      type="button"
                      aria-pressed={active}
                      onClick={() =>
                        patchSchedule({
                          days: active
                            ? schedule.days.filter((entry) => entry !== day.key)
                            : [...schedule.days, day.key],
                        })
                      }
                      className={`${CHIP} ${active ? CHIP_ON : CHIP_OFF}`}
                    >
                      {t(day.label)}
                    </button>
                  );
                })}
              </div>
            ),
          },
        },
        {
          id: "runAtStartup",
          label: t("next.schedules.runAtStartup"),
          help: t("next.postProcessing.runAtStartupHelp"),
          control: {
            kind: "toggle",
            value: schedule.startup,
            onChange: (next) => patchSchedule({ startup: next }),
          },
        },
  ];

  /* ------------------------------------------------------------- its inputs */

  const changeInput = (index: number, change: (input: InstanceInputForm) => InstanceInputForm) => {
    setError(null);
    setForm((current) => ({
      ...current,
      inputs: current.inputs.map((entry, at) => (at === index ? change(entry) : entry)),
    }));
  };
  const setInput = (index: number, value: string) => changeInput(index, (entry) => ({ ...entry, value }));
  const linkSecret = (index: number, secretId: string | null) =>
    changeInput(index, (entry) => withLinkedSecret(entry, secretId));

  // The first entry clears the link: a prompt while nothing is linked, and
  // "No secret" once something is.
  const secretOptions = (input: InstanceInputForm) => {
    const known = input.secretId === null || secrets.some((entry) => entry.id === input.secretId);
    return [
      {
        value: "",
        label: t(input.secretId === null ? "next.postProcessing.chooseSecret" : "next.postProcessing.noSecret"),
      },
      ...secrets.map((entry) => ({ value: entry.id, label: entry.name })),
      ...(known ? [] : [{ value: input.secretId!, label: linked.get(input.secretId!)?.name ?? input.secretId! }]),
      { value: CREATE_SECRET, label: t("next.postProcessing.createSecret") },
    ];
  };

  const inputField = (input: InstanceInputForm, index: number): FieldSpec => {
    const option = declaredOption(script, input.name);
    const label = option?.displayName || input.name;
    const help = [
      ...(option?.description ?? []),
      option?.required ? t("next.postProcessing.required") : "",
      option?.defaultValue && !input.secret
        ? t("next.postProcessing.defaultValue", { value: option.defaultValue })
        : "",
      input.secret ? t("next.postProcessing.secretLinkHelp") : "",
      script && !option ? t("next.postProcessing.undeclaredInput") : "",
    ]
      .filter(Boolean)
      .join(" ");
    const onChange = (next: string) => setInput(index, next);
    // A choice takes a text field's width, so the inputs' boxes line up whichever kind each is.
    const width = "w-[268px] max-w-full";
    const field: FieldSpec = {
      id: `input:${input.name}`,
      label,
      help: help || undefined,
      keywords: input.name,
      control: input.own
        ? {
            kind: "text",
            type: "password",
            value: input.value,
            // A saved one is not read back: left blank, it stays as it is.
            placeholder: input.held ? t("next.postProcessing.ownSecretSaved") : undefined,
            onChange,
          }
        : input.secret
        ? {
            kind: "select",
            value: input.secretId ?? "",
            className: width,
            options: secretOptions(input),
            onChange: (next: string) => {
              if (next === CREATE_SECRET) {
                setCreatingFor(index);
              } else {
                linkSecret(index, next === "" ? null : next);
              }
            },
          }
        : option && option.select.length > 0
          ? {
              kind: "select",
              value: input.value,
              className: width,
              options: (option.select.includes(input.value) ? option.select : [input.value, ...option.select]).map(
                (entry) => ({ value: entry, label: entry }),
              ),
              onChange,
            }
          : option?.optionType === "BOOLEAN"
            ? {
                kind: "toggle",
                value: booleanInputOn(input.value),
                onChange: (next: boolean) => onChange(booleanInputValue(next)),
              }
            : { kind: "text", value: input.value, onChange },
    };
    // An input is a value or a secret, not a thing switched between the two:
    // the header says which for one it declares, and one added here is a value
    // or the job's own secret. The box is only for where the header can be wrong. It takes
    // an input for a secret by its name, and an input it calls plain may have
    // been saved as a secret; either can be made plain, which empties it.
    // Only an input the header does not ask for can be taken away.
    const secretChoice = option !== undefined && (option.optionType === "SECRET" || input.secret);
    return {
      ...field,
      control: {
        kind: "custom",
        control: (
          <div className="flex min-w-0 items-center gap-2">
            <FieldControlView spec={field} />
            {/* Named on screen as the new input's box is, rather than by a tooltip alone. */}
            {secretChoice ? (
              <label className="flex flex-none cursor-pointer items-center gap-2 text-[12.5px] text-wv-secondary">
                <CheckBox
                  checked={input.secret}
                  onChange={(next) => changeInput(index, (entry) => withSecret(entry, next))}
                  label={t("next.postProcessing.inputIsSecret", { name: input.name })}
                />
                {t("next.postProcessing.secret")}
              </label>
            ) : null}
            {option ? null : (
              <SecondaryButton
                className="px-[10px]"
                title={t("next.postProcessing.removeInput", { name: input.name })}
                onClick={() => {
                  setError(null);
                  setForm((current) => ({
                    ...current,
                    inputs: current.inputs.filter((_, at) => at !== index),
                  }));
                }}
              >
                <Icon name="remove" size={13} />
              </SecondaryButton>
            )}
          </div>
        ),
      },
    };
  };

  const addInput = () => {
    const problem = inputNameProblem(newName, form.inputs);
    setNewProblem(problem);
    if (problem) {
      return;
    }
    setError(null);
    setForm((current) => ({
      ...current,
      inputs: [
        ...current.inputs,
        { name: newName.trim(), value: newValue, secret: false, secretId: null, own: newSecret, held: false },
      ],
    }));
    setNewName("");
    setNewValue("");
    setNewSecret(false);
  };
  // A secret with nothing typed would give the script nothing.
  const canAdd = newName.trim() !== "" && (!newSecret || newValue !== "");
  const addOnEnter = (event: { key: string; preventDefault: () => void }) => {
    if (event.key === "Enter") {
      event.preventDefault();
      if (canAdd) {
        addInput();
      }
    }
  };

  const inputsBody = (
    <>
      {editing?.headerDrift ? (
        <div className="flex-none border-b border-wv-hairline px-4 py-3 text-[12.5px] leading-[1.5] text-wv-warn sm:px-6">
          {t("next.postProcessing.headerDriftBody")}
        </div>
      ) : null}
      {form.inputs.length === 0 ? (
        <div className="flex-none border-b border-wv-hairline px-4 py-[14px] text-[12.5px] text-wv-muted sm:px-6">
          {t("next.postProcessing.noInputs")}
        </div>
      ) : null}
      <div className="flex flex-none flex-col gap-2 border-b border-wv-hairline px-4 py-[14px] sm:px-6">
        <div className="flex flex-wrap items-center gap-2">
          <TextField
            label={t("next.postProcessing.newInputName")}
            placeholder={t("next.postProcessing.newInputName")}
            value={newName}
            className="w-[150px] max-w-full"
            onChange={(next) => {
              setNewProblem(null);
              setNewName(next);
            }}
            onKeyDown={addOnEnter}
          />
          <TextField
            label={t("next.postProcessing.newInputValue")}
            placeholder={t("next.secrets.value")}
            type={newSecret ? "password" : "text"}
            value={newValue}
            className="w-[176px] max-w-full"
            onChange={setNewValue}
            onKeyDown={addOnEnter}
          />
          <label className="flex flex-none cursor-pointer items-center gap-2 text-[12.5px] text-wv-secondary">
            <CheckBox
              checked={newSecret}
              onChange={setNewSecret}
              label={t("next.postProcessing.newInputIsSecret")}
            />
            {t("next.postProcessing.secret")}
          </label>
          <SecondaryButton icon="add" disabled={!canAdd} onClick={addInput}>
            {t("next.postProcessing.addInput")}
          </SecondaryButton>
        </div>
        {newProblem ? (
          <span role="alert" className="text-[12px] leading-[1.45] text-wv-error-text">
            {t(newProblem)}
          </span>
        ) : null}
      </div>
    </>
  );

  /* ------------------------------------------------------------ how it runs */

  const sameCategory = (left: string, right: string) => left.toLowerCase() === right.toLowerCase();
  // A category that has since been removed stays offered while it is still saved.
  const offered = [
    ...categories,
    ...form.categories.filter((saved) => !categories.some((name) => sameCategory(name, saved))),
  ];

  const runFields: FieldSpec[] = [
    ...(categoryScoped(form.trigger)
      ? [
          {
            id: "categories",
            label: t("next.postProcessing.categories"),
            help: t("next.postProcessing.categoriesHelp"),
            keywords: offered.join(" "),
            control:
              offered.length === 0
                ? { kind: "static" as const, value: t("next.postProcessing.noCategories") }
                : {
                    kind: "multiselect" as const,
                    // What is saved may differ in case from the category as it is named now.
                    values: offered.filter((name) => form.categories.some((entry) => sameCategory(entry, name))),
                    options: offered.map((name) => ({ value: name, label: name })),
                    placeholder: t("next.postProcessing.everyCategory"),
                    className: "w-[268px] max-w-full",
                    onChange: (next: string[]) => patch({ categories: next }),
                  },
          },
        ]
      : []),
    {
      id: "runMode",
      label: t("next.postProcessing.runMode"),
      help: t("next.postProcessing.runModeHelp"),
      control: {
        kind: "segmented",
        value: form.blocking ? "blocking" : "background",
        options: [
          { value: "blocking", label: t("next.postProcessing.blocking") },
          { value: "background", label: t("next.postProcessing.fireAndForget") },
        ],
        onChange: (next) => patch({ blocking: next === "blocking" }),
      },
    },
    {
      id: "timeoutSeconds",
      label: t("next.postProcessing.timeout"),
      help: t("next.postProcessing.timeoutHelp"),
      control: {
        kind: "number",
        value: form.timeoutSeconds,
        min: 0,
        max: MAX_TIMEOUT_SECONDS,
        suffix: t("next.general.seconds"),
        onChange: (next) => patch({ timeoutSeconds: next }),
      },
    },
    {
      id: "enabled",
      label: t("next.postProcessing.enabled"),
      help: t("next.postProcessing.enabledHelp"),
      control: {
        kind: "toggle",
        value: form.enabled,
        onChange: (next) => patch({ enabled: next }),
      },
    },
  ];

  // Under the choice of script: why a saved instance's script cannot run, or,
  // for a new one, why the script looked for is not among the choices.
  const NOTE = "flex-none border-b border-wv-hairline px-4 py-3 text-[12.5px] leading-[1.5] sm:px-6";
  const scriptNotes = editing ? (
    editing.scriptProblem ? (
      <div className={`${NOTE} text-wv-error-text`}>{editing.scriptProblem}</div>
    ) : undefined
  ) : scripts.length === 0 || problems.length > 0 ? (
    <>
      {scripts.length === 0 ? (
        <div className={`${NOTE} text-wv-muted`}>{t("next.postProcessing.discoveredEmpty")}</div>
      ) : null}
      {problems.length > 0 ? (
        <div role="group" aria-label={t("next.postProcessing.problems")} className={`${NOTE} flex flex-col gap-1`}>
          <span className="text-wv-muted">{t("next.postProcessing.problems")}</span>
          {problems.map((problem) => (
            <span key={problem.name} className="flex flex-wrap items-baseline gap-x-3">
              <span className="font-wv-mono text-wv-fg">{problem.name}</span>
              <span className="min-w-0 flex-1 text-wv-error-text">{problem.message}</span>
            </span>
          ))}
        </div>
      ) : null}
    </>
  ) : undefined;

  const sections: EditorSection[] = [
    {
      id: "script",
      title: t("next.postProcessing.script"),
      fields: scriptFields,
      body: scriptNotes,
    },
    ...(scheduled
      ? [{ id: "schedule", title: t("next.schedules.schedule"), fields: scheduleFields }]
      : []),
    {
      id: "inputs",
      title: t("next.postProcessing.inputs"),
      note: t("next.postProcessing.inputsNote"),
      fields: form.inputs.map(inputField),
      body: inputsBody,
    },
    {
      id: "run",
      title: t("next.postProcessing.runPolicy"),
      fields: runFields,
    },
  ];

  // Setting up from the header only has something to do while the header asks
  // for an instance the script does not have yet.
  const canSetUp = editing === null && script !== undefined && unwiredTriggers(script, instances).length > 0;

  return (
    <>
    <RecordEditor
      open
      title={editing ? editing.name : t("next.postProcessing.createInstance")}
      note={
        editing
          ? triggerTitle(t, editing.trigger, editing.queueEvent)
          : t("next.postProcessing.createInstanceNote")
      }
      width={640}
      sections={sections}
      error={error}
      busy={busy}
      saveDisabled={form.script === ""}
      onSave={() => void save()}
      onDismiss={onDismiss}
      onDelete={editing ? () => onDelete(editing) : undefined}
      extraActions={
        <>
          {editing?.headerDrift ? (
            <SecondaryButton icon="reset" disabled={busy} onClick={() => onReapply(editing)}>
              {t("next.postProcessing.reapply")}
            </SecondaryButton>
          ) : null}
          {canSetUp ? (
            <SecondaryButton
              disabled={busy}
              title={t("next.postProcessing.setUpFromHeaderHelp")}
              onClick={() => void setUp()}
            >
              {t("next.postProcessing.setUpFromHeader")}
            </SecondaryButton>
          ) : null}
        </>
      }
    />
    {creatingFor !== null ? (
      <SecretEditor
        target={{ mode: "new" }}
        onSaved={(secret) => {
          setCreated((current) => [...current, secret]);
          linkSecret(creatingFor, secret.id);
          setCreatingFor(null);
        }}
        onDismiss={() => setCreatingFor(null)}
      />
    ) : null}
    </>
  );
}
