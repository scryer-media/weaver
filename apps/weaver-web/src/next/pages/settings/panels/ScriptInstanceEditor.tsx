import { useMemo, useState } from "react";
import { useMutation } from "urql";
import {
  CREATE_SCRIPT_INSTANCE_MUTATION,
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
  newInstanceForm,
  triggerTitle,
  unwiredTriggers,
  withScript,
  type DiscoveredScript,
  type InstanceForm,
  type InstanceInputForm,
  type QueueEvent,
  type ScriptInstance,
  type ScriptKind,
} from "../../../data/script-instances";
import { FieldControlView, type FieldSpec } from "../framework";

/**
 * The editor of one script instance: which script, what starts it, what it is
 * given, and how it is run.
 *
 * A new instance is filled from the script's header and then belongs to the
 * operator; nothing here reads the header back into a saved instance. A secret
 * is never shown: its field starts blank, and left blank it keeps what is saved.
 */

/** What the editor was opened on: a saved instance, or a new one of a script. */
export type InstanceEditorTarget =
  | { mode: "new"; script: string | null }
  | { mode: "edit"; instance: ScriptInstance };

const CHIP = "flex h-8 cursor-pointer items-center justify-center border px-[11px] font-wv-mono text-[11.5px]";
const CHIP_ON = "border-wv-accent bg-wv-segment-active font-medium text-wv-strong";
const CHIP_OFF = "border-wv-control bg-wv-input text-wv-muted hover:border-wv-control-hover";

export function ScriptInstanceEditor({
  target,
  scripts,
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
  instances: readonly ScriptInstance[];
  /** Every category's name, for narrowing an instance to some of them. */
  categories: readonly string[];
  /** The instance was saved; `status` says so in the panel's own words. */
  onSaved: (status: string) => void;
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
      : newInstanceForm(target.script === null ? undefined : byName.get(target.script)),
  );
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [newName, setNewName] = useState("");
  const [newSecret, setNewSecret] = useState(false);
  const [newProblem, setNewProblem] = useState<string | null>(null);

  const script = byName.get(form.script);
  const patch = (next: Partial<InstanceForm>) => {
    setError(null);
    setForm((current) => ({ ...current, ...next }));
  };

  const save = async () => {
    setBusy(true);
    const input = inputFromForm(form);
    const result = editing
      ? await updateInstance({ id: editing.id, input })
      : await createInstance({ input });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    onSaved(
      t(editing ? "next.postProcessing.instanceSaved" : "next.postProcessing.instanceCreated", {
        name: input.name || input.script,
      }),
    );
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

  const declared = script?.preset.triggers ?? [];
  const marked = (name: string, isDeclared: boolean) =>
    isDeclared ? t("next.postProcessing.declaredOption", { name }) : name;

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
          label: marked(t(SCRIPT_KIND_LABELS[kind]), declared.some((entry) => entry.trigger === kind)),
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
              options: queueEvents.map((event) => ({
                value: event,
                label: marked(
                  event,
                  declared.some((entry) => entry.trigger === "QUEUE" && entry.queueEvent === event),
                ),
              })),
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

  /* ------------------------------------------------------------- its inputs */

  const setInput = (index: number, value: string) => {
    setError(null);
    setForm((current) => ({
      ...current,
      inputs: current.inputs.map((entry, at) => (at === index ? { ...entry, value } : entry)),
    }));
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
      input.secret ? t(input.stored ? "next.postProcessing.secretKeep" : "next.postProcessing.secretNew") : "",
      script && !option ? t("next.postProcessing.undeclaredInput") : "",
    ]
      .filter(Boolean)
      .join(" ");
    const onChange = (next: string) => setInput(index, next);
    const field: FieldSpec = {
      id: `input:${input.name}`,
      label,
      help: help || undefined,
      keywords: input.name,
      control: input.secret
        ? {
            kind: "text",
            type: "password",
            secret: true,
            value: input.value,
            placeholder: input.stored ? t("next.postProcessing.secretSaved") : undefined,
            onChange,
          }
        : option && option.select.length > 0
          ? {
              kind: "select",
              value: input.value,
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
    if (option) {
      return field;
    }
    // Only an input the header does not ask for can be taken away again.
    return {
      ...field,
      control: {
        kind: "custom",
        control: (
          <div className="flex min-w-0 items-center gap-2">
            <FieldControlView spec={field} />
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
      inputs: [...current.inputs, { name: newName.trim(), value: "", secret: newSecret, stored: false }],
    }));
    setNewName("");
    setNewSecret(false);
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
        <div className="flex flex-wrap items-center gap-3">
          <TextField
            label={t("next.postProcessing.newInputName")}
            placeholder={t("next.postProcessing.newInputName")}
            value={newName}
            className="w-[220px] max-w-full"
            onChange={(next) => {
              setNewProblem(null);
              setNewName(next);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                addInput();
              }
            }}
          />
          <label className="flex cursor-pointer items-center gap-2 text-[12.5px] text-wv-secondary">
            <CheckBox
              checked={newSecret}
              onChange={setNewSecret}
              label={t("next.postProcessing.newInputSecret")}
            />
            {t("next.postProcessing.secret")}
          </label>
          <SecondaryButton icon="add" disabled={newName.trim() === ""} onClick={addInput}>
            {t("next.postProcessing.addInput")}
          </SecondaryButton>
        </div>
        {newProblem ? (
          <span role="alert" className="text-[12px] leading-[1.45] text-wv-error-text">
            {t(newProblem)}
          </span>
        ) : null}
        <span className="text-[12px] leading-[1.45] text-pretty text-wv-muted">
          {t("next.postProcessing.addInputHelp")}
        </span>
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
                ? { kind: "static" as const, value: t("next.postProcessing.everyCategory") }
                : {
                    kind: "custom" as const,
                    control: (
                      <div
                        role="group"
                        aria-label={t("next.postProcessing.categories")}
                        className="flex max-w-[380px] flex-wrap justify-end gap-1.5"
                      >
                        {offered.map((name) => {
                          const active = form.categories.some((entry) => sameCategory(entry, name));
                          return (
                            <button
                              key={name}
                              type="button"
                              aria-pressed={active}
                              onClick={() =>
                                patch({
                                  categories: active
                                    ? form.categories.filter((entry) => !sameCategory(entry, name))
                                    : [...form.categories, name],
                                })
                              }
                              className={`${CHIP} ${active ? CHIP_ON : CHIP_OFF}`}
                            >
                              {name}
                            </button>
                          );
                        })}
                      </div>
                    ),
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

  const sections: EditorSection[] = [
    {
      id: "script",
      title: t("next.postProcessing.script"),
      fields: scriptFields,
      body: editing?.scriptProblem ? (
        <div className="flex-none border-b border-wv-hairline px-4 py-3 text-[12.5px] leading-[1.5] text-wv-error-text sm:px-6">
          {editing.scriptProblem}
        </div>
      ) : undefined,
    },
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
  );
}
