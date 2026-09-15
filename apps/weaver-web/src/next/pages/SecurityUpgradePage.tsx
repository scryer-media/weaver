import { BIND_CHOICES, CONTAINER_BIND_NOTE, MODES, NO_LOGIN_NETWORK_WARNING } from "@/lib/setup-flow";
import { useSecurityUpgradeForm, type SecurityUpgradeState } from "@/lib/security-upgrade";
import { Eyebrow } from "../components/chrome";
import { PrimaryButton, SecondaryButton } from "../components/controls";
import { Choice, ErrorLine, RestartActions, SetupFrame, SetupHeading } from "./SetupPage";

/**
 * The Next interface's page for an install that predates the security choices:
 * the same three modes and bind question as first-run setup, with a way to
 * keep what it has. Like setup, it shows before the interface's providers
 * mount, so its copy is English.
 */
export default function SecurityUpgradePage({
  state,
  onDone,
}: {
  state: SecurityUpgradeState;
  onDone: () => void;
}) {
  const form = useSecurityUpgradeForm(state, onDone);

  if (form.restartNote) {
    return (
      <SetupFrame welcome={false}>
        <SetupHeading
          title="Settings saved"
          body="Your browser access choice applies now. The network address change waits for a restart — restart Weaver, then open it at its new address."
        />
        <RestartActions
          supported={state.restartSupported}
          unsupportedReason={state.restartUnsupportedReason}
          onContinue={onDone}
        />
      </SetupFrame>
    );
  }

  return (
    <SetupFrame welcome={false}>
      <SetupHeading
        title="Weaver added security options"
        body="This version lets you choose how browsers get in. Pick one, or keep what you have. Changeable later in Settings → Security."
      />

      <fieldset className="flex flex-col gap-2">
        <legend className="mb-2.5">
          <Eyebrow tone="rail">Who can open Weaver?</Eyebrow>
        </legend>
        {MODES.map((candidate) => (
          <Choice
            key={candidate.id}
            name="upgrade-access-mode"
            title={candidate.title}
            body={candidate.body}
            checked={form.mode === candidate.id}
            disabledReason={form.disabledReason(candidate.id)}
            onSelect={() => form.setMode(candidate.id)}
          />
        ))}
      </fieldset>

      {form.containerized ? (
        <p className="text-[12px] leading-[1.5] text-wv-muted">{CONTAINER_BIND_NOTE}</p>
      ) : state.bindEditable ? (
        <fieldset className="flex flex-col gap-2">
          <legend className="mb-2.5">
            <Eyebrow tone="rail">How can I access Weaver?</Eyebrow>
          </legend>
          {(["local", "network"] as const).map((choice) => (
            <Choice
              key={choice}
              name="upgrade-bind"
              title={BIND_CHOICES[choice].title}
              body={BIND_CHOICES[choice].body}
              checked={form.bindWide === (choice === "network")}
              onSelect={() => form.setBindWide(choice === "network")}
            />
          ))}
          {form.bindWide && form.mode === "no_login" ? (
            <p className="pt-1 text-[12px] leading-[1.5] text-wv-warn">{NO_LOGIN_NETWORK_WARNING}</p>
          ) : null}
        </fieldset>
      ) : null}

      {form.error ? <ErrorLine>{form.error}</ErrorLine> : null}

      <div className="flex flex-col gap-3">
        <PrimaryButton
          onClick={() => void form.finish()}
          disabled={form.mode === null || form.submitting}
          className="w-full justify-center"
        >
          {form.submitting ? "Saving…" : "Save"}
        </PrimaryButton>
        <SecondaryButton
          onClick={() => void form.keepCurrent()}
          disabled={form.submitting}
          className="w-full justify-center"
        >
          Keep my current setup
        </SecondaryButton>
      </div>
    </SetupFrame>
  );
}
