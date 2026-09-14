import { useLayoutEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { BrandLockup, BrandMark } from "@/lib/brand";
import { PASSWORD_MANAGER_IGNORE } from "@/lib/password-manager";
import { SETUP_CODE_DISPLAY_LENGTH } from "@/lib/setup-code";
import {
  BIND_CHOICES,
  CONTAINER_BIND_NOTE,
  ENV_PINNED_BIND_NOTE,
  MODES,
  NO_LOGIN_NETWORK_WARNING,
  RESTART_UNREACHABLE_NOTE,
  useRestartAction,
  useSetupForm,
  type SetupEnvironment,
} from "@/lib/setup-flow";
import { cn } from "@/lib/utils";
import { Eyebrow } from "../components/chrome";
import { DangerButton, PrimaryButton, SecondaryButton, TextField } from "../components/controls";

/**
 * The Next interface's first-run setup page: who can open Weaver, the
 * administrator account, and where Weaver answers.
 *
 * It shows before any credentials exist, so it is loaded on its own and draws
 * only controls that need nothing from the interface's providers. Its copy
 * comes with the shared setup flow, in English, because no translations have
 * loaded yet.
 */
export default function SetupPage({ environment }: { environment?: SetupEnvironment | null }) {
  const form = useSetupForm(environment);

  if (form.restart) {
    return (
      <SetupFrame>
        <SetupHeading
          title="Setup complete"
          body="Your choices are saved. Weaver is still listening only on this machine until it restarts — restart it now, then open it at its network address."
        />
        <RestartActions
          supported={form.restart.supported}
          unsupportedReason={form.restart.unsupportedReason}
        />
      </SetupFrame>
    );
  }

  return (
    <SetupFrame>
      <SetupHeading
        title="Set up Weaver"
        body={form.authenticatedAccess ? undefined : "Two decisions, changeable later in Settings → Security."}
      />

      {!form.authenticatedAccess ? (
        <fieldset className="flex flex-col gap-2">
          <legend className="mb-2.5">
            <Eyebrow tone="rail">Who can open Weaver?</Eyebrow>
          </legend>
          {MODES.map((candidate) => (
            <Choice
              key={candidate.id}
              name="access-mode"
              title={candidate.title}
              body={candidate.body}
              checked={form.mode === candidate.id}
              onSelect={() => form.setMode(candidate.id)}
            />
          ))}
        </fieldset>
      ) : null}

      {form.needsCredentials ? (
        <div className="flex flex-col gap-3">
          <Eyebrow tone="rail">Administrator account</Eyebrow>
          <FormField label="Username" htmlFor="setup-username">
            <TextField
              id="setup-username"
              label="Username"
              value={form.username}
              onChange={form.setUsername}
              secret
              className="w-full"
            />
          </FormField>
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <FormField label="Password" htmlFor="setup-password">
              <TextField
                id="setup-password"
                type="password"
                label="Password"
                value={form.password}
                onChange={form.setPassword}
                secret
                className="w-full"
              />
            </FormField>
            <FormField label="Confirm password" htmlFor="setup-confirm">
              <TextField
                id="setup-confirm"
                type="password"
                label="Confirm password"
                value={form.confirm}
                onChange={form.setConfirm}
                secret
                className="w-full"
              />
            </FormField>
          </div>
          {form.passwordsDiffer ? <ErrorLine>Passwords do not match.</ErrorLine> : null}
        </div>
      ) : null}

      {form.codeRequired ? (
        <FormField
          label="One-time setup code"
          htmlFor="setup-code"
          help="Enter the code shown when Weaver started. It is accepted only while setup is pending."
        >
          <input
            id="setup-code"
            value={form.setupCode}
            onChange={(event) => form.setSetupCode(event.target.value)}
            {...PASSWORD_MANAGER_IGNORE}
            autoCapitalize="characters"
            spellCheck={false}
            maxLength={SETUP_CODE_DISPLAY_LENGTH}
            placeholder="···-···"
            className="h-[42px] w-[180px] border !border-wv-control bg-wv-input px-3 font-wv-mono text-[18px] tracking-[0.35em] text-wv-strong uppercase outline-none placeholder:text-wv-faint focus:!border-wv-control-focus"
          />
        </FormField>
      ) : null}

      {form.bindQuestionApplies ? (
        <fieldset className="flex flex-col gap-2">
          <legend className="mb-2.5">
            <Eyebrow tone="rail">How can I access Weaver?</Eyebrow>
          </legend>
          {(["local", "network"] as const).map((choice) => (
            <Choice
              key={choice}
              name="bind"
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
      ) : (
        <p className="text-[12px] leading-[1.5] text-wv-muted">
          {form.containerized ? CONTAINER_BIND_NOTE : ENV_PINNED_BIND_NOTE}
        </p>
      )}

      {form.error ? <ErrorLine>{form.error}</ErrorLine> : null}

      <PrimaryButton
        onClick={() => void form.submit()}
        disabled={!form.canSubmit}
        className="w-full justify-center"
      >
        {form.submitting ? "Setting up…" : "Finish setup"}
      </PrimaryButton>
    </SetupFrame>
  );
}

/** The mark's share of the lockup's width: both viewBoxes start at x 0 and share a height. */
const MARK_SHARE_OF_LOCKUP = 553 / 2500;

function prefersReducedMotion() {
  return window.matchMedia?.("(prefers-reduced-motion: reduce)").matches === true;
}

function SetupFrame({ children }: { children: ReactNode }) {
  // The welcome plays once per page load. It stays mounted across the switch
  // to "Setup complete", so finishing setup does not play it again.
  const [intro, setIntro] = useState(() => !prefersReducedMotion());
  const lockupRef = useRef<HTMLDivElement>(null);
  const flyingRef = useRef<HTMLDivElement>(null);
  const [flight, setFlight] = useState<CSSProperties | null>(null);

  // Measured before the first paint: where the mark in the lockup sits, and how
  // far the big centred mark has to travel and shrink to land exactly on it.
  useLayoutEffect(() => {
    const lockupArt = lockupRef.current?.querySelector("svg");
    if (!intro || !lockupArt || !flyingRef.current) {
      return;
    }
    const lockup = lockupArt.getBoundingClientRect();
    const flying = flyingRef.current.getBoundingClientRect();
    const target = {
      x: lockup.left + (lockup.width * MARK_SHARE_OF_LOCKUP) / 2,
      y: lockup.top + lockup.height / 2,
    };
    setFlight({
      "--wv-intro-x": `${target.x - (flying.left + flying.width / 2)}px`,
      "--wv-intro-y": `${target.y - (flying.top + flying.height / 2)}px`,
      "--wv-intro-scale": String(lockup.height / flying.height),
    } as CSSProperties);
  }, [intro]);

  return (
    // The Next interface locks page scrolling, so the page scrolls itself.
    <div className={cn("h-dvh overflow-y-auto bg-wv-app text-wv-fg", intro && "wv-setup-intro")}>
      <div className="mx-auto flex w-full max-w-[560px] flex-col gap-8 px-4 py-10 sm:px-6 sm:py-16">
        <div ref={lockupRef} className="h-[26px] self-center">
          <BrandLockup className="block h-full w-auto text-wv-strong" />
        </div>
        <main className="wv-setup-intro-form flex flex-col gap-6 border !border-wv-control bg-wv-chrome px-5 py-6 shadow-wv-menu sm:px-7">
          {children}
        </main>
      </div>
      {intro ? (
        <div aria-hidden="true" className="pointer-events-none fixed inset-0 flex items-center justify-center">
          <div
            ref={flyingRef}
            style={flight ?? { opacity: 0 }}
            className={cn("aspect-[553/363] h-[min(38vmin,260px)]", flight && "wv-setup-intro-mark")}
            onAnimationEnd={() => setIntro(false)}
          >
            <BrandMark decorative className="block size-full" />
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SetupHeading({ title, body }: { title: string; body?: string }) {
  return (
    <div className="flex flex-col gap-1.5">
      <h1 className="font-wv-title text-[19px] font-semibold text-wv-strong">{title}</h1>
      {body ? <p className="text-[13px] leading-[1.55] text-wv-muted">{body}</p> : null}
    </div>
  );
}

function FormField({
  label,
  htmlFor,
  help,
  children,
}: {
  label: string;
  htmlFor: string;
  help?: string;
  children: ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-[7px]">
      <label htmlFor={htmlFor} className="text-[12px] font-medium text-wv-secondary">
        {label}
      </label>
      {children}
      {help ? <span className="text-[11.5px] leading-[1.45] text-wv-muted">{help}</span> : null}
    </div>
  );
}

function ErrorLine({ children }: { children: ReactNode }) {
  return (
    <div role="alert" className="text-[12.5px] leading-[1.45] text-wv-error-text">
      {children}
    </div>
  );
}

/**
 * One option of a choice. The native radio stays for keyboard and screen
 * readers; the square beside the title is what shows. Borders use `!` because
 * the global unlayered `* { border-color }` rule otherwise wins.
 */
function Choice({
  name,
  title,
  body,
  checked,
  onSelect,
}: {
  name: string;
  title: string;
  body: string;
  checked: boolean;
  onSelect: () => void;
}) {
  return (
    <label
      className={cn(
        "flex cursor-pointer items-start gap-3 border px-3.5 py-3 has-[:focus-visible]:!border-wv-control-focus",
        checked
          ? "!border-wv-accent bg-[rgb(63_179_156_/_0.07)]"
          : "!border-wv-control hover:!border-wv-control-focus",
      )}
    >
      <input type="radio" name={name} checked={checked} onChange={onSelect} className="sr-only" />
      <span
        aria-hidden="true"
        className={cn(
          "mt-[3px] flex size-3.5 flex-none items-center justify-center border",
          checked ? "!border-wv-accent" : "!border-wv-faint",
        )}
      >
        {checked ? <span className="size-1.5 bg-wv-accent" /> : null}
      </span>
      <span className="flex min-w-0 flex-col gap-1">
        <span className="text-[13px] font-medium text-wv-fg">{title}</span>
        <span className="text-[12px] leading-[1.5] text-wv-muted">{body}</span>
      </span>
    </label>
  );
}

/// Restart where that is safe; otherwise the manual instruction and a way on.
function RestartActions({
  supported,
  unsupportedReason,
}: {
  supported: boolean;
  unsupportedReason: string | null;
}) {
  const { phase, error, restart } = useRestartAction();
  const onContinue = () => window.location.reload();

  if (!supported) {
    return (
      <div className="flex flex-col gap-3">
        {unsupportedReason ? (
          <p className="text-[12.5px] leading-[1.5] text-wv-muted">{unsupportedReason}</p>
        ) : null}
        <PrimaryButton onClick={onContinue} className="w-full justify-center">
          Continue on this machine
        </PrimaryButton>
      </div>
    );
  }

  if (phase === "restarting") {
    return (
      <p role="status" className="text-[12.5px] leading-[1.5] text-wv-muted">
        Restarting Weaver. This page reloads by itself as soon as Weaver answers again.
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-3">
      {phase === "unreachable" ? (
        <p className="text-[12.5px] leading-[1.5] text-wv-muted">{RESTART_UNREACHABLE_NOTE}</p>
      ) : null}
      {error ? <ErrorLine>{error}</ErrorLine> : null}
      {phase === "idle" ? (
        <DangerButton onClick={() => void restart()} className="w-full">
          Restart Weaver
        </DangerButton>
      ) : null}
      <SecondaryButton onClick={onContinue} className="w-full justify-center">
        Continue on this machine
      </SecondaryButton>
    </div>
  );
}
