import { useState } from "react";
import { useMutation } from "urql";
import { SET_ACCESS_POLICY_MUTATION, SET_HTTP_BIND_ADDRESS_MUTATION } from "@/graphql/queries";
import { isContainerDeployment, type AccessMode } from "@/lib/setup-flow";

/**
 * The security choices put once more to an install that predates them, shared
 * by both interfaces' upgrade pages: the same three modes, the same bind
 * question and the same writes, drawn in each one's own controls.
 */

export interface SecurityUpgradeState {
  loginEnabled: boolean;
  strictSecurity: boolean;
  bindEditable: boolean;
  /** The address the next restart uses: the stored setting, or what is running. */
  bindEffective: string;
  /** Whether this deployment can restart Weaver from the browser. */
  restartSupported: boolean;
  /** The server's refusal when it cannot, for the manual instruction. */
  restartUnsupportedReason: string | null;
  /** `native`, `docker`, or `container` — who decides network exposure. */
  deployment: string;
}

/// Loopback judged on the spelling the server reports, including the
/// IPv4-mapped form a dual-stack listener produces.
function isLoopbackAddress(value: string): boolean {
  const normalized = value.trim().toLowerCase().replace(/^::ffff:/, "");
  return normalized === "::1" || normalized.startsWith("127.");
}

/// The upgrade form: its state, which modes this deployment refuses, and the
/// writes behind Save and Keep my current setup.
export function useSecurityUpgradeForm(state: SecurityUpgradeState, onDone: () => void) {
  const [mode, setMode] = useState<AccessMode | null>(null);
  const wideNow = !isLoopbackAddress(state.bindEffective);
  const [bindWide, setBindWide] = useState(wideNow);
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [restartNote, setRestartNote] = useState(false);
  // Which mode is already stored. A policy write that succeeded must not be
  // replayed when the bind step is retried after failing — but a mode the
  // operator changed in between must still be sent.
  const [savedMode, setSavedMode] = useState<AccessMode | null>(null);
  const [, setPolicy] = useMutation(SET_ACCESS_POLICY_MUTATION);
  const [, setBindAddress] = useMutation(SET_HTTP_BIND_ADDRESS_MUTATION);

  const disabledReason = (candidate: AccessMode): string | null => {
    if (state.strictSecurity && candidate !== "login_required") {
      return "WEAVER_STRICT_SECURITY is set in this deployment's environment, which refuses trusting access modes.";
    }
    if (candidate === "no_login" && state.loginEnabled) {
      return "Your login stays. To remove it, disable login in Settings → Security first.";
    }
    return null;
  };

  // A container's exposure is its published ports, so the question is never
  // asked there — not even when nothing pins the address.
  const containerized = isContainerDeployment(state.deployment);
  // Only when the answer differs from what the next restart already does —
  // an unchanged choice must not produce a write or a restart notice.
  const bindChanges = state.bindEditable && !containerized && bindWide !== wideNow;

  const applyPolicy = async (chosen: AccessMode): Promise<boolean> => {
    const result = await setPolicy({ mode: chosen });
    if (result.error) {
      setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
      return false;
    }
    return true;
  };

  const finish = async () => {
    if (mode === null) {
      return;
    }
    setError(null);
    setSubmitting(true);
    // Policy first: it takes effect immediately, so it is the half worth
    // landing even if the bind change then fails.
    if (savedMode !== mode) {
      if (!(await applyPolicy(mode))) {
        setSubmitting(false);
        return;
      }
      setSavedMode(mode);
    }
    if (bindChanges) {
      const result = await setBindAddress({ address: bindWide ? "0.0.0.0" : "" });
      if (result.error) {
        setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
        setSubmitting(false);
        return;
      }
      setRestartNote(true);
      return;
    }
    onDone();
  };

  const keepCurrent = async () => {
    setError(null);
    setSubmitting(true);
    // The upgrader's status quo: every browser signs in, exactly as before.
    // Storing it is what stops this wizard coming back.
    if (!(await applyPolicy("login_required"))) {
      setSubmitting(false);
      return;
    }
    onDone();
  };

  return {
    mode,
    setMode,
    bindWide,
    setBindWide,
    error,
    submitting,
    restartNote,
    containerized,
    disabledReason,
    finish,
    keepCurrent,
  };
}
