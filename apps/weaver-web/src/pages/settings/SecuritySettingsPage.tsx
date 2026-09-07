import { useCallback, useEffect, useState } from "react";
import { Lock, LockOpen, LogOut } from "lucide-react";
import { useClient, useMutation, useQuery } from "urql";
import { ApiKeysSection, SettingsPageHeader } from "@/pages/settings/shared";
import { SectionCard } from "@/components/SectionCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { authHeaders } from "@/graphql/client";
import {
  ACCESS_POLICY_QUERY,
  CHANGE_PASSWORD_MUTATION,
  DISABLE_LOGIN_MUTATION,
  ENABLE_LOGIN_MUTATION,
  HTTP_BIND_ADDRESS_QUERY,
  LOGIN_STATUS_QUERY,
  NETWORK_ACCESS_QUERY,
  PREVIEW_NETWORK_ACCESS_QUERY,
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
  UPDATE_NETWORK_ACCESS_MUTATION,
} from "@/graphql/queries";

interface LoginStatus {
  enabled: boolean;
  username: string | null;
}

function LoginProtectionSection() {
  const [status, setStatus] = useState<LoginStatus | null>(null);
  const [authenticatedAccess, setAuthenticatedAccess] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // Form state for enabling login.
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");

  // Form state for changing password.
  const [currentPassword, setCurrentPassword] = useState("");
  const [changeNewPassword, setChangeNewPassword] = useState("");
  const [changeConfirmPassword, setChangeConfirmPassword] = useState("");
  const [verificationPassword, setVerificationPassword] = useState("");
  const [verifying, setVerifying] = useState(false);
  const [passwordVerified, setPasswordVerified] = useState(false);

  const [, enableLogin] = useMutation(ENABLE_LOGIN_MUTATION);
  const [, disableLogin] = useMutation(DISABLE_LOGIN_MUTATION);
  const [, changePassword] = useMutation(CHANGE_PASSWORD_MUTATION);
  const [{ data, fetching, error: loginStatusError }, reexecuteLoginStatus] = useQuery<{
    adminLoginStatus: LoginStatus;
  }>({
    query: LOGIN_STATUS_QUERY,
    requestPolicy: "network-only",
  });

  const refreshStatus = useCallback(() => {
    reexecuteLoginStatus({ requestPolicy: "network-only" });
  }, [reexecuteLoginStatus]);

  useEffect(() => {
    if (data?.adminLoginStatus) {
      setStatus(data.adminLoginStatus);
    }
  }, [data?.adminLoginStatus]);

  useEffect(() => {
    if (loginStatusError) {
      setError(loginStatusError.message);
    }
  }, [loginStatusError]);

  useEffect(() => {
    void fetch(new URL("api/auth/status", document.baseURI), { credentials: "include" })
      .then(async (response) => {
        if (!response.ok) return;
        const body = (await response.json()) as { authenticatedAccess?: boolean };
        setAuthenticatedAccess(body.authenticatedAccess === true);
      })
      .catch(() => {});
  }, []);

  const handleEnable = async () => {
    setError(null);
    setSuccess(null);
    if (!newUsername.trim() || !newPassword) {
      setError("Username and password are required");
      return;
    }
    if (newPassword !== confirmPassword) {
      setError("Passwords do not match");
      return;
    }
    const result = await enableLogin({
      username: newUsername.trim(),
      password: newPassword,
    });
    if (result.error) {
      setError(result.error.message);
    } else {
      setSuccess("Login protection enabled");
      setNewUsername("");
      setNewPassword("");
      setConfirmPassword("");
      refreshStatus();
    }
  };

  const handleDisable = async () => {
    setError(null);
    setSuccess(null);
    const result = await disableLogin({});
    if (result.error) {
      setError(result.error.message);
    } else {
      setSuccess("Login protection disabled");
      refreshStatus();
    }
  };

  const handleChangePassword = async () => {
    setError(null);
    setSuccess(null);
    if (!changeNewPassword) {
      setError("New password is required");
      return;
    }
    if (changeNewPassword !== changeConfirmPassword) {
      setError("New passwords do not match");
      return;
    }
    const result = await changePassword({
      currentPassword,
      newPassword: changeNewPassword,
    });
    if (result.error) {
      setError(result.error.message);
    } else {
      setSuccess("Password changed — existing sessions invalidated");
      setCurrentPassword("");
      setChangeNewPassword("");
      setChangeConfirmPassword("");
    }
  };

  const handleLogout = async () => {
    setError(null);
    try {
      const response = await fetch(new URL("api/logout", document.baseURI), {
        method: "POST",
        headers: authHeaders(),
        credentials: "include",
      });
      if (!response.ok) {
        setError("Could not sign out. Try again.");
        return;
      }
      window.location.assign(new URL(".", document.baseURI));
    } catch {
      setError("Could not reach Weaver to sign out. Try again.");
    }
  };

  const verifyPassword = async () => {
    setError(null);
    setSuccess(null);
    if (!verificationPassword) {
      setError("Enter your password to continue");
      return;
    }
    setVerifying(true);
    try {
      const response = await fetch(new URL("api/auth/verify", document.baseURI), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        credentials: "include",
        body: JSON.stringify({ password: verificationPassword }),
      });
      if (!response.ok) {
        setError(
          response.status === 429
            ? "Password verification is busy. Try again shortly."
            : "Password verification failed",
        );
        return;
      }
      setVerificationPassword("");
      setPasswordVerified(true);
      setSuccess("Password verified for this browser session for 15 minutes");
    } catch {
      setError("Could not reach Weaver to verify your password. Try again.");
    } finally {
      setVerifying(false);
    }
  };

  const handleSignOutAll = async () => {
    setError(null);
    setSuccess(null);
    if (!passwordVerified) {
      setError("Verify your password before signing out every session");
      return;
    }
    try {
      const response = await fetch(new URL("api/auth/signout-all", document.baseURI), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        credentials: "include",
      });
      if (!response.ok) {
        setPasswordVerified(false);
        setError(
          response.status === 428
            ? "Password verification expired. Verify your password and try again."
            : "Could not sign out every session. Try again.",
        );
        return;
      }
      window.location.assign(new URL(".", document.baseURI));
    } catch {
      setError("Could not reach Weaver to sign out every session. Try again.");
    }
  };

  if (fetching && !status) {
    return null;
  }

  return (
    <SectionCard
      title={
        <span className="flex items-center gap-2">
          {status?.enabled ? (
            <Lock className="size-4" />
          ) : (
            <LockOpen className="size-4" />
          )}
          Login Protection
        </span>
      }
      description={
        authenticatedAccess
          ? `Password login is required${status?.username ? ` — signed in as ${status.username}` : ""}`
          : status?.enabled
          ? `Enabled — signed in as ${status.username}`
          : "Disabled — the UI is accessible without authentication"
      }
    >
      <div className="space-y-4">
        {error ? (
          <div className="rounded-inner border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
            {error}
          </div>
        ) : null}
        {success ? (
          <div className="rounded-inner border border-status-completed/30 bg-status-completed/10 px-3 py-2 text-sm text-status-completed">
            {success}
          </div>
        ) : null}

        {!status?.enabled && !authenticatedAccess ? (
          <div className="space-y-3 rounded-inner border border-border p-5">
            <div className="space-y-1.5">
              <Label htmlFor="login-username">Username</Label>
              <Input
                id="login-username"
                value={newUsername}
                onChange={(e) => setNewUsername(e.target.value)}
                placeholder="admin"
                autoComplete="username"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="login-password">Password</Label>
              <Input
                id="login-password"
                type="password"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                autoComplete="new-password"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="login-confirm">Confirm Password</Label>
              <Input
                id="login-confirm"
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                autoComplete="new-password"
              />
            </div>
            <Button onClick={handleEnable}>
              <Lock className="size-4" />
              Enable Login
            </Button>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="space-y-3 rounded-inner border border-border p-5">
              <div className="text-sm font-semibold text-foreground">Change Password</div>
              <div className="space-y-1.5">
                <Label htmlFor="current-password">Current Password</Label>
                <Input
                  id="current-password"
                  type="password"
                  value={currentPassword}
                  onChange={(e) => setCurrentPassword(e.target.value)}
                  autoComplete="current-password"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="new-password">New Password</Label>
                <Input
                  id="new-password"
                  type="password"
                  value={changeNewPassword}
                  onChange={(e) => setChangeNewPassword(e.target.value)}
                  autoComplete="new-password"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="confirm-new-password">Confirm New Password</Label>
                <Input
                  id="confirm-new-password"
                  type="password"
                  value={changeConfirmPassword}
                  onChange={(e) => setChangeConfirmPassword(e.target.value)}
                  autoComplete="new-password"
                />
              </div>
              <Button onClick={handleChangePassword} variant="secondary">
                Change Password
              </Button>
            </div>
            <div className="space-y-3 rounded-inner border border-border p-5">
              <div className="text-sm font-semibold text-foreground">Sensitive changes</div>
              <p className="text-sm text-muted-foreground">
                Verify your password before changing access or security settings. The verification
                is limited to this browser session and expires after 15 minutes.
              </p>
              <div className="flex flex-wrap items-end gap-2">
                <div className="min-w-56 flex-1 space-y-1.5">
                  <Label htmlFor="security-verify-password">Password</Label>
                  <Input
                    id="security-verify-password"
                    type="password"
                    value={verificationPassword}
                    onChange={(event) => setVerificationPassword(event.target.value)}
                    autoComplete="current-password"
                  />
                </div>
                <Button onClick={() => void verifyPassword()} disabled={verifying} variant="secondary">
                  {verifying ? "Verifying…" : "Verify Password"}
                </Button>
                <Button onClick={() => void handleSignOutAll()} variant="destructive">
                  Sign Out Everywhere
                </Button>
              </div>
            </div>
            <div className="flex gap-2">
              <Button onClick={() => void handleLogout()} variant="outline">
                <LogOut className="size-4" />
                Sign Out
              </Button>
              {!authenticatedAccess ? (
                <Button onClick={handleDisable} variant="destructive">
                  <LockOpen className="size-4" />
                  Disable Login
                </Button>
              ) : null}
            </div>
          </div>
        )}
      </div>
    </SectionCard>
  );
}

interface BindAddressStatus {
  address: string;
  storedAddress: string | null;
  source: "ENVIRONMENT" | "SETTING" | "DEFAULT";
  editable: boolean;
  exposedWithoutLogin: boolean;
  restartRequired: boolean;
  bindFallback: string | null;
}

interface AccessPolicyStatus {
  mode: string;
  trustedNetworks: string[];
  editable: boolean;
  envPinned: boolean;
}

function NetworkAccessSection() {
  const [draft, setDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [, setBindAddress] = useMutation(SET_HTTP_BIND_ADDRESS_MUTATION);
  const [{ data, error: queryError }, refetch] = useQuery<{
    httpBindAddress: BindAddressStatus;
  }>({
    query: HTTP_BIND_ADDRESS_QUERY,
    requestPolicy: "network-only",
  });

  const status = data?.httpBindAddress ?? null;
  const savedForNextRestart = status?.storedAddress?.trim() || "127.0.0.1 (default)";
  const draftChanged = status != null && draft.trim() !== (status.storedAddress ?? "").trim();

  // The draft mirrors the STORED value only — an empty box with the loopback
  // placeholder is "not configured". Re-proposing the running address after a
  // clear would let Save silently undo the clear.
  useEffect(() => {
    setDraft(status?.storedAddress ?? "");
  }, [status]);

  const save = async () => {
    setError(null);
    setSuccess(null);
    setSaving(true);
    const result = await setBindAddress({ address: draft.trim() });
    setSaving(false);
    if (result.error) {
      setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
      return;
    }
    setSuccess(
      draft.trim().length > 0
        ? "Saved. Restart Weaver for the new address to take effect."
        : "Cleared. Weaver returns to this machine only at its next restart.",
    );
    refetch({ requestPolicy: "network-only" });
  };

  return (
    <SectionCard
      title="Network access"
      description="Which addresses Weaver answers on"
    >
      <div className="space-y-4">
        {queryError ? (
          <p className="text-sm text-destructive">{queryError.message}</p>
        ) : null}

        {status?.bindFallback ? (
          <p className="rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-500">
            {status.bindFallback}
          </p>
        ) : null}

        <p className="text-sm text-muted-foreground">
          Weaver listens on <code>127.0.0.1</code> by default, which only this
          machine can reach. Set <code>0.0.0.0</code> to answer on every
          interface, or name a single interface address. Leave empty for the
          default.
        </p>

        {status ? (
          <div className="rounded-inner border border-border bg-muted/20 p-3 text-sm">
            <div>
              Listening now: <code>{status.address}</code>
            </div>
            <div className="mt-1 text-muted-foreground">
              Saved for the next restart: <code>{savedForNextRestart}</code>
            </div>
          </div>
        ) : null}

        {status && !status.editable ? (
          <p className="text-sm text-muted-foreground">
            This is pinned by <code>WEAVER_HTTP_BIND_ADDRESS</code> in Weaver's
            environment — a container image or service unit sets it — so it
            cannot be changed here. Override the variable in your deployment
            instead. Currently listening on <code>{status.address}</code>.
          </p>
        ) : (
          <div className="flex flex-wrap items-end gap-3">
            <div className="space-y-2">
              <Label htmlFor="bind-address">Listen address</Label>
              <Input
                id="bind-address"
                value={draft}
                onChange={(event) => {
                  setDraft(event.target.value);
                  setSuccess(null);
                }}
                placeholder="127.0.0.1 (default)"
                className="w-64"
              />
            </div>
            <Button onClick={save} disabled={!status || saving || !draftChanged}>
              {saving ? "Saving…" : "Save"}
            </Button>
          </div>
        )}

        {status?.restartRequired ? (
          <p className="text-sm text-amber-500">
            {status.storedAddress ? (
              <>
                Saved as <code>{status.storedAddress}</code>.
              </>
            ) : (
              <>Address cleared.</>
            )}{" "}
            Weaver is still listening on <code>{status.address}</code> until it
            restarts.
          </p>
        ) : null}

        {status?.exposedWithoutLogin ? (
          <p className="text-sm text-amber-500">
            With this address, Weaver is reachable beyond this machine after
            the next restart while no login is configured. Enable login
            protection below, or anyone who can reach it has full
            administrative access.
          </p>
        ) : null}

        {error ? <p className="text-sm text-destructive">{error}</p> : null}
        {success ? <p className="text-sm text-emerald-500">{success}</p> : null}
      </div>
    </SectionCard>
  );
}

const ACCESS_MODE_LABELS: Record<string, string> = {
  login_required: "Login required for every browser",
  login_except_local: "Login required, except trusted local networks",
  no_login: "No login (this machine only)",
  env: "Managed by WEAVER_TRUSTED_CIDRS in the environment",
};

function AccessPolicySection() {
  const [mode, setMode] = useState<string | null>(null);
  const [networksDraft, setNetworksDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [, setPolicy] = useMutation(SET_ACCESS_POLICY_MUTATION);
  const [{ data, error: queryError }, refetch] = useQuery<{
    accessPolicy: AccessPolicyStatus;
  }>({
    query: ACCESS_POLICY_QUERY,
    requestPolicy: "network-only",
  });

  const status = data?.accessPolicy ?? null;
  const draftNetworks = networksDraft
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  const policyChanged =
    status != null &&
    (mode !== status.mode ||
      draftNetworks.join("\n") !== status.trustedNetworks.map((line) => line.trim()).join("\n"));

  useEffect(() => {
    if (status) {
      setMode(status.mode);
      setNetworksDraft(status.trustedNetworks.join("\n"));
    }
  }, [status]);

  const save = async () => {
    if (!mode) {
      return;
    }
    setError(null);
    setSuccess(null);
    setSaving(true);
    const trustedNetworks =
      mode === "login_except_local"
        ? networksDraft
            .split("\n")
            .map((line) => line.trim())
            .filter((line) => line.length > 0)
        : undefined;
    const result = await setPolicy({ mode, trustedNetworks });
    setSaving(false);
    if (result.error) {
      setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
      return;
    }
    setSuccess("Access policy updated. Applies immediately.");
    refetch({ requestPolicy: "network-only" });
  };

  return (
    <SectionCard
      title="Browser access"
      description="Who may use the web UI without signing in"
    >
      <div className="space-y-4">
        {queryError ? (
          <p className="text-sm text-destructive">{queryError.message}</p>
        ) : null}

        {status?.envPinned ? (
          <p className="text-sm text-muted-foreground">
            {ACCESS_MODE_LABELS.env}. Trusted networks:{" "}
            <code>{status.trustedNetworks.join(", ") || "none"}</code>. Change
            the variable in your deployment to edit this.
          </p>
        ) : (
          <>
            {status ? (
              <p className="rounded-inner border border-border bg-muted/20 p-3 text-sm text-muted-foreground">
                Active policy: <span className="text-foreground">{ACCESS_MODE_LABELS[status.mode] ?? status.mode}</span>
              </p>
            ) : null}
            <div className="space-y-2">
              {(["login_required", "login_except_local", "no_login"] as const).map(
                (candidate) => (
                  <label key={candidate} className="flex cursor-pointer items-start gap-2">
                    <input
                      type="radio"
                      name="access-policy-mode"
                      className="mt-1"
                      checked={mode === candidate}
                      onChange={() => {
                        setMode(candidate);
                        setSuccess(null);
                      }}
                    />
                    <span className="text-sm">{ACCESS_MODE_LABELS[candidate]}</span>
                  </label>
                ),
              )}
            </div>

            {mode === "login_except_local" ? (
              <div className="space-y-2">
                <Label htmlFor="trusted-networks">
                  Trusted networks (one CIDR per line)
                </Label>
                <textarea
                  id="trusted-networks"
                  value={networksDraft}
                  onChange={(event) => {
                    setNetworksDraft(event.target.value);
                    setSuccess(null);
                  }}
                  rows={5}
                  className="w-full max-w-md rounded-md border border-border bg-background p-2 font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground">
                  Trusted local networks can use the legacy browser policy without a login.
                  Weaver uses the socket peer unless that peer is listed in
                  <code>WEAVER_TRUSTED_PROXIES</code>; only then does it use
                  the forwarding headers supplied by that proxy.
                </p>
              </div>
            ) : null}

            <Button onClick={save} disabled={!status || !mode || saving || !policyChanged}>
              {saving ? "Saving…" : "Save"}
            </Button>
          </>
        )}

        {error ? <p className="text-sm text-destructive">{error}</p> : null}
        {success ? <p className="text-sm text-emerald-500">{success}</p> : null}
      </div>
    </SectionCard>
  );
}

interface NetworkAccessStatus {
  authenticatedAccess: boolean;
  legacyCompatibility: boolean;
  trustedNetworks: string[];
  trustedProxies: string[];
  trustedNetworksSource: string;
  proxiesSource: string;
  editable: boolean;
  envPinned: boolean;
  proxiesEditable: boolean;
  proxiesEnvPinned: boolean;
  rememberedPolicyValid: boolean;
  currentClient: {
    available: boolean;
    peer: string | null;
    resolvedClient: string | null;
    forwardingHeadersIgnored: boolean;
    rememberedClientAllowed: boolean | null;
  };
  bindAddress: BindAddressStatus;
}

interface NetworkAccessPreview {
  trustedNetworks: string[];
  trustedProxies: string[];
  bindAddress: string | null;
  restartRequired: boolean;
  currentClientAllowed: boolean | null;
}

function AuthenticatedNetworkAccessSection({
  status,
  onRefresh,
}: {
  status: NetworkAccessStatus;
  onRefresh: () => void;
}) {
  const client = useClient();
  const [networksDraft, setNetworksDraft] = useState(status.trustedNetworks.join("\n"));
  const [proxiesDraft, setProxiesDraft] = useState(status.trustedProxies.join("\n"));
  const [bindDraft, setBindDraft] = useState(status.bindAddress.storedAddress ?? "");
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const [preview, setPreview] = useState<NetworkAccessPreview | null>(null);
  const [, updateNetworkAccess] = useMutation(UPDATE_NETWORK_ACCESS_MUTATION);

  const networks = networksDraft
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const bindChanged = bindDraft.trim() !== (status.bindAddress.storedAddress ?? "").trim();
  const networksChanged = networks.join("\n") !== status.trustedNetworks.join("\n");
  const proxies = proxiesDraft
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const proxiesChanged = proxies.join("\n") !== status.trustedProxies.join("\n");
  const changed = (status.editable && networksChanged) ||
    (status.proxiesEditable && proxiesChanged) ||
    (status.bindAddress.editable && bindChanged);

  const input = (): { trustedNetworks: string[]; trustedProxies: string[]; bindAddress?: string } => {
    const next = { trustedNetworks: networks, trustedProxies: proxies };
    return status.bindAddress.editable && bindChanged
      ? { ...next, bindAddress: bindDraft.trim() }
      : next;
  };

  const runPreview = async () => {
    setError(null);
    setSuccess(null);
    setPreviewing(true);
    try {
      const result = await client.query(PREVIEW_NETWORK_ACCESS_QUERY, { input: input() }).toPromise();
      if (result.error) {
        setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
        return;
      }
      setPreview(result.data?.previewNetworkAccess ?? null);
    } catch {
      setError("Could not validate this network draft. Try again.");
    } finally {
      setPreviewing(false);
    }
  };

  const save = async () => {
    setError(null);
    setSuccess(null);
    setSaving(true);
    try {
      const result = await updateNetworkAccess({ input: input() });
      if (result.error) {
        setError(result.error.message.replace(/^\[GraphQL\]\s*/, ""));
        return;
      }
      const updated = result.data?.updateNetworkAccess;
      if (updated) {
        setNetworksDraft(updated.trustedNetworks.join("\n"));
        setProxiesDraft(updated.trustedProxies.join("\n"));
        setBindDraft(updated.bindAddress.storedAddress ?? "");
      }
      setSuccess(
        preview?.currentClientAllowed === false
          ? "Saved. This browser will no longer be eligible for remembered sessions under the new CIDRs."
          : "Saved. CIDR restrictions apply immediately; a listener address change applies after restart.",
      );
      onRefresh();
    } catch {
      setError("Could not save this network draft. Try again.");
    } finally {
      setSaving(false);
    }
  };

  const storedListener = status.bindAddress.storedAddress?.trim() || "127.0.0.1 (default)";
  return (
    <SectionCard
      title="Network access"
      description="Remembered browser sessions and the HTTP listener"
    >
      <div className="space-y-4">
        {!status.rememberedPolicyValid ? (
          <p className="rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
            The saved remembered-session CIDR policy is invalid and is currently denying remembered
            browser sessions. Correct it here before relying on remote access.
          </p>
        ) : null}
        <div className="rounded-inner border border-border bg-muted/20 p-3 text-sm">
          <div>
            Listening now: <code>{status.bindAddress.address}</code>
          </div>
          <div className="mt-1 text-muted-foreground">
            Saved for the next restart: <code>{storedListener}</code>
          </div>
        </div>
        <div className="space-y-2">
          <Label htmlFor="authenticated-trusted-networks">
            Remembered-session CIDRs (one per line)
          </Label>
          <textarea
            id="authenticated-trusted-networks"
            value={networksDraft}
            disabled={!status.editable}
            onChange={(event) => {
              setNetworksDraft(event.target.value);
              setSuccess(null);
              setPreview(null);
            }}
            rows={5}
            className="w-full max-w-md rounded-md border border-border bg-background p-2 font-mono text-sm disabled:opacity-60"
          />
          <p className="text-xs text-muted-foreground">
            Empty allows remembered sessions from any resolved client. Weaver uses the socket peer
            unless it is a configured trusted proxy; only then are forwarding headers considered.
            Configure only proxies operated by your deployment; direct peers never influence the
            resolved client address.
          </p>
        </div>
        <div className="space-y-2">
          <Label htmlFor="authenticated-trusted-proxies">
            Trusted proxy addresses or CIDRs (one per line)
          </Label>
          <textarea
            id="authenticated-trusted-proxies"
            value={proxiesDraft}
            disabled={!status.proxiesEditable}
            onChange={(event) => {
              setProxiesDraft(event.target.value);
              setSuccess(null);
              setPreview(null);
            }}
            rows={3}
            className="w-full max-w-md rounded-md border border-border bg-background p-2 font-mono text-sm disabled:opacity-60"
          />
          <p className="text-xs text-muted-foreground">
            Forwarding headers are accepted only from these proxy addresses. Keep this list narrow.
          </p>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <div className="space-y-2">
            <Label htmlFor="authenticated-bind-address">Listen address</Label>
            <Input
              id="authenticated-bind-address"
              value={bindDraft}
              disabled={!status.bindAddress.editable}
              onChange={(event) => {
                setBindDraft(event.target.value);
                setSuccess(null);
                setPreview(null);
              }}
              placeholder="127.0.0.1 (default)"
              className="w-64"
            />
          </div>
          <Button onClick={() => void runPreview()} disabled={!changed || previewing} variant="secondary">
            {previewing ? "Checking…" : "Preview changes"}
          </Button>
          <Button onClick={() => void save()} disabled={!changed || saving}>
            {saving ? "Saving…" : "Save network access"}
          </Button>
        </div>
        {preview ? (
          <p className="rounded-inner border border-border bg-muted/20 p-3 text-sm text-muted-foreground">
            This draft resolves to <code>{preview.trustedNetworks.join(", ") || "all clients"}</code>
            {preview.trustedProxies.length > 0
              ? <> with trusted proxies <code>{preview.trustedProxies.join(", ")}</code></>
              : " without trusted proxies"}.
            {preview.restartRequired ? " The listener address will apply after restart." : ""}
            {preview.currentClientAllowed === false
              ? " The current browser will not be eligible for remembered sessions."
              : ""}
          </p>
        ) : null}
        {status.envPinned || status.proxiesEnvPinned || !status.bindAddress.editable ? (
          <p className="text-sm text-muted-foreground">
            {status.envPinned ? "CIDRs are pinned by the deployment environment. " : ""}
            {status.proxiesEnvPinned ? "Trusted proxies are pinned by the deployment environment. " : ""}
            {!status.bindAddress.editable
              ? "The listener address is pinned by the deployment environment."
              : ""}
          </p>
        ) : null}
        {status.currentClient.available ? (
          <p className="text-xs text-muted-foreground">
            Current connection: peer <code>{status.currentClient.peer ?? "unknown"}</code>, resolved
            client <code>{status.currentClient.resolvedClient ?? "unknown"}</code>
            {status.currentClient.forwardingHeadersIgnored ? "; forwarding headers were ignored" : ""}.
          </p>
        ) : null}
        {error ? <p className="text-sm text-destructive">{error}</p> : null}
        {success ? <p className="text-sm text-emerald-500">{success}</p> : null}
      </div>
    </SectionCard>
  );
}

function NetworkSettingsSection() {
  const [{ data, fetching, error }, reexecute] = useQuery<{
    networkAccess: NetworkAccessStatus;
  }>({ query: NETWORK_ACCESS_QUERY, requestPolicy: "network-only" });
  if (fetching && !data) {
    return null;
  }
  if (error || !data?.networkAccess) {
    return (
      <SectionCard title="Network access" description="Network security settings">
        <p className="text-sm text-destructive">{error?.message ?? "Could not load network access settings"}</p>
      </SectionCard>
    );
  }
  if (data.networkAccess.authenticatedAccess) {
    return (
      <AuthenticatedNetworkAccessSection
        status={data.networkAccess}
        onRefresh={() => reexecute({ requestPolicy: "network-only" })}
      />
    );
  }
  return (
    <>
      <NetworkAccessSection />
      <AccessPolicySection />
    </>
  );
}

export function SecuritySettingsPage() {
  return (
    <div className="max-w-[1180px]">
      <SettingsPageHeader
        title="Security"
        description="Manage network access, login protection, and API keys"
      />
      <div className="space-y-6">
        <NetworkSettingsSection />
        <LoginProtectionSection />
        <ApiKeysSection />
      </div>
    </div>
  );
}
