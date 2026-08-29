import { useCallback, useEffect, useState } from "react";
import { Lock, LockOpen, LogOut } from "lucide-react";
import { useMutation, useQuery } from "urql";
import { ApiKeysSection, SettingsPageHeader } from "@/pages/settings/shared";
import { SectionCard } from "@/components/SectionCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  ACCESS_POLICY_QUERY,
  CHANGE_PASSWORD_MUTATION,
  DISABLE_LOGIN_MUTATION,
  ENABLE_LOGIN_MUTATION,
  HTTP_BIND_ADDRESS_QUERY,
  LOGIN_STATUS_QUERY,
  SET_ACCESS_POLICY_MUTATION,
  SET_HTTP_BIND_ADDRESS_MUTATION,
} from "@/graphql/queries";

interface LoginStatus {
  enabled: boolean;
  username: string | null;
}

function LoginProtectionSection() {
  const [status, setStatus] = useState<LoginStatus | null>(null);
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

  const handleLogout = () => {
    fetch("/api/logout", { method: "POST" }).then(() => {
      window.location.href = "/";
    });
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
        status?.enabled
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

        {!status?.enabled ? (
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
            <div className="flex gap-2">
              <Button onClick={handleLogout} variant="outline">
                <LogOut className="size-4" />
                Sign Out
              </Button>
              <Button onClick={handleDisable} variant="destructive">
                <LockOpen className="size-4" />
                Disable Login
              </Button>
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
            <Button onClick={save} disabled={!status || saving}>
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
                  Judged on the address a connection actually arrives from —
                  behind a reverse proxy that is the proxy, and trusting it
                  trusts everyone it forwards.
                </p>
              </div>
            ) : null}

            <Button onClick={save} disabled={!status || !mode || saving}>
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

export function SecuritySettingsPage() {
  return (
    <div className="max-w-[1180px]">
      <SettingsPageHeader
        title="Security"
        description="Manage network access, login protection, and API keys"
      />
      <div className="space-y-6">
        <NetworkAccessSection />
        <AccessPolicySection />
        <LoginProtectionSection />
        <ApiKeysSection />
      </div>
    </div>
  );
}
