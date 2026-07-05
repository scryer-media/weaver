import { useCallback, useEffect, useState } from "react";
import { Lock, LockOpen, LogOut } from "lucide-react";
import { useMutation, useQuery } from "urql";
import { ApiKeysSection, SettingsPageHeader } from "@/pages/settings/shared";
import { SectionCard } from "@/components/SectionCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  CHANGE_PASSWORD_MUTATION,
  DISABLE_LOGIN_MUTATION,
  ENABLE_LOGIN_MUTATION,
  LOGIN_STATUS_QUERY,
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

export function SecuritySettingsPage() {
  return (
    <div className="max-w-[1180px]">
      <SettingsPageHeader
        title="Security"
        description="Manage login protection and API keys"
      />
      <div className="space-y-6">
        <LoginProtectionSection />
        <ApiKeysSection />
      </div>
    </div>
  );
}
