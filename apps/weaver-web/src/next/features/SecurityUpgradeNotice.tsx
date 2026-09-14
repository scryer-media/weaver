import { useEffect, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  DISMISS_SECURITY_UPGRADE_NOTICE_MUTATION,
  SECURITY_UPGRADE_NOTICE_QUERY,
} from "@/graphql/queries";
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { Dialog } from "../components/Dialog";
import { PrimaryButton } from "../components/controls";
import { Icon } from "../components/icons";

const GUIDE_URL =
  "https://www.scryer.media/weaver/docs/guides/security-and-access/#installs-that-predate-0120";

interface SecurityUpgradeNoticeState {
  securityUpgradeNotice: {
    pending: boolean;
    deployment: string;
    operatingSystem: string;
    loginEnabled: boolean;
  };
}

/** What to run, and what to do after it, for where this install runs. */
function moveSteps(
  t: Translate,
  deployment: string,
  operatingSystem: string,
): { lead: string; code: string | null; after: string | null } {
  if (deployment === "docker" || deployment === "container") {
    return {
      lead: t("next.securityNotice.container"),
      code: "environment:\n  WEAVER_ACCESS_MODE: authenticated",
      after: t("next.securityNotice.containerAfter"),
    };
  }
  switch (operatingSystem) {
    case "windows":
      return {
        lead: t("next.securityNotice.windows"),
        code: "setx WEAVER_ACCESS_MODE authenticated",
        after: t("next.securityNotice.windowsAfter"),
      };
    case "macos":
      return {
        lead: t("next.securityNotice.macos"),
        code: "launchctl setenv WEAVER_ACCESS_MODE authenticated",
        after: t("next.securityNotice.macosAfter"),
      };
    case "linux":
      return {
        lead: t("next.securityNotice.linux"),
        code: "Environment=WEAVER_ACCESS_MODE=authenticated",
        after: t("next.securityNotice.linuxAfter"),
      };
    default:
      return { lead: t("next.securityNotice.other"), code: null, after: null };
  }
}

/**
 * Tells an install still on the access settings from before 0.12.0, once,
 * that a simpler model exists and how to move to it.
 *
 * Moving takes an environment variable and a restart, which nothing in the
 * interface can do for the operator, so this explains rather than asks. The
 * server keeps whether it was seen, so it shows once per install; closing it
 * any way at all counts. If the question cannot be answered, nothing shows.
 */
export function SecurityUpgradeNotice() {
  const t = useTranslate();
  const [open, setOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [{ data }] = useQuery<SecurityUpgradeNoticeState>({
    query: SECURITY_UPGRADE_NOTICE_QUERY,
    requestPolicy: "network-only",
  });
  const [dismissState, dismissNotice] = useMutation(DISMISS_SECURITY_UPGRADE_NOTICE_MUTATION);
  const notice = data?.securityUpgradeNotice;

  // Opened once per page load; a refetch never reopens a closed notice.
  const [decided, setDecided] = useState(false);
  useEffect(() => {
    if (!decided && notice) {
      setDecided(true);
      setOpen(notice.pending);
    }
  }, [decided, notice]);

  if (!notice) {
    return null;
  }

  const dismiss = () => {
    setError(null);
    void dismissNotice({}).then((result) => {
      if (result.error) {
        setError(t("next.securityNotice.dismissFailed", { message: result.error.message }));
        return;
      }
      setOpen(false);
    });
  };

  const steps = moveSteps(t, notice.deployment, notice.operatingSystem);

  return (
    <Dialog
      open={open}
      title={t("next.securityNotice.title")}
      width={600}
      onDismiss={dismiss}
      footer={
        <>
          <a
            href={GUIDE_URL}
            target="_blank"
            rel="noreferrer noopener"
            className="flex h-[34px] items-center justify-center whitespace-nowrap border border-wv-control bg-wv-button px-[14px] text-[13px] font-medium text-wv-fg hover:border-wv-control-hover hover:bg-wv-button-hover"
          >
            <Icon name="external" size={14} className="-ml-[2px] mr-[7px] flex-none" />
            {t("next.securityNotice.guide")}
          </a>
          <PrimaryButton disabled={dismissState.fetching} onClick={dismiss}>
            {t("next.securityNotice.dismiss")}
          </PrimaryButton>
        </>
      }
    >
      <div className="flex flex-col gap-5 px-4 py-5 text-[13px] leading-[1.6] sm:px-6">
        <p className="text-wv-secondary">{t("next.securityNotice.intro")}</p>

        <section className="flex flex-col gap-2">
          <h3 className="font-wv-title text-[13.5px] font-semibold text-wv-fg">
            {t("next.securityNotice.changesTitle")}
          </h3>
          <ul className="flex list-disc flex-col gap-1 pl-5 text-wv-muted marker:text-wv-faint">
            <li>{t("next.securityNotice.changeSignIn")}</li>
            <li>{t("next.securityNotice.changeProxies")}</li>
            <li>{t("next.securityNotice.changeHosts")}</li>
          </ul>
        </section>

        <section className="flex flex-col gap-2">
          <h3 className="font-wv-title text-[13.5px] font-semibold text-wv-fg">
            {t("next.securityNotice.stepsTitle")}
          </h3>
          <p className="text-wv-muted">{steps.lead}</p>
          {steps.code === null ? null : (
            <pre className="overflow-x-auto bg-wv-input px-3 py-2 font-wv-mono text-[12px] leading-[1.55] text-wv-fg">
              {steps.code}
            </pre>
          )}
          {steps.after === null ? null : <p className="text-wv-muted">{steps.after}</p>}
          <p className="text-wv-muted">
            {notice.loginEnabled
              ? t("next.securityNotice.keepsLogin")
              : t("next.securityNotice.createsLogin")}
          </p>
        </section>

        {error === null ? null : <p className="text-[12.5px] text-wv-error-text">{error}</p>}
      </div>
    </Dialog>
  );
}
