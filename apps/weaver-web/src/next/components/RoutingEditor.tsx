import { useQuery } from "urql";
import { Link } from "react-router";
import { PROXY_PROFILES_QUERY } from "@/graphql/proxies";
import { useTranslate } from "@/lib/context/translate-context";
import {
  appendProxy,
  MAX_PROXY_ROUTES,
  moveProxy,
  proxyLabels,
  type ProxyProfile,
  type RoutingPolicy,
  type RoutingStatus,
} from "@/lib/proxies";
import { Square } from "./chrome";
import { SecondaryButton, Select, Toggle } from "./controls";
import { Icon } from "./icons";
import { WV } from "../data/palette";

/**
 * How a consumer reaches the network: an ordered list of proxy routes and
 * whether a direct connection is allowed once they have all failed.
 *
 * Lives beside the record editors rather than inside one, because a provider
 * and a feed choose their route the same way.
 */
export function RoutingEditor({
  value,
  onChange,
}: {
  value: RoutingPolicy;
  onChange: (next: RoutingPolicy) => void;
}) {
  const t = useTranslate();
  const [{ data, error }] = useQuery<{ proxyProfiles: ProxyProfile[] }>({
    query: PROXY_PROFILES_QUERY,
  });
  const profiles = data?.proxyProfiles ?? [];
  const available = profiles.filter((profile) => !value.proxyIds.includes(profile.id));

  return (
    <div className="flex w-full flex-col gap-3">
      {error ? (
        <div className="font-wv-mono text-[11.5px] text-wv-error-text">
          {t("next.routing.profilesUnreadable")}
        </div>
      ) : null}

      {value.proxyIds.length === 0 ? (
        <div className="font-wv-mono text-[11.5px] text-wv-muted">{t("next.routing.noRoutes")}</div>
      ) : (
        <ol className="flex flex-col">
          {value.proxyIds.map((id, index) => {
            const profile = profiles.find((entry) => entry.id === id);
            return (
              <li
                key={id}
                className="flex items-center gap-[10px] border-b border-wv-hairline py-2 text-[12.5px] last:border-b-0"
              >
                <Square color={profile?.enabled === false ? WV.inert : WV.accent} />
                <span className="font-wv-mono text-[11px] text-wv-faint">{index + 1}</span>
                <span className="min-w-0 flex-1 truncate">
                  {profile?.name ?? t("next.routing.proxyId", { id })}
                  {profile ? (
                    <span className="ml-2 font-wv-mono text-[11px] text-wv-muted">
                      {proxyLabels[profile.kind]}
                      {profile.enabled ? "" : ` · ${t("next.routing.disabled")}`}
                    </span>
                  ) : null}
                </span>
                <SecondaryButton
                  className="h-7 px-2"
                  title={t("next.routing.moveUp")}
                  disabled={index === 0}
                  onClick={() => onChange(moveProxy(value, index, -1))}
                >
                  <Icon name="moveUp" size={13} />
                </SecondaryButton>
                <SecondaryButton
                  className="h-7 px-2"
                  title={t("next.routing.moveDown")}
                  disabled={index === value.proxyIds.length - 1}
                  onClick={() => onChange(moveProxy(value, index, 1))}
                >
                  <Icon name="moveDown" size={13} />
                </SecondaryButton>
                <SecondaryButton
                  icon="remove"
                  className="h-7 px-2"
                  onClick={() =>
                    onChange({ ...value, proxyIds: value.proxyIds.filter((entry) => entry !== id) })
                  }
                >
                  {t("next.common.remove")}
                </SecondaryButton>
              </li>
            );
          })}
        </ol>
      )}

      <div className="flex items-center justify-between gap-3">
        {value.proxyIds.length >= MAX_PROXY_ROUTES ? (
          <span className="font-wv-mono text-[11.5px] text-wv-muted">
            {t("next.routing.full", { count: MAX_PROXY_ROUTES })}
          </span>
        ) : (
          <Select
            label={t("next.routing.addRoute")}
            value=""
            className="min-w-[240px]"
            options={[
              {
                value: "",
                label: available.length === 0 ? t("next.routing.noneAvailable") : t("next.routing.addProxy"),
              },
              ...available.map((profile) => ({
                value: String(profile.id),
                label: `${profile.name} · ${proxyLabels[profile.kind]}${profile.enabled ? "" : ` (${t("next.routing.disabled")})`}`,
              })),
            ]}
            onChange={(next) => {
              if (next) {
                onChange(appendProxy(value, Number(next)));
              }
            }}
          />
        )}
        <Link
          to="/settings/proxies"
          className="font-wv-mono text-[11.5px] text-wv-accent hover:underline"
        >
          {t("next.routing.manage")}
        </Link>
      </div>

      <div className="flex items-center justify-between gap-3 border-t border-wv-hairline pt-3">
        <span className="text-[12.5px] text-wv-muted">
          {value.allowDirect ? t("next.routing.finalDirect") : t("next.routing.finalBlocked")}
        </span>
        <Toggle
          checked={value.allowDirect}
          onChange={(next) => onChange({ ...value, allowDirect: next })}
          label={t("next.routing.allowDirect")}
        />
      </div>
      {!value.allowDirect && value.proxyIds.length === 0 ? (
        <div className="text-[12px] text-wv-warn">
          {t("next.routing.cannotConnect")}
        </div>
      ) : null}
    </div>
  );
}

/** The live outcome of the policy above, for a row or a header. */
export function RoutingState({ status }: { status?: RoutingStatus }) {
  const t = useTranslate();
  if (!status) {
    return null;
  }
  const stateKey = `next.routing.state.${status.state.toLowerCase()}`;
  const state = t(stateKey);
  return (
    <span className="font-wv-mono text-[11px] text-wv-muted">
      {state === stateKey ? status.state.toLowerCase() : state}
      {status.selectedProxyId == null
        ? ""
        : ` · ${t("next.routing.proxyId", { id: status.selectedProxyId })}`}
    </span>
  );
}
