import { useQuery } from "urql";
import { Link } from "react-router";
import { PROXY_PROFILES_QUERY } from "@/graphql/proxies";
import {
  appendProxy,
  moveProxy,
  proxyLabels,
  type ProxyProfile,
  type RoutingPolicy,
  type RoutingStatus,
} from "@/lib/proxies";
import { Square } from "./chrome";
import { SecondaryButton, Select, Toggle } from "./controls";
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
  const [{ data, error }] = useQuery<{ proxyProfiles: ProxyProfile[] }>({
    query: PROXY_PROFILES_QUERY,
  });
  const profiles = data?.proxyProfiles ?? [];
  const available = profiles.filter((profile) => !value.proxyIds.includes(profile.id));

  return (
    <div className="flex w-full flex-col gap-3">
      {error ? (
        <div className="font-wv-mono text-[11.5px] text-wv-error-text">
          Proxy profiles could not be read. Existing assignments are kept as they are.
        </div>
      ) : null}

      {value.proxyIds.length === 0 ? (
        <div className="font-wv-mono text-[11.5px] text-wv-muted">no proxy routes</div>
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
                  {profile?.name ?? `Proxy #${id}`}
                  {profile ? (
                    <span className="ml-2 font-wv-mono text-[11px] text-wv-muted">
                      {proxyLabels[profile.kind]}
                      {profile.enabled ? "" : " · disabled"}
                    </span>
                  ) : null}
                </span>
                <SecondaryButton
                  className="h-7 px-2"
                  title="Move up"
                  disabled={index === 0}
                  onClick={() => onChange(moveProxy(value, index, -1))}
                >
                  &#8593;
                </SecondaryButton>
                <SecondaryButton
                  className="h-7 px-2"
                  title="Move down"
                  disabled={index === value.proxyIds.length - 1}
                  onClick={() => onChange(moveProxy(value, index, 1))}
                >
                  &#8595;
                </SecondaryButton>
                <SecondaryButton
                  className="h-7 px-2"
                  onClick={() =>
                    onChange({ ...value, proxyIds: value.proxyIds.filter((entry) => entry !== id) })
                  }
                >
                  Remove
                </SecondaryButton>
              </li>
            );
          })}
        </ol>
      )}

      <div className="flex items-center justify-between gap-3">
        <Select
          label="Add a proxy route"
          value=""
          className="min-w-[240px]"
          options={[
            { value: "", label: available.length === 0 ? "No proxies available" : "Add a proxy…" },
            ...available.map((profile) => ({
              value: String(profile.id),
              label: `${profile.name} · ${proxyLabels[profile.kind]}${profile.enabled ? "" : " (disabled)"}`,
            })),
          ]}
          onChange={(next) => {
            if (next) {
              onChange(appendProxy(value, Number(next)));
            }
          }}
        />
        <Link
          to="/settings/proxies"
          className="font-wv-mono text-[11.5px] text-wv-accent hover:underline"
        >
          Manage proxies
        </Link>
      </div>

      <div className="flex items-center justify-between gap-3 border-t border-wv-hairline pt-3">
        <span className="text-[12.5px] text-wv-muted">
          Final route: {value.allowDirect ? "direct" : "blocked"}
        </span>
        <Toggle
          checked={value.allowDirect}
          onChange={(next) => onChange({ ...value, allowDirect: next })}
          label="Allow a direct connection as the final fallback"
        />
      </div>
      {!value.allowDirect && value.proxyIds.length === 0 ? (
        <div className="text-[12px] text-wv-warn">
          With no proxy and no direct fallback this consumer cannot connect at all.
        </div>
      ) : null}
    </div>
  );
}

/** The live outcome of the policy above, for a row or a header. */
export function RoutingState({ status }: { status?: RoutingStatus }) {
  if (!status) {
    return null;
  }
  return (
    <span className="font-wv-mono text-[11px] text-wv-muted">
      route {status.state.toLowerCase()}
      {status.selectedProxyId == null ? "" : ` · proxy #${status.selectedProxyId}`}
    </span>
  );
}
