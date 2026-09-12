import { useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import {
  ADD_RSS_FEED_MUTATION,
  ADD_RSS_RULE_MUTATION,
  CLEAR_RSS_SEEN_ITEMS_MUTATION,
  DELETE_RSS_FEED_MUTATION,
  DELETE_RSS_RULE_MUTATION,
  DELETE_RSS_SEEN_ITEM_MUTATION,
  RSS_SETTINGS_QUERY,
  RUN_RSS_SYNC_MUTATION,
  UPDATE_RSS_FEED_MUTATION,
  UPDATE_RSS_RULE_MUTATION,
} from "@/graphql/queries";
import { directRouting, type RoutingPolicy, type RoutingStatus } from "@/lib/proxies";
import { Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { SecondaryButton } from "../../../components/controls";
import { RoutingEditor, RoutingState } from "../../../components/RoutingEditor";
import { Cell } from "../../../components/rows";
import { formatDate, formatSize } from "../../../data/format";
import { WV } from "../../../data/palette";
import {
  PanelControls,
  SettingsBlocks,
  usePanelStatus,
  type FieldSpec,
  type SettingsBlock,
} from "../framework";

/**
 * RSS: the feeds weaver polls, the rules that decide what it takes from them,
 * and what it has already seen.
 *
 * Three tables rather than the classic page's nested cards: a rule belongs to
 * a feed, but it reads as one flat list of decisions, and the feed it belongs
 * to is just its first column.
 */

interface MetadataEntry {
  key: string;
  value: string;
}

type RuleAction = "ACCEPT" | "REJECT";

interface RssRule {
  id: number;
  feedId: number;
  sortOrder: number;
  enabled: boolean;
  action: RuleAction;
  titleRegex: string | null;
  itemCategories: string[];
  minSizeBytes: number | null;
  maxSizeBytes: number | null;
  categoryOverride: string | null;
  metadata: MetadataEntry[];
}

interface RssFeed {
  routing: RoutingPolicy | null;
  routingStatus?: RoutingStatus;
  id: number;
  name: string;
  url: string;
  enabled: boolean;
  pollIntervalSecs: number;
  username: string | null;
  hasPassword: boolean;
  defaultCategory: string | null;
  defaultMetadata: MetadataEntry[];
  lastPolledAt: number | null;
  lastSuccessAt: number | null;
  lastError: string | null;
  consecutiveFailures: number;
  rules: RssRule[];
}

interface SeenItem {
  feedId: number;
  itemId: string;
  itemTitle: string;
  publishedAt: number | null;
  sizeBytes: number | null;
  decision: string;
  seenAt: number;
  jobId: number | null;
  itemUrl: string | null;
  error: string | null;
}

interface SyncReport {
  feedsPolled: number;
  itemsFetched: number;
  itemsNew: number;
  itemsAccepted: number;
  itemsSubmitted: number;
  itemsIgnored: number;
  errors: string[];
}

interface RssData {
  rssFeeds: RssFeed[];
  rssSeenItems: SeenItem[];
  categories: { id: number; name: string }[];
}

interface FeedForm {
  routing: RoutingPolicy;
  name: string;
  url: string;
  enabled: boolean;
  pollIntervalSecs: number;
  username: string;
  password: string;
  clearPassword: boolean;
  defaultCategory: string;
  metadata: string;
}

interface RuleForm {
  feedId: number;
  enabled: boolean;
  sortOrder: number;
  action: RuleAction;
  titleRegex: string;
  itemCategories: string;
  minSizeBytes: string;
  maxSizeBytes: string;
  categoryOverride: string;
  metadata: string;
}

const NO_CATEGORY = "";

const NEW_FEED: FeedForm = {
  routing: directRouting,
  name: "",
  url: "",
  enabled: true,
  pollIntervalSecs: 900,
  username: "",
  password: "",
  clearPassword: false,
  defaultCategory: NO_CATEGORY,
  metadata: "",
};

const NEW_RULE: Omit<RuleForm, "feedId"> = {
  enabled: true,
  sortOrder: 0,
  action: "ACCEPT",
  titleRegex: "",
  itemCategories: "",
  minSizeBytes: "",
  maxSizeBytes: "",
  categoryOverride: NO_CATEGORY,
  metadata: "",
};

const ACTIONS = [
  { value: "ACCEPT", label: "Accept" },
  { value: "REJECT", label: "Reject" },
];

/** `key = value` per line — the same shape a feed's own metadata is written in. */
function metadataText(entries: readonly MetadataEntry[]): string {
  return entries.map((entry) => `${entry.key} = ${entry.value}`).join("\n");
}

function parseMetadata(text: string): MetadataEntry[] {
  return text
    .split(/\r?\n/)
    .map((line) => {
      const separator = line.indexOf("=");
      return separator === -1
        ? { key: line.trim(), value: "" }
        : { key: line.slice(0, separator).trim(), value: line.slice(separator + 1).trim() };
    })
    .filter((entry) => entry.key !== "");
}

function splitCommaList(value: string): string[] {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseBytes(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

/** "In bytes", and what those bytes come to once something is typed. */
function sizeHelp(value: string): string {
  const bytes = parseBytes(value);
  return bytes === null || bytes <= 0
    ? "In bytes. Blank means no limit."
    : `In bytes — ${formatSize(bytes)}.`;
}

function pollHelp(seconds: number): string {
  if (seconds % 3600 === 0) {
    return `Every ${seconds / 3600} ${seconds === 3600 ? "hour" : "hours"}.`;
  }
  if (seconds % 60 === 0) {
    return `Every ${seconds / 60} ${seconds === 60 ? "minute" : "minutes"}.`;
  }
  return `Every ${seconds} seconds.`;
}

function feedState(feed: RssFeed): { color: string; text: string } {
  if (!feed.enabled) {
    return { color: WV.inert, text: "paused" };
  }
  if (feed.lastError) {
    return {
      color: feed.consecutiveFailures > 2 ? WV.error : WV.warn,
      text: feed.lastError,
    };
  }
  return { color: WV.accent, text: feed.lastSuccessAt ? formatDate(feed.lastSuccessAt) : "not polled yet" };
}

function reportLine(report: SyncReport): string {
  const parts = [
    `${report.feedsPolled} ${report.feedsPolled === 1 ? "feed" : "feeds"} polled`,
    `${report.itemsNew} new`,
    `${report.itemsSubmitted} queued`,
  ];
  if (report.itemsIgnored > 0) {
    parts.push(`${report.itemsIgnored} ignored`);
  }
  if (report.errors.length > 0) {
    parts.push(report.errors[0] ?? "");
  }
  return parts.filter(Boolean).join(" · ");
}

export function RssPanel() {
  const [{ data }, reexecute] = useQuery<RssData>({ query: RSS_SETTINGS_QUERY });
  const [, addFeed] = useMutation(ADD_RSS_FEED_MUTATION);
  const [, updateFeed] = useMutation(UPDATE_RSS_FEED_MUTATION);
  const [, deleteFeed] = useMutation(DELETE_RSS_FEED_MUTATION);
  const [, addRule] = useMutation(ADD_RSS_RULE_MUTATION);
  const [, updateRule] = useMutation(UPDATE_RSS_RULE_MUTATION);
  const [, deleteRule] = useMutation(DELETE_RSS_RULE_MUTATION);
  const [, forgetSeenItem] = useMutation(DELETE_RSS_SEEN_ITEM_MUTATION);
  const [, clearSeenItems] = useMutation(CLEAR_RSS_SEEN_ITEMS_MUTATION);
  const [, runSync] = useMutation(RUN_RSS_SYNC_MUTATION);

  const [feedId, setFeedId] = useState<number | "new" | null>(null);
  const [feedForm, setFeedForm] = useState<FeedForm>(NEW_FEED);
  const [ruleId, setRuleId] = useState<number | "new" | null>(null);
  const [ruleForm, setRuleForm] = useState<RuleForm>({ ...NEW_RULE, feedId: 0 });
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [confirmFeed, setConfirmFeed] = useState<RssFeed | null>(null);
  const [confirmRule, setConfirmRule] = useState<RssRule | null>(null);
  const [confirmSeen, setConfirmSeen] = useState<SeenItem | "all" | null>(null);

  const feeds = useMemo(
    () => [...(data?.rssFeeds ?? [])].sort((left, right) => left.name.localeCompare(right.name)),
    [data?.rssFeeds],
  );
  const categories = useMemo(
    () => [...(data?.categories ?? [])].sort((left, right) => left.name.localeCompare(right.name)),
    [data?.categories],
  );
  const rules = useMemo(
    () =>
      feeds
        .flatMap((feed) => feed.rules.map((rule) => ({ rule, feed })))
        .sort(
          (left, right) =>
            left.feed.name.localeCompare(right.feed.name)
            || left.rule.sortOrder - right.rule.sortOrder,
        ),
    [feeds],
  );
  const seenItems = data?.rssSeenItems ?? [];
  const feedNames = useMemo(
    () => new Map(feeds.map((feed) => [feed.id, feed.name])),
    [feeds],
  );

  usePanelStatus(error ?? status, error !== null);

  const categoryOptions = [
    { value: NO_CATEGORY, label: "None" },
    ...categories.map((category) => ({ value: category.name, label: category.name })),
  ];
  const feedOptions = feeds.map((feed) => ({ value: String(feed.id), label: feed.name }));

  const editingFeed = typeof feedId === "number" ? (feeds.find((f) => f.id === feedId) ?? null) : null;
  const editingRule =
    typeof ruleId === "number" ? (rules.find((entry) => entry.rule.id === ruleId)?.rule ?? null) : null;

  const patchFeed = (next: Partial<FeedForm>) => setFeedForm((current) => ({ ...current, ...next }));
  const patchRule = (next: Partial<RuleForm>) => setRuleForm((current) => ({ ...current, ...next }));

  const openFeed = (feed: RssFeed | null) => {
    setError(null);
    setStatus(null);
    setFeedForm(
      feed
        ? {
            routing: feed.routing ?? directRouting,
            name: feed.name,
            url: feed.url,
            enabled: feed.enabled,
            pollIntervalSecs: feed.pollIntervalSecs,
            username: feed.username ?? "",
            password: "",
            clearPassword: false,
            defaultCategory: feed.defaultCategory ?? NO_CATEGORY,
            metadata: metadataText(feed.defaultMetadata),
          }
        : NEW_FEED,
    );
    setFeedId(feed ? feed.id : "new");
  };

  const openRule = (rule: RssRule | null) => {
    setError(null);
    setStatus(null);
    setRuleForm(
      rule
        ? {
            feedId: rule.feedId,
            enabled: rule.enabled,
            sortOrder: rule.sortOrder,
            action: rule.action,
            titleRegex: rule.titleRegex ?? "",
            itemCategories: rule.itemCategories.join(", "),
            minSizeBytes: rule.minSizeBytes == null ? "" : String(rule.minSizeBytes),
            maxSizeBytes: rule.maxSizeBytes == null ? "" : String(rule.maxSizeBytes),
            categoryOverride: rule.categoryOverride ?? NO_CATEGORY,
            metadata: metadataText(rule.metadata),
          }
        : {
            ...NEW_RULE,
            feedId: feeds[0]?.id ?? 0,
            sortOrder: rules.length === 0 ? 0 : Math.max(...rules.map((e) => e.rule.sortOrder)) + 1,
          },
    );
    setRuleId(rule ? rule.id : "new");
  };

  const refresh = () => reexecute({ requestPolicy: "network-only" });

  const saveFeed = async () => {
    if (!feedForm.name.trim()) {
      setError("A feed needs a name.");
      return;
    }
    if (!feedForm.url.trim()) {
      setError("A feed needs a URL.");
      return;
    }
    const input = {
      routing: { proxyIds: feedForm.routing.proxyIds, allowDirect: feedForm.routing.allowDirect },
      name: feedForm.name.trim(),
      url: feedForm.url.trim(),
      enabled: feedForm.enabled,
      pollIntervalSecs: Math.max(30, Math.round(feedForm.pollIntervalSecs || 900)),
      username: feedForm.username.trim(),
      // A blank password keeps the stored one; clearing it is explicit.
      password: feedForm.clearPassword ? "" : feedForm.password.trim() || null,
      defaultCategory: feedForm.defaultCategory,
      defaultMetadata: parseMetadata(feedForm.metadata),
    };
    setBusy(true);
    const result =
      feedId === "new" ? await addFeed({ input }) : await updateFeed({ id: feedId, input });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setError(null);
    setFeedId(null);
    refresh();
  };

  const saveRule = async () => {
    if (!ruleForm.feedId) {
      setError("Pick the feed this rule belongs to.");
      return;
    }
    const input = {
      enabled: ruleForm.enabled,
      sortOrder: ruleForm.sortOrder,
      action: ruleForm.action,
      titleRegex: ruleForm.titleRegex,
      itemCategories: splitCommaList(ruleForm.itemCategories),
      minSizeBytes: parseBytes(ruleForm.minSizeBytes),
      maxSizeBytes: parseBytes(ruleForm.maxSizeBytes),
      categoryOverride: ruleForm.categoryOverride,
      metadata: parseMetadata(ruleForm.metadata),
    };
    setBusy(true);
    const result =
      ruleId === "new"
        ? await addRule({ feedId: ruleForm.feedId, input })
        : await updateRule({ id: ruleId, input });
    setBusy(false);
    if (result.error) {
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    setError(null);
    setRuleId(null);
    refresh();
  };

  const sync = async (feed?: RssFeed) => {
    setError(null);
    setStatus(feed ? `Polling ${feed.name}…` : "Polling every feed…");
    const result = await runSync({ feedId: feed ? feed.id : null });
    if (result.error) {
      setStatus(null);
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    const report = result.data?.runRssSync as SyncReport | undefined;
    setStatus(report ? reportLine(report) : "Sync finished.");
    refresh();
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "feeds",
      title: "Feeds",
      note: "polled in the background",
      columns: "minmax(0, 1fr) minmax(0, 1.3fr) 110px minmax(0, 1fr) 64px",
      headers: ["Name", "URL", "Interval", "Last poll", ""],
      empty: "No feeds. Add one to have weaver watch an indexer's search.",
      onRowClick: (id) => {
        const feed = feeds.find((entry) => String(entry.id) === id);
        if (feed) {
          openFeed(feed);
        }
      },
      rows: feeds.map((feed) => {
        const state = feedState(feed);
        return {
          id: String(feed.id),
          searchText: `${feed.name} ${feed.url} ${feed.defaultCategory ?? ""}`,
          cells: [
            <span key="name" className="flex min-w-0 items-center gap-[10px]">
              <Square color={state.color} />
              <span className="min-w-0 truncate">{feed.name}</span>
            </span>,
            <Cell key="url" mono className="text-wv-muted" title={feed.url}>
              {feed.url}
            </Cell>,
            <Cell key="interval" mono className="text-wv-secondary">
              {feed.pollIntervalSecs}s
            </Cell>,
            <Cell key="state" mono className="text-wv-muted" title={state.text}>
              {state.text}
            </Cell>,
            <span key="sync" onClick={(event) => event.stopPropagation()}>
              <SecondaryButton className="h-7 px-2" onClick={() => void sync(feed)}>
                Poll
              </SecondaryButton>
            </span>,
          ],
        };
      }),
    },
    {
      kind: "table",
      id: "rules",
      title: "Rules",
      note: "applied in order, first match wins",
      columns: "minmax(0, 1fr) 74px 64px minmax(0, 1.4fr) minmax(0, 1fr)",
      headers: ["Feed", "Action", "Order", "Title matches", "Files into"],
      empty: "No rules. Every item a feed reports is accepted.",
      onRowClick: (id) => {
        const entry = rules.find((candidate) => String(candidate.rule.id) === id);
        if (entry) {
          openRule(entry.rule);
        }
      },
      footer:
        feeds.length > 0 ? (
          <SecondaryButton onClick={() => openRule(null)}>Add rule</SecondaryButton>
        ) : undefined,
      rows: rules.map(({ rule, feed }) => ({
        id: String(rule.id),
        searchText: `${feed.name} ${rule.action} ${rule.titleRegex ?? ""} ${rule.categoryOverride ?? ""}`,
        cells: [
          <span key="feed" className="flex min-w-0 items-center gap-[10px]">
            <Square color={rule.enabled ? WV.accent : WV.inert} />
            <span className="min-w-0 truncate">{feed.name}</span>
          </span>,
          <Cell
            key="action"
            className={rule.action === "REJECT" ? "text-wv-error-text" : "text-wv-secondary"}
          >
            {rule.action === "REJECT" ? "Reject" : "Accept"}
          </Cell>,
          <Cell key="order" mono className="text-wv-muted">
            {rule.sortOrder}
          </Cell>,
          <Cell key="regex" mono className="text-wv-secondary" title={rule.titleRegex ?? ""}>
            {rule.titleRegex || "anything"}
          </Cell>,
          <Cell key="category" className="text-wv-muted">
            {rule.categoryOverride || feed.defaultCategory || "—"}
          </Cell>,
        ],
      })),
    },
    {
      kind: "table",
      id: "seen",
      title: "Recently seen",
      note: `${seenItems.length} remembered`,
      columns: "minmax(0, 1.6fr) minmax(0, 1fr) 96px 92px minmax(0, 140px)",
      headers: ["Item", "Feed", "Decision", "Size", "Seen"],
      empty: "Nothing seen yet. Items appear here once a feed has been polled.",
      onRowClick: (id) => {
        const item = seenItems.find((entry) => `${entry.feedId}:${entry.itemId}` === id);
        if (item) {
          setConfirmSeen(item);
        }
      },
      footer:
        seenItems.length > 0 ? (
          <SecondaryButton onClick={() => setConfirmSeen("all")}>Clear history</SecondaryButton>
        ) : undefined,
      rows: seenItems.map((item) => ({
        id: `${item.feedId}:${item.itemId}`,
        searchText: `${item.itemTitle} ${feedNames.get(item.feedId) ?? ""} ${item.decision}`,
        cells: [
          <Cell key="title" className="text-wv-fg" title={item.error ?? item.itemTitle}>
            {item.itemTitle}
          </Cell>,
          <Cell key="feed" className="text-wv-muted">
            {feedNames.get(item.feedId) ?? `#${item.feedId}`}
          </Cell>,
          <Cell
            key="decision"
            mono
            className={item.error ? "text-wv-error-text" : "text-wv-secondary"}
          >
            {item.decision.toLowerCase()}
          </Cell>,
          <Cell key="size" mono className="text-wv-muted">
            {item.sizeBytes ? formatSize(item.sizeBytes) : "—"}
          </Cell>,
          <Cell key="seen" mono className="text-wv-muted">
            {formatDate(item.seenAt)}
          </Cell>,
        ],
      })),
    },
  ];

  const feedSections: EditorSection[] = [
    {
      id: "feed",
      title: "Feed",
      fields: [
        {
          id: "name",
          label: "Name",
          control: {
            kind: "text",
            mono: false,
            value: feedForm.name,
            onChange: (next) => patchFeed({ name: next }),
          },
        },
        {
          id: "url",
          label: "URL",
          help: "The indexer's RSS or Newznab search, with its API key.",
          control: {
            kind: "text",
            type: "url",
            value: feedForm.url,
            onChange: (next) => patchFeed({ url: next }),
          },
        },
        {
          id: "enabled",
          label: "Enabled",
          help: "A disabled feed keeps its rules but is never polled.",
          control: {
            kind: "toggle",
            value: feedForm.enabled,
            onChange: (next) => patchFeed({ enabled: next }),
          },
        },
        {
          id: "pollIntervalSecs",
          label: "Poll interval",
          help: pollHelp(feedForm.pollIntervalSecs),
          control: {
            kind: "number",
            value: feedForm.pollIntervalSecs,
            min: 30,
            max: 86400,
            step: 30,
            suffix: "seconds",
            onChange: (next) => patchFeed({ pollIntervalSecs: next }),
          },
        },
        {
          id: "defaultCategory",
          label: "Default category",
          help: "What an accepted item is filed under when no rule overrides it.",
          control: {
            kind: "select",
            value: feedForm.defaultCategory,
            options: categoryOptions,
            onChange: (next) => patchFeed({ defaultCategory: next }),
          },
        },
        {
          id: "metadata",
          label: "Default metadata",
          help: "One key = value per line, attached to every item this feed queues.",
          control: {
            kind: "textarea",
            value: feedForm.metadata,
            rows: 2,
            placeholder: "indexer = example",
            onChange: (next) => patchFeed({ metadata: next }),
          },
        },
      ],
    },
    {
      id: "credentials",
      title: "Credentials",
      note: "optional · stored write-only",
      fields: [
        {
          id: "username",
          label: "Username",
          control: {
            kind: "text",
            value: feedForm.username,
            onChange: (next) => patchFeed({ username: next }),
          },
        },
        {
          id: "password",
          label: "Password",
          help: editingFeed?.hasPassword ? "Stored. Leave blank to keep it." : undefined,
          control: {
            kind: "text",
            type: "password",
            value: feedForm.password,
            placeholder: editingFeed?.hasPassword ? "••••••••" : "",
            onChange: (next) => patchFeed({ password: next, clearPassword: false }),
          },
        },
        ...(editingFeed?.hasPassword
          ? [
              {
                id: "clearPassword",
                label: "Forget the stored password",
                control: {
                  kind: "toggle" as const,
                  value: feedForm.clearPassword,
                  onChange: (next: boolean) => patchFeed({ clearPassword: next, password: "" }),
                },
              },
            ]
          : []),
      ],
    },
    {
      id: "routing",
      title: "Network route",
      fields: [
        {
          id: "routing",
          label: "Proxy route",
          help: "Each route is tried in order; new polls return to the first when it recovers.",
          control: {
            kind: "custom",
            control: (
              <RoutingEditor
                value={feedForm.routing}
                onChange={(next) => patchFeed({ routing: next })}
              />
            ),
          },
        },
      ],
    },
  ];

  const ruleFields: FieldSpec[] = [
    ...(ruleId === "new"
      ? [
          {
            id: "feedId",
            label: "Feed",
            control: {
              kind: "select" as const,
              value: String(ruleForm.feedId),
              options: feedOptions,
              onChange: (next: string) => patchRule({ feedId: Number(next) }),
            },
          },
        ]
      : []),
    {
      id: "action",
      label: "Action",
      help: "The first rule that matches an item decides it.",
      control: {
        kind: "segmented",
        value: ruleForm.action,
        options: ACTIONS,
        onChange: (next) => patchRule({ action: next as RuleAction }),
      },
    },
    {
      id: "enabled",
      label: "Enabled",
      control: {
        kind: "toggle",
        value: ruleForm.enabled,
        onChange: (next) => patchRule({ enabled: next }),
      },
    },
    {
      id: "sortOrder",
      label: "Order",
      help: "Lower numbers are tried first.",
      control: {
        kind: "number",
        value: ruleForm.sortOrder,
        min: 0,
        max: 9999,
        onChange: (next) => patchRule({ sortOrder: next }),
      },
    },
    {
      id: "titleRegex",
      label: "Title matches",
      help: "A regular expression. Blank matches every title.",
      control: {
        kind: "text",
        value: ruleForm.titleRegex,
        placeholder: "^Some\\.Show\\.S02",
        onChange: (next) => patchRule({ titleRegex: next }),
      },
    },
    {
      id: "itemCategories",
      label: "Feed categories",
      help: "Comma-separated categories as the indexer labels them. Blank matches any.",
      control: {
        kind: "text",
        value: ruleForm.itemCategories,
        placeholder: "5030, 5040",
        onChange: (next) => patchRule({ itemCategories: next }),
      },
    },
    {
      id: "minSizeBytes",
      label: "Minimum size",
      help: sizeHelp(ruleForm.minSizeBytes),
      control: {
        kind: "text",
        value: ruleForm.minSizeBytes,
        className: "w-[160px]",
        onChange: (next) => patchRule({ minSizeBytes: next }),
      },
    },
    {
      id: "maxSizeBytes",
      label: "Maximum size",
      help: sizeHelp(ruleForm.maxSizeBytes),
      control: {
        kind: "text",
        value: ruleForm.maxSizeBytes,
        className: "w-[160px]",
        onChange: (next) => patchRule({ maxSizeBytes: next }),
      },
    },
    {
      id: "categoryOverride",
      label: "File into",
      help: "Overrides the feed's default category for items this rule accepts.",
      control: {
        kind: "select",
        value: ruleForm.categoryOverride,
        options: categoryOptions,
        onChange: (next) => patchRule({ categoryOverride: next }),
      },
    },
    {
      id: "metadata",
      label: "Metadata",
      help: "One key = value per line, attached to items this rule accepts.",
      control: {
        kind: "textarea",
        value: ruleForm.metadata,
        rows: 2,
        onChange: (next) => patchRule({ metadata: next }),
      },
    },
  ];

  return (
    <>
      <PanelControls>
        <SecondaryButton onClick={() => void sync()} disabled={feeds.length === 0}>
          Poll all
        </SecondaryButton>
        <SecondaryButton onClick={() => openFeed(null)}>Add feed</SecondaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} />

      <RecordEditor
        open={feedId !== null}
        title={feedId === "new" ? "Add feed" : (editingFeed?.name ?? "Feed")}
        note={
          feedId === "new" ? (
            "new feed"
          ) : editingFeed?.routingStatus ? (
            <RoutingState status={editingFeed.routingStatus} />
          ) : undefined
        }
        width={620}
        sections={feedSections}
        error={error}
        busy={busy}
        onSave={() => void saveFeed()}
        onDismiss={() => {
          setError(null);
          setFeedId(null);
        }}
        onDelete={editingFeed ? () => setConfirmFeed(editingFeed) : undefined}
        deleteLabel="Remove feed"
      />

      <RecordEditor
        open={ruleId !== null}
        title={ruleId === "new" ? "Add rule" : "Edit rule"}
        note={feedNames.get(ruleForm.feedId) ?? undefined}
        width={620}
        sections={[{ id: "rule", title: "Rule", fields: ruleFields }]}
        error={error}
        busy={busy}
        onSave={() => void saveRule()}
        onDismiss={() => {
          setError(null);
          setRuleId(null);
        }}
        onDelete={editingRule ? () => setConfirmRule(editingRule) : undefined}
        deleteLabel="Remove rule"
      />

      <ConfirmDialog
        open={confirmFeed !== null}
        title="Remove feed"
        note={confirmFeed?.name}
        busy={busy}
        confirmLabel="Remove feed"
        body="Its rules and everything it remembers seeing go with it. Transfers it already queued are untouched."
        onConfirm={() => {
          if (confirmFeed) {
            void deleteFeed({ id: confirmFeed.id }).then(() => {
              setConfirmFeed(null);
              setFeedId(null);
              refresh();
            });
          }
        }}
        onDismiss={() => setConfirmFeed(null)}
      />

      <ConfirmDialog
        open={confirmRule !== null}
        title="Remove rule"
        note={confirmRule ? (feedNames.get(confirmRule.feedId) ?? undefined) : undefined}
        busy={busy}
        confirmLabel="Remove rule"
        body="Items this rule used to decide fall through to the rules after it."
        onConfirm={() => {
          if (confirmRule) {
            void deleteRule({ id: confirmRule.id }).then(() => {
              setConfirmRule(null);
              setRuleId(null);
              refresh();
            });
          }
        }}
        onDismiss={() => setConfirmRule(null)}
      />

      <ConfirmDialog
        open={confirmSeen !== null}
        title={confirmSeen === "all" ? "Clear seen history" : "Forget this item"}
        note={confirmSeen === "all" ? `${seenItems.length} items` : confirmSeen?.itemTitle}
        busy={busy}
        confirmLabel={confirmSeen === "all" ? "Clear history" : "Forget item"}
        body={
          confirmSeen === "all"
            ? "Every feed may queue anything it reports again on its next poll."
            : "The next poll that reports this item will treat it as new and may queue it again."
        }
        onConfirm={() => {
          if (confirmSeen === "all") {
            void clearSeenItems({ feedId: null }).then(() => {
              setConfirmSeen(null);
              refresh();
            });
            return;
          }
          if (confirmSeen) {
            void forgetSeenItem({ feedId: confirmSeen.feedId, itemId: confirmSeen.itemId }).then(
              () => {
                setConfirmSeen(null);
                refresh();
              },
            );
          }
        }}
        onDismiss={() => setConfirmSeen(null)}
      />
    </>
  );
}
