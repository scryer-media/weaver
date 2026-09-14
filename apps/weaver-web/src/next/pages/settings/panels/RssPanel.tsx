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
import { useTranslate, type Translate } from "@/lib/context/translate-context";
import { directRouting, type RoutingPolicy, type RoutingStatus } from "@/lib/proxies";
import { BetaTag, Square } from "../../../components/chrome";
import { ConfirmDialog } from "../../../components/ConfirmDialog";
import { RecordEditor, type EditorSection } from "../../../components/RecordEditor";
import { PrimaryButton, SecondaryButton } from "../../../components/controls";
import { RoutingEditor, RoutingState } from "../../../components/RoutingEditor";
import { Cell } from "../../../components/rows";
import { formatDate, formatSize } from "../../../data/format";
import { WV } from "../../../data/palette";
import { countLabel } from "../../../i18n/labels";
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

/** Labels are translation keys, resolved when the panel renders. */
const ACTIONS = [
  { value: "ACCEPT", label: "next.rss.accept" },
  { value: "REJECT", label: "next.rss.reject" },
];

const DECISIONS = new Set(["submitted", "accepted", "rejected", "ignored", "error"]);

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
function sizeHelp(t: Translate, value: string): string {
  const bytes = parseBytes(value);
  return bytes === null || bytes <= 0
    ? t("next.rss.sizeHelp")
    : t("next.rss.sizeHelpValue", { size: formatSize(bytes) });
}

function pollHelp(t: Translate, seconds: number): string {
  if (seconds % 3600 === 0) {
    return countLabel(t, "next.rss.everyHours", seconds / 3600);
  }
  if (seconds % 60 === 0) {
    return countLabel(t, "next.rss.everyMinutes", seconds / 60);
  }
  return countLabel(t, "next.rss.everySeconds", seconds);
}

/** The daemon reports a decision as a lowercase word; unknown ones pass through. */
function decisionLabel(t: Translate, decision: string): string {
  const value = decision.toLowerCase();
  return DECISIONS.has(value) ? t(`next.rss.decision.${value}`) : value;
}

function feedState(t: Translate, feed: RssFeed): { color: string; text: string } {
  if (!feed.enabled) {
    return { color: WV.inert, text: t("next.rss.paused") };
  }
  if (feed.lastError) {
    return {
      color: feed.consecutiveFailures > 2 ? WV.error : WV.warn,
      text: feed.lastError,
    };
  }
  return { color: WV.accent, text: feed.lastSuccessAt ? formatDate(feed.lastSuccessAt) : t("next.rss.notPolled") };
}

function reportLine(t: Translate, report: SyncReport): string {
  const parts = [
    countLabel(t, "next.rss.feedsPolled", report.feedsPolled),
    countLabel(t, "next.rss.itemsNew", report.itemsNew),
    countLabel(t, "next.rss.itemsQueued", report.itemsSubmitted),
  ];
  if (report.itemsIgnored > 0) {
    parts.push(countLabel(t, "next.rss.itemsIgnored", report.itemsIgnored));
  }
  if (report.errors.length > 0) {
    parts.push(report.errors[0] ?? "");
  }
  return parts.filter(Boolean).join(" · ");
}

export function RssPanel() {
  const t = useTranslate();
  const [{ data, fetching }, reexecute] = useQuery<RssData>({ query: RSS_SETTINGS_QUERY });
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
    { value: NO_CATEGORY, label: t("next.rss.noCategory") },
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
      setError(t("next.rss.nameRequired"));
      return;
    }
    if (!feedForm.url.trim()) {
      setError(t("next.rss.urlRequired"));
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
      setError(t("next.rss.feedRequired"));
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
    setStatus(feed ? t("next.rss.polling", { name: feed.name }) : t("next.rss.pollingAll"));
    const result = await runSync({ feedId: feed ? feed.id : null });
    if (result.error) {
      setStatus(null);
      setError(result.error.graphQLErrors[0]?.message ?? result.error.message);
      return;
    }
    const report = result.data?.runRssSync as SyncReport | undefined;
    setStatus(report ? reportLine(t, report) : t("next.rss.syncFinished"));
    refresh();
  };

  const blocks: SettingsBlock[] = [
    {
      kind: "table",
      id: "feeds",
      title: t("next.rss.feeds"),
      note: t("next.rss.feedsNote"),
      columns: "minmax(0, 1fr) minmax(0, 1.3fr) 110px minmax(0, 1fr) 64px",
      headers: [
        t("next.rss.name"),
        t("next.rss.url"),
        t("next.rss.interval"),
        t("next.rss.lastPoll"),
        "",
      ],
      empty: t("next.rss.feedsEmpty"),
      emptyAction: { label: t("next.rss.addFeed"), onClick: () => openFeed(null) },
      onRowClick: (id) => {
        const feed = feeds.find((entry) => String(entry.id) === id);
        if (feed) {
          openFeed(feed);
        }
      },
      rows: feeds.map((feed) => {
        const state = feedState(t, feed);
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
              <SecondaryButton icon="refresh" className="h-7 px-2" onClick={() => void sync(feed)}>
                {t("next.rss.poll")}
              </SecondaryButton>
            </span>,
          ],
        };
      }),
    },
    {
      kind: "table",
      id: "rules",
      title: t("next.rss.rules"),
      note: t("next.rss.rulesNote"),
      columns: "minmax(0, 1fr) 74px 64px minmax(0, 1.4fr) minmax(0, 1fr)",
      headers: [
        t("next.rss.feed"),
        t("next.rss.action"),
        t("next.rss.order"),
        t("next.rss.titleMatches"),
        t("next.rss.filesInto"),
      ],
      empty: t("next.rss.rulesEmpty"),
      onRowClick: (id) => {
        const entry = rules.find((candidate) => String(candidate.rule.id) === id);
        if (entry) {
          openRule(entry.rule);
        }
      },
      footer:
        feeds.length > 0 ? (
          <SecondaryButton icon="add" onClick={() => openRule(null)}>{t("next.rss.addRule")}</SecondaryButton>
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
            {rule.action === "REJECT" ? t("next.rss.reject") : t("next.rss.accept")}
          </Cell>,
          <Cell key="order" mono className="text-wv-muted">
            {rule.sortOrder}
          </Cell>,
          <Cell key="regex" mono className="text-wv-secondary" title={rule.titleRegex ?? ""}>
            {rule.titleRegex || t("next.rss.anything")}
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
      title: t("next.rss.recentlySeen"),
      note: countLabel(t, "next.rss.remembered", seenItems.length),
      columns: "minmax(0, 1.6fr) minmax(0, 1fr) 96px 92px minmax(0, 140px)",
      headers: [
        t("next.rss.item"),
        t("next.rss.feed"),
        t("next.rss.decisionColumn"),
        t("next.rss.size"),
        t("next.rss.seen"),
      ],
      empty: t("next.rss.seenEmpty"),
      onRowClick: (id) => {
        const item = seenItems.find((entry) => `${entry.feedId}:${entry.itemId}` === id);
        if (item) {
          setConfirmSeen(item);
        }
      },
      footer:
        seenItems.length > 0 ? (
          <SecondaryButton icon="remove" onClick={() => setConfirmSeen("all")}>
            {t("next.rss.clearHistory")}
          </SecondaryButton>
        ) : undefined,
      rows: seenItems.map((item) => ({
        id: `${item.feedId}:${item.itemId}`,
        searchText: `${item.itemTitle} ${feedNames.get(item.feedId) ?? ""} ${decisionLabel(t, item.decision)}`,
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
            {decisionLabel(t, item.decision)}
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
      title: t("next.rss.feed"),
      fields: [
        {
          id: "name",
          label: t("next.rss.name"),
          control: {
            kind: "text",
            mono: false,
            value: feedForm.name,
            onChange: (next) => patchFeed({ name: next }),
          },
        },
        {
          id: "url",
          label: t("next.rss.url"),
          help: t("next.rss.urlHelp"),
          control: {
            kind: "text",
            type: "url",
            value: feedForm.url,
            onChange: (next) => patchFeed({ url: next }),
          },
        },
        {
          id: "enabled",
          label: t("next.rss.enabled"),
          help: t("next.rss.enabledHelp"),
          control: {
            kind: "toggle",
            value: feedForm.enabled,
            onChange: (next) => patchFeed({ enabled: next }),
          },
        },
        {
          id: "pollIntervalSecs",
          label: t("next.rss.pollInterval"),
          help: pollHelp(t, feedForm.pollIntervalSecs),
          control: {
            kind: "number",
            value: feedForm.pollIntervalSecs,
            min: 30,
            max: 86400,
            step: 30,
            suffix: t("next.general.seconds"),
            onChange: (next) => patchFeed({ pollIntervalSecs: next }),
          },
        },
        {
          id: "defaultCategory",
          label: t("next.rss.defaultCategory"),
          help: t("next.rss.defaultCategoryHelp"),
          control: {
            kind: "select",
            value: feedForm.defaultCategory,
            options: categoryOptions,
            onChange: (next) => patchFeed({ defaultCategory: next }),
          },
        },
        {
          id: "metadata",
          label: t("next.rss.defaultMetadata"),
          help: t("next.rss.defaultMetadataHelp"),
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
      title: t("next.rss.credentials"),
      note: t("next.rss.credentialsNote"),
      fields: [
        {
          id: "username",
          label: t("next.rss.username"),
          control: {
            kind: "text",
            secret: true,
            value: feedForm.username,
            onChange: (next) => patchFeed({ username: next }),
          },
        },
        {
          id: "password",
          label: t("next.rss.password"),
          help: editingFeed?.hasPassword ? t("next.rss.passwordStored") : undefined,
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
                label: t("next.rss.forgetPassword"),
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
      title: t("next.providers.networkRoute"),
      tag: <BetaTag />,
      fields: [
        {
          id: "routing",
          label: t("next.providers.proxyRoute"),
          help: t("next.rss.proxyRouteHelp"),
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
            label: t("next.rss.feed"),
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
      label: t("next.rss.action"),
      help: t("next.rss.actionHelp"),
      control: {
        kind: "segmented",
        value: ruleForm.action,
        options: ACTIONS.map((option) => ({ ...option, label: t(option.label) })),
        onChange: (next) => patchRule({ action: next as RuleAction }),
      },
    },
    {
      id: "enabled",
      label: t("next.rss.enabled"),
      control: {
        kind: "toggle",
        value: ruleForm.enabled,
        onChange: (next) => patchRule({ enabled: next }),
      },
    },
    {
      id: "sortOrder",
      label: t("next.rss.order"),
      help: t("next.rss.orderHelp"),
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
      label: t("next.rss.titleMatches"),
      help: t("next.rss.titleMatchesHelp"),
      control: {
        kind: "text",
        value: ruleForm.titleRegex,
        placeholder: "^Some\\.Show\\.S02",
        onChange: (next) => patchRule({ titleRegex: next }),
      },
    },
    {
      id: "itemCategories",
      label: t("next.rss.feedCategories"),
      help: t("next.rss.feedCategoriesHelp"),
      control: {
        kind: "text",
        value: ruleForm.itemCategories,
        placeholder: "5030, 5040",
        onChange: (next) => patchRule({ itemCategories: next }),
      },
    },
    {
      id: "minSizeBytes",
      label: t("next.rss.minSize"),
      help: sizeHelp(t, ruleForm.minSizeBytes),
      control: {
        kind: "text",
        value: ruleForm.minSizeBytes,
        className: "w-[160px]",
        onChange: (next) => patchRule({ minSizeBytes: next }),
      },
    },
    {
      id: "maxSizeBytes",
      label: t("next.rss.maxSize"),
      help: sizeHelp(t, ruleForm.maxSizeBytes),
      control: {
        kind: "text",
        value: ruleForm.maxSizeBytes,
        className: "w-[160px]",
        onChange: (next) => patchRule({ maxSizeBytes: next }),
      },
    },
    {
      id: "categoryOverride",
      label: t("next.rss.fileInto"),
      help: t("next.rss.fileIntoHelp"),
      control: {
        kind: "select",
        value: ruleForm.categoryOverride,
        options: categoryOptions,
        onChange: (next) => patchRule({ categoryOverride: next }),
      },
    },
    {
      id: "metadata",
      label: t("next.rss.metadata"),
      help: t("next.rss.metadataHelp"),
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
        <SecondaryButton icon="refresh" onClick={() => void sync()} disabled={feeds.length === 0}>
          {t("next.rss.pollAll")}
        </SecondaryButton>
        <PrimaryButton icon="add" onClick={() => openFeed(null)}>{t("next.rss.addFeed")}</PrimaryButton>
      </PanelControls>

      <SettingsBlocks blocks={blocks} loading={fetching && !data} />

      <RecordEditor
        open={feedId !== null}
        title={feedId === "new" ? t("next.rss.addFeed") : (editingFeed?.name ?? t("next.rss.feed"))}
        note={
          feedId === "new" ? (
            t("next.rss.newFeed")
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
        deleteLabel={t("next.rss.removeFeed")}
      />

      <RecordEditor
        open={ruleId !== null}
        title={ruleId === "new" ? t("next.rss.addRule") : t("next.rss.editRule")}
        note={feedNames.get(ruleForm.feedId) ?? undefined}
        width={620}
        sections={[{ id: "rule", title: t("next.rss.rule"), fields: ruleFields }]}
        error={error}
        busy={busy}
        onSave={() => void saveRule()}
        onDismiss={() => {
          setError(null);
          setRuleId(null);
        }}
        onDelete={editingRule ? () => setConfirmRule(editingRule) : undefined}
        deleteLabel={t("next.rss.removeRule")}
      />

      <ConfirmDialog
        open={confirmFeed !== null}
        title={t("next.rss.removeFeed")}
        note={confirmFeed?.name}
        busy={busy}
        confirmLabel={t("next.rss.removeFeed")}
        body={t("next.rss.removeFeedBody")}
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
        title={t("next.rss.removeRule")}
        note={confirmRule ? (feedNames.get(confirmRule.feedId) ?? undefined) : undefined}
        busy={busy}
        confirmLabel={t("next.rss.removeRule")}
        body={t("next.rss.removeRuleBody")}
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
        title={confirmSeen === "all" ? t("next.rss.clearSeenTitle") : t("next.rss.forgetItemTitle")}
        note={
          confirmSeen === "all"
            ? countLabel(t, "next.rss.items", seenItems.length)
            : confirmSeen?.itemTitle
        }
        busy={busy}
        confirmLabel={confirmSeen === "all" ? t("next.rss.clearHistory") : t("next.rss.forgetItem")}
        body={
          confirmSeen === "all" ? t("next.rss.clearSeenBody") : t("next.rss.forgetItemBody")
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
