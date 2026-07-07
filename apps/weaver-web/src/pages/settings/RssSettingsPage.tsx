import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { FolderPathInput } from "@/components/FolderPathInput";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { formatBytes } from "@/components/SpeedDisplay";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { cn } from "@/lib/utils";
import {
  ADD_RSS_FEED_MUTATION,
  ADD_RSS_RULE_MUTATION,
  CLEAR_RSS_SEEN_ITEMS_MUTATION,
  DELETE_RSS_FEED_MUTATION,
  DELETE_RSS_SEEN_ITEM_MUTATION,
  DELETE_RSS_RULE_MUTATION,
  RSS_SETTINGS_QUERY,
  RUN_RSS_SYNC_MUTATION,
  SCAN_WATCH_FOLDER_MUTATION,
  UPDATE_SETTINGS_MUTATION,
  UPDATE_RSS_FEED_MUTATION,
  UPDATE_RSS_RULE_MUTATION,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";

type MetadataEntry = {
  key: string;
  value: string;
};

type Category = {
  id: number;
  name: string;
};

type RssRuleAction = "ACCEPT" | "REJECT";

type RssRule = {
  id: number;
  feedId: number;
  sortOrder: number;
  enabled: boolean;
  action: RssRuleAction;
  titleRegex: string | null;
  itemCategories: string[];
  minSizeBytes: number | null;
  maxSizeBytes: number | null;
  categoryOverride: string | null;
  metadata: MetadataEntry[];
};

type RssFeed = {
  id: number;
  name: string;
  url: string;
  enabled: boolean;
  pollIntervalSecs: number;
  username: string | null;
  hasPassword: boolean;
  defaultCategory: string | null;
  defaultMetadata: MetadataEntry[];
  etag: string | null;
  lastModified: string | null;
  lastPolledAt: number | null;
  lastSuccessAt: number | null;
  lastError: string | null;
  consecutiveFailures: number;
  rules: RssRule[];
};

type RssFeedSyncResult = {
  feedId: number;
  feedName: string;
  itemsFetched: number;
  itemsNew: number;
  itemsAccepted: number;
  itemsSubmitted: number;
  itemsIgnored: number;
  errors: string[];
};

type RssSyncReport = {
  feedsPolled: number;
  itemsFetched: number;
  itemsNew: number;
  itemsAccepted: number;
  itemsSubmitted: number;
  itemsIgnored: number;
  errors: string[];
  feedResults: RssFeedSyncResult[];
};

type WatchFolderMode = "off" | "polling" | "realtime";

type WatchFolderSettings = {
  mode: WatchFolderMode;
  path: string | null;
  pollIntervalSecs: number;
  stabilitySecs: number;
  categoryFromSubfolders: boolean;
  scanningPaused: boolean;
};

type WatchFolderFormValues = {
  mode: WatchFolderMode;
  path: string;
  pollIntervalSecs: number;
  stabilitySecs: number;
  categoryFromSubfolders: boolean;
  scanningPaused: boolean;
};

type WatchFolderScanIssue = {
  path: string;
  reason: string;
};

type WatchFolderMarkerRename = {
  from: string;
  to: string;
  marker: string;
};

type WatchFolderScanReport = {
  discoveredFiles: string[];
  queuedNzbs: string[];
  skippedInputs: WatchFolderScanIssue[];
  permanentErrors: WatchFolderScanIssue[];
  transientErrors: WatchFolderScanIssue[];
  markerRenamedSources: WatchFolderMarkerRename[];
};

type RssSettingsData = {
  settings: {
    watchFolder: WatchFolderSettings;
  };
  rssFeeds: RssFeed[];
  rssSeenItems: RssSeenItem[];
  categories: Category[];
};

type RssSeenItem = {
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
};

type FeedFormValues = {
  name: string;
  url: string;
  enabled: boolean;
  pollIntervalSecs: number;
  username: string;
  password: string;
  clearPassword: boolean;
  defaultCategory: string;
  defaultMetadata: MetadataEntry[];
};

type RuleFormValues = {
  enabled: boolean;
  sortOrder: number;
  action: RssRuleAction;
  titleRegex: string;
  itemCategories: string;
  minSizeBytes: string;
  maxSizeBytes: string;
  categoryOverride: string;
  metadata: MetadataEntry[];
};

const NONE_VALUE = "__none__";

const defaultFeedForm: FeedFormValues = {
  name: "",
  url: "",
  enabled: true,
  pollIntervalSecs: 900,
  username: "",
  password: "",
  clearPassword: false,
  defaultCategory: "",
  defaultMetadata: [],
};

const defaultRuleForm: RuleFormValues = {
  enabled: true,
  sortOrder: 0,
  action: "ACCEPT",
  titleRegex: "",
  itemCategories: "",
  minSizeBytes: "",
  maxSizeBytes: "",
  categoryOverride: "",
  metadata: [],
};

const defaultWatchFolderSettings: WatchFolderSettings = {
  mode: "off",
  path: null,
  pollIntervalSecs: 60,
  stabilitySecs: 3,
  categoryFromSubfolders: true,
  scanningPaused: false,
};

function watchFolderFormFromSettings(settings: WatchFolderSettings): WatchFolderFormValues {
  return {
    mode: settings.mode,
    path: settings.path ?? "",
    pollIntervalSecs: settings.pollIntervalSecs,
    stabilitySecs: settings.stabilitySecs,
    categoryFromSubfolders: settings.categoryFromSubfolders,
    scanningPaused: settings.scanningPaused,
  };
}

export function RssSettingsPage() {
  const t = useTranslate();
  const [{ data, fetching, error }, reexecuteQuery] = useQuery<RssSettingsData>({
    query: RSS_SETTINGS_QUERY,
  });
  const [addFeedState, addFeed] = useMutation(ADD_RSS_FEED_MUTATION);
  const [updateFeedState, updateFeed] = useMutation(UPDATE_RSS_FEED_MUTATION);
  const [, deleteFeed] = useMutation(DELETE_RSS_FEED_MUTATION);
  const [addRuleState, addRule] = useMutation(ADD_RSS_RULE_MUTATION);
  const [updateRuleState, updateRule] = useMutation(UPDATE_RSS_RULE_MUTATION);
  const [, deleteRule] = useMutation(DELETE_RSS_RULE_MUTATION);
  const [, deleteSeenItem] = useMutation(DELETE_RSS_SEEN_ITEM_MUTATION);
  const [, clearSeenItems] = useMutation(CLEAR_RSS_SEEN_ITEMS_MUTATION);
  const [runSyncState, runSync] = useMutation(RUN_RSS_SYNC_MUTATION);
  const [updateSettingsState, updateSettings] = useMutation(UPDATE_SETTINGS_MUTATION);
  const [scanWatchFolderState, scanWatchFolder] = useMutation(SCAN_WATCH_FOLDER_MUTATION);

  const feeds = useMemo(
    () =>
      [...(data?.rssFeeds ?? [])].sort((left, right) =>
        left.name.localeCompare(right.name),
      ),
    [data?.rssFeeds],
  );
  const categories = useMemo(
    () => [...(data?.categories ?? [])].sort((left, right) => left.name.localeCompare(right.name)),
    [data?.categories],
  );
  const seenItemsByFeed = useMemo(() => {
    const grouped = new Map<number, RssSeenItem[]>();
    for (const item of data?.rssSeenItems ?? []) {
      const existing = grouped.get(item.feedId);
      if (existing) {
        existing.push(item);
      } else {
        grouped.set(item.feedId, [item]);
      }
    }
    return grouped;
  }, [data?.rssSeenItems]);
  const totalSeenItems = data?.rssSeenItems?.length ?? 0;
  const watchFolderSettings = data?.settings?.watchFolder ?? defaultWatchFolderSettings;

  const [editingFeed, setEditingFeed] = useState<RssFeed | null>(null);
  const [showFeedForm, setShowFeedForm] = useState(false);
  const [ruleEditor, setRuleEditor] = useState<{ feedId: number; rule: RssRule | null } | null>(
    null,
  );
  const [deleteFeedId, setDeleteFeedId] = useState<number | null>(null);
  const [deleteRuleTarget, setDeleteRuleTarget] = useState<{
    feedId: number;
    ruleId: number;
  } | null>(null);
  const [clearSeenTarget, setClearSeenTarget] = useState<number | "all" | null>(null);
  const [syncTarget, setSyncTarget] = useState<number | "all" | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [syncReport, setSyncReport] = useState<RssSyncReport | null>(null);
  const [watchFolderValues, setWatchFolderValues] = useState<WatchFolderFormValues>(
    watchFolderFormFromSettings(defaultWatchFolderSettings),
  );
  const [watchFolderScanReport, setWatchFolderScanReport] =
    useState<WatchFolderScanReport | null>(null);
  const isSavingFeed = addFeedState.fetching || updateFeedState.fetching;
  const isSavingRule = addRuleState.fetching || updateRuleState.fetching;

  useEffect(() => {
    if (error) {
      setErrorMessage(error.message);
    }
  }, [error]);

  useEffect(() => {
    setWatchFolderValues(watchFolderFormFromSettings(watchFolderSettings));
  }, [watchFolderSettings]);

  const refresh = () => {
    reexecuteQuery({ requestPolicy: "network-only" });
  };

  const resetFeedback = () => {
    setErrorMessage(null);
    setNotice(null);
  };

  const openAddFeed = () => {
    resetFeedback();
    setEditingFeed(null);
    setShowFeedForm(true);
  };

  const openEditFeed = (feed: RssFeed) => {
    resetFeedback();
    setEditingFeed(feed);
    setShowFeedForm(true);
  };

  const closeFeedForm = () => {
    setEditingFeed(null);
    setShowFeedForm(false);
  };

  const openAddRule = (feedId: number) => {
    resetFeedback();
    setRuleEditor({ feedId, rule: null });
  };

  const openEditRule = (feedId: number, rule: RssRule) => {
    resetFeedback();
    setRuleEditor({ feedId, rule });
  };

  const closeRuleEditor = () => {
    setRuleEditor(null);
  };

  const handleFeedSave = async (values: FeedFormValues) => {
    resetFeedback();
    const input = {
      name: values.name.trim(),
      url: values.url.trim(),
      enabled: values.enabled,
      pollIntervalSecs: values.pollIntervalSecs,
      username: values.username,
      password: editingFeed
        ? values.clearPassword
          ? ""
          : values.password.trim() || null
        : values.password.trim() || null,
      defaultCategory: values.defaultCategory,
      defaultMetadata: serializeMetadata(values.defaultMetadata),
    };

    const result = editingFeed
      ? await updateFeed({ id: editingFeed.id, input })
      : await addFeed({ input });

    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }

    setNotice(editingFeed ? t("rss.feedUpdated") : t("rss.feedAdded"));
    closeFeedForm();
    refresh();
  };

  const handleRuleSave = async (feedId: number, ruleId: number | null, values: RuleFormValues) => {
    resetFeedback();
    const input = {
      enabled: values.enabled,
      sortOrder: values.sortOrder,
      action: values.action,
      titleRegex: values.titleRegex,
      itemCategories: splitCommaList(values.itemCategories),
      minSizeBytes: parseNumberInput(values.minSizeBytes),
      maxSizeBytes: parseNumberInput(values.maxSizeBytes),
      categoryOverride: values.categoryOverride,
      metadata: serializeMetadata(values.metadata),
    };

    const result = ruleId
      ? await updateRule({ id: ruleId, input })
      : await addRule({ feedId, input });

    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }

    setNotice(ruleId ? t("rss.ruleUpdated") : t("rss.ruleAdded"));
    closeRuleEditor();
    refresh();
  };

  const handleDeleteFeed = async (id: number) => {
    resetFeedback();
    const result = await deleteFeed({ id });
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    setDeleteFeedId(null);
    setNotice(t("rss.feedDeleted"));
    refresh();
  };

  const handleDeleteRule = async (id: number) => {
    resetFeedback();
    const result = await deleteRule({ id });
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    setDeleteRuleTarget(null);
    setNotice(t("rss.ruleDeleted"));
    refresh();
  };

  const handleForgetSeenItem = async (item: RssSeenItem) => {
    resetFeedback();
    const result = await deleteSeenItem({ feedId: item.feedId, itemId: item.itemId });
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    setNotice(t("rss.seenItemForgotten"));
    refresh();
  };

  const handleClearSeenItems = async (feedId?: number) => {
    resetFeedback();
    const result = await clearSeenItems({ feedId: feedId ?? null });
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    setClearSeenTarget(null);
    setNotice(
      t("rss.seenHistoryCleared", {
        count: result.data?.clearRssSeenItems ?? 0,
      }),
    );
    refresh();
  };

  const handleRunSync = async (feedId?: number) => {
    resetFeedback();
    setSyncTarget(feedId ?? "all");
    const result = await runSync({ feedId: feedId ?? null });
    setSyncTarget(null);
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    if (result.data?.runRssSync) {
      setSyncReport(result.data.runRssSync);
      setNotice(t("rss.syncComplete"));
      refresh();
    }
  };

  const handleWatchFolderSave = async (values: WatchFolderFormValues) => {
    resetFeedback();
    const result = await updateSettings({
      input: {
        watchFolder: {
          mode: values.mode,
          path: values.path.trim() || null,
          pollIntervalSecs: Math.max(1, Math.round(values.pollIntervalSecs || 60)),
          stabilitySecs: Math.max(0, Math.round(values.stabilitySecs || 0)),
          categoryFromSubfolders: values.categoryFromSubfolders,
          scanningPaused: values.scanningPaused,
        },
      },
    });
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    setNotice(t("watchFolder.settingsSaved"));
    refresh();
  };

  const handleWatchFolderScan = async () => {
    resetFeedback();
    const result = await scanWatchFolder({});
    if (result.error) {
      setErrorMessage(result.error.message);
      return;
    }
    if (result.data?.scanWatchFolder) {
      setWatchFolderScanReport(result.data.scanWatchFolder);
      setNotice(t("watchFolder.scanComplete"));
      refresh();
    }
  };

  return (
    <div className="max-w-[1180px] space-y-6">
      <PageHeader
        title={t("settings.rss")}
        description={t("settings.rssDesc")}
        actions={
          <>
            <Button
              variant="outline"
              onClick={() => setClearSeenTarget("all")}
              disabled={totalSeenItems === 0}
            >
              {t("rss.clearAllSeen")}
            </Button>
            <Button
              variant="outline"
              onClick={() => void handleRunSync()}
              disabled={runSyncState.fetching}
            >
              {syncTarget === "all" ? t("rss.syncing") : t("rss.runAllFeeds")}
            </Button>
            <Button onClick={openAddFeed}>{t("rss.addFeed")}</Button>
          </>
        }
      />

      {errorMessage ? (
        <StatusBanner variant="destructive">{errorMessage}</StatusBanner>
      ) : null}

      {notice ? <StatusBanner variant="success">{notice}</StatusBanner> : null}

      {syncReport ? <SyncReportCard report={syncReport} /> : null}

      <WatchFolderSettingsCard
        values={watchFolderValues}
        saving={updateSettingsState.fetching}
        scanning={scanWatchFolderState.fetching}
        scanReport={watchFolderScanReport}
        onChange={setWatchFolderValues}
        onSave={handleWatchFolderSave}
        onScan={handleWatchFolderScan}
      />

      {showFeedForm ? (
        <FeedFormCard
          categories={categories}
          editing={!!editingFeed}
          initialValues={
            editingFeed
              ? {
                  name: editingFeed.name,
                  url: editingFeed.url,
                  enabled: editingFeed.enabled,
                  pollIntervalSecs: editingFeed.pollIntervalSecs,
                  username: editingFeed.username ?? "",
                  password: "",
                  clearPassword: false,
                  defaultCategory: editingFeed.defaultCategory ?? "",
                  defaultMetadata: editingFeed.defaultMetadata,
                }
              : defaultFeedForm
          }
          saving={isSavingFeed}
          onCancel={closeFeedForm}
          onSave={handleFeedSave}
        />
      ) : null}

      {fetching && !data ? (
        <div className="rounded-card border border-border bg-card p-6 text-sm text-muted-foreground">
          {t("label.loading")}
        </div>
      ) : null}

      {!fetching && feeds.length === 0 && !showFeedForm ? (
        <EmptyState
          title={t("rss.empty")}
          description={t("rss.emptyHint")}
          actionLabel={t("rss.addFeed")}
          onAction={openAddFeed}
        />
      ) : null}

      {feeds.map((feed) => (
        <SectionCard
          key={feed.id}
          title={
            <span className="flex flex-wrap items-center gap-2">
              <span
                className={cn(
                  "size-2 rounded-pill",
                  feed.enabled ? "bg-status-completed" : "bg-muted-foreground/40",
                )}
              />
              {feed.name}
              <Badge variant={feed.enabled ? "success" : "muted"}>
                {feed.enabled ? t("label.enabled") : t("label.disabled")}
              </Badge>
              {feed.hasPassword ? (
                <Badge variant="info">{t("rss.passwordSaved")}</Badge>
              ) : null}
              {feed.consecutiveFailures > 0 ? (
                <Badge variant="warning">
                  {t("rss.failures")} {feed.consecutiveFailures}
                </Badge>
              ) : null}
            </span>
          }
          description={<span className="break-all font-mono">{feed.url}</span>}
          actions={
            <div className="flex flex-wrap gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => void handleRunSync(feed.id)}
                disabled={runSyncState.fetching}
              >
                {syncTarget === feed.id ? t("rss.syncing") : t("rss.runFeed")}
              </Button>
              <Button variant="outline" size="sm" onClick={() => openAddRule(feed.id)}>
                {t("rss.addRule")}
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setClearSeenTarget(feed.id)}
                disabled={(seenItemsByFeed.get(feed.id)?.length ?? 0) === 0}
              >
                {t("rss.clearSeen")}
              </Button>
              <Button variant="ghost" size="sm" onClick={() => openEditFeed(feed)}>
                {t("action.edit")}
              </Button>
              <Button variant="ghost" size="sm" onClick={() => setDeleteFeedId(feed.id)}>
                {t("action.delete")}
              </Button>
            </div>
          }
        >
          <div className="space-y-5">
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <DetailCard label={t("rss.pollInterval")}>
                {feed.pollIntervalSecs}s
              </DetailCard>
              <DetailCard label={t("rss.defaultCategory")}>
                {feed.defaultCategory || t("rss.none")}
              </DetailCard>
              <DetailCard label={t("rss.username")}>
                {feed.username || t("rss.none")}
              </DetailCard>
              <DetailCard label={t("rss.lastPolled")}>
                {formatTimestamp(feed.lastPolledAt) ?? t("rss.never")}
              </DetailCard>
              <DetailCard label={t("rss.lastSuccess")}>
                {formatTimestamp(feed.lastSuccessAt) ?? t("rss.never")}
              </DetailCard>
              <DetailCard label={t("rss.etag")}>
                {feed.etag || t("rss.none")}
              </DetailCard>
              <DetailCard label={t("rss.lastModified")}>
                {feed.lastModified || t("rss.none")}
              </DetailCard>
              <DetailCard label={t("rss.authConfigured")}>
                {feed.username || feed.hasPassword ? t("label.enabled") : t("label.disabled")}
              </DetailCard>
            </div>

            {feed.defaultMetadata.length > 0 ? (
              <div className="space-y-2">
                <div className="text-sm font-semibold text-foreground">{t("rss.defaultMetadata")}</div>
                <div className="flex flex-wrap gap-2">
                  {feed.defaultMetadata.map((entry) => (
                    <Badge key={`${entry.key}:${entry.value}`} variant="outline">
                      {entry.key}={entry.value}
                    </Badge>
                  ))}
                </div>
              </div>
            ) : null}

            {feed.lastError ? (
              <div className="rounded-inner border border-destructive/30 bg-destructive/5 px-4 py-3">
                <div className="text-sm font-semibold text-destructive">{t("rss.lastError")}</div>
                <div className="mt-1 text-sm text-muted-foreground">{feed.lastError}</div>
              </div>
            ) : null}

            {ruleEditor?.feedId === feed.id ? (
              <RuleFormCard
                categories={categories}
                editing={!!ruleEditor.rule}
                initialValues={
                  ruleEditor.rule
                    ? {
                        enabled: ruleEditor.rule.enabled,
                        sortOrder: ruleEditor.rule.sortOrder,
                        action: ruleEditor.rule.action,
                        titleRegex: ruleEditor.rule.titleRegex ?? "",
                        itemCategories: ruleEditor.rule.itemCategories.join(", "),
                        minSizeBytes: ruleEditor.rule.minSizeBytes?.toString() ?? "",
                        maxSizeBytes: ruleEditor.rule.maxSizeBytes?.toString() ?? "",
                        categoryOverride: ruleEditor.rule.categoryOverride ?? "",
                        metadata: ruleEditor.rule.metadata,
                      }
                    : defaultRuleForm
                }
                saving={isSavingRule}
                onCancel={closeRuleEditor}
                onSave={(values) =>
                  handleRuleSave(feed.id, ruleEditor.rule?.id ?? null, values)
                }
              />
            ) : null}

            {feed.rules.length === 0 ? (
              <div className="rounded-inner border border-dashed border-border px-4 py-6 text-sm text-muted-foreground">
                <div className="font-semibold text-foreground">{t("rss.noRules")}</div>
                <p className="mt-1">{t("rss.noRulesHint")}</p>
              </div>
            ) : (
              <div className="space-y-3">
                {[...feed.rules]
                  .sort(
                    (left, right) =>
                      left.sortOrder - right.sortOrder || left.id - right.id,
                  )
                  .map((rule) => (
                    <RuleCard
                      key={rule.id}
                      rule={rule}
                      onEdit={() => openEditRule(feed.id, rule)}
                      onDelete={() =>
                        setDeleteRuleTarget({ feedId: feed.id, ruleId: rule.id })
                      }
                    />
                  ))}
              </div>
            )}

            <SeenItemsCard
              items={seenItemsByFeed.get(feed.id) ?? []}
              onForget={handleForgetSeenItem}
            />
          </div>
        </SectionCard>
      ))}

      <ConfirmDialog
        open={deleteFeedId != null}
        title={t("confirm.deleteRssFeed")}
        message={t("confirm.deleteRssFeedMessage")}
        confirmLabel={t("confirm.deleteRssFeedConfirm")}
        cancelLabel={t("confirm.deleteRssFeedDismiss")}
        onConfirm={() => deleteFeedId != null && void handleDeleteFeed(deleteFeedId)}
        onCancel={() => setDeleteFeedId(null)}
      />

      <ConfirmDialog
        open={deleteRuleTarget != null}
        title={t("confirm.deleteRssRule")}
        message={t("confirm.deleteRssRuleMessage")}
        confirmLabel={t("confirm.deleteRssRuleConfirm")}
        cancelLabel={t("confirm.deleteRssRuleDismiss")}
        onConfirm={() =>
          deleteRuleTarget != null && void handleDeleteRule(deleteRuleTarget.ruleId)
        }
        onCancel={() => setDeleteRuleTarget(null)}
      />

      <ConfirmDialog
        open={clearSeenTarget != null}
        title={t("confirm.clearRssSeen")}
        message={
          clearSeenTarget === "all"
            ? t("confirm.clearRssSeenMessageAll")
            : t("confirm.clearRssSeenMessageFeed")
        }
        confirmLabel={t("confirm.clearRssSeenConfirm")}
        cancelLabel={t("confirm.clearRssSeenDismiss")}
        onConfirm={() =>
          clearSeenTarget != null &&
          void handleClearSeenItems(clearSeenTarget === "all" ? undefined : clearSeenTarget)
        }
        onCancel={() => setClearSeenTarget(null)}
      />

    </div>
  );
}

function WatchFolderSettingsCard({
  values,
  saving,
  scanning,
  scanReport,
  onChange,
  onSave,
  onScan,
}: {
  values: WatchFolderFormValues;
  saving: boolean;
  scanning: boolean;
  scanReport: WatchFolderScanReport | null;
  onChange: (values: WatchFolderFormValues) => void;
  onSave: (values: WatchFolderFormValues) => Promise<void>;
  onScan: () => Promise<void>;
}) {
  const t = useTranslate();
  const hasPath = values.path.trim().length > 0;
  const automaticEnabled = values.mode !== "off" && !values.scanningPaused;

  return (
    <SectionCard
      title={t("watchFolder.title")}
      description={t("watchFolder.desc")}
      actions={
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" onClick={() => void onScan()} disabled={!hasPath || scanning}>
            {scanning ? t("watchFolder.scanning") : t("watchFolder.scanNow")}
          </Button>
          <Button onClick={() => void onSave(values)} disabled={saving}>
            {saving ? t("watchFolder.saving") : t("action.save")}
          </Button>
        </div>
      }
    >
      <div className="space-y-5">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <Field label={t("watchFolder.mode")}>
            <Select
              value={values.mode}
              onValueChange={(mode) =>
                onChange({ ...values, mode: mode as WatchFolderMode })
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="off">{t("watchFolder.modeOff")}</SelectItem>
                <SelectItem value="polling">{t("watchFolder.modePolling")}</SelectItem>
                <SelectItem value="realtime">{t("watchFolder.modeRealtime")}</SelectItem>
              </SelectContent>
            </Select>
          </Field>

          <Field label={t("watchFolder.folder")}>
            <FolderPathInput
              value={values.path}
              onChange={(path) => onChange({ ...values, path })}
              browseLabel={t("watchFolder.browse")}
            />
          </Field>

          <Field label={t("watchFolder.pollInterval")}>
            <Input
              type="number"
              min={1}
              value={values.pollIntervalSecs}
              onChange={(event) =>
                onChange({
                  ...values,
                  pollIntervalSecs: Number(event.target.value),
                })
              }
            />
          </Field>

          <Field label={t("watchFolder.stabilityWait")}>
            <Input
              type="number"
              min={0}
              value={values.stabilitySecs}
              onChange={(event) =>
                onChange({
                  ...values,
                  stabilitySecs: Number(event.target.value),
                })
              }
            />
          </Field>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
            <div>
              <div className="text-sm font-semibold text-foreground">
                {t("watchFolder.categorySubfolders")}
              </div>
              <div className="mt-1 text-[12.5px] text-muted-foreground">
                {t("watchFolder.categorySubfoldersDesc")}
              </div>
            </div>
            <Switch
              checked={values.categoryFromSubfolders}
              onCheckedChange={(checked) =>
                onChange({ ...values, categoryFromSubfolders: checked })
              }
            />
          </div>

          <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
            <div>
              <div className="text-sm font-semibold text-foreground">
                {t("watchFolder.automaticScanning")}
              </div>
              <div className="mt-1 text-[12.5px] text-muted-foreground">
                {automaticEnabled
                  ? t("watchFolder.automaticEnabled")
                  : t("watchFolder.automaticPausedOrOff")}
              </div>
            </div>
            <Switch
              checked={!values.scanningPaused && values.mode !== "off"}
              disabled={values.mode === "off"}
              onCheckedChange={(checked) =>
                onChange({ ...values, scanningPaused: !checked })
              }
            />
          </div>
        </div>

        {scanReport ? <WatchFolderScanReportCard report={scanReport} /> : null}
      </div>
    </SectionCard>
  );
}

function WatchFolderScanReportCard({ report }: { report: WatchFolderScanReport }) {
  const t = useTranslate();
  const issueCount =
    report.skippedInputs.length + report.permanentErrors.length + report.transientErrors.length;

  return (
    <div className="space-y-4 rounded-inner border border-border p-5">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        <DetailCard label={t("watchFolder.discovered")}>{report.discoveredFiles.length}</DetailCard>
        <DetailCard label={t("watchFolder.queued")}>{report.queuedNzbs.length}</DetailCard>
        <DetailCard label={t("watchFolder.markers")}>{report.markerRenamedSources.length}</DetailCard>
        <DetailCard label={t("watchFolder.skipped")}>{report.skippedInputs.length}</DetailCard>
        <DetailCard label={t("watchFolder.issues")}>{issueCount}</DetailCard>
      </div>

      {report.queuedNzbs.length > 0 ? (
        <ScanList title={t("watchFolder.queuedNzbs")} items={report.queuedNzbs} />
      ) : null}
      {report.markerRenamedSources.length > 0 ? (
        <ScanList
          title={t("watchFolder.markerRenames")}
          items={report.markerRenamedSources.map((rename) => `${rename.from} -> ${rename.to}`)}
        />
      ) : null}
      {report.permanentErrors.length > 0 ? (
        <ScanIssueList title={t("watchFolder.permanentErrors")} items={report.permanentErrors} />
      ) : null}
      {report.transientErrors.length > 0 ? (
        <ScanIssueList title={t("watchFolder.transientErrors")} items={report.transientErrors} />
      ) : null}
      {report.skippedInputs.length > 0 ? (
        <ScanIssueList title={t("watchFolder.skippedInputs")} items={report.skippedInputs} />
      ) : null}
    </div>
  );
}

function ScanList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="space-y-2">
      <div className="text-sm font-semibold text-foreground">{title}</div>
      <div className="space-y-1">
        {items.slice(0, 8).map((item) => (
          <div key={item} className="break-all font-mono text-xs text-muted-foreground">
            {item}
          </div>
        ))}
      </div>
    </div>
  );
}

function ScanIssueList({ title, items }: { title: string; items: WatchFolderScanIssue[] }) {
  return (
    <ScanList
      title={title}
      items={items.map((item) => `${item.path}: ${item.reason}`)}
    />
  );
}

function FeedFormCard({
  categories,
  editing,
  initialValues,
  saving,
  onSave,
  onCancel,
}: {
  categories: Category[];
  editing: boolean;
  initialValues: FeedFormValues;
  saving: boolean;
  onSave: (values: FeedFormValues) => Promise<void>;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  return (
    <SectionCard
      title={editing ? t("rss.editFeed") : t("rss.addFeed")}
      description={t("settings.rssDesc")}
    >
      <div className="space-y-5">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <Field label={t("rss.feedName")}>
            <Input
              value={values.name}
              placeholder="Nyaa"
              onChange={(event) =>
                setValues((current) => ({ ...current, name: event.target.value }))
              }
            />
          </Field>

          <Field label={t("rss.feedUrl")}>
            <Input
              value={values.url}
              placeholder="https://example.com/rss.xml"
              onChange={(event) =>
                setValues((current) => ({ ...current, url: event.target.value }))
              }
              className="font-mono"
            />
          </Field>

          <Field label={t("rss.pollInterval")} description={t("rss.pollIntervalDesc")}>
            <Input
              type="number"
              min={1}
              value={values.pollIntervalSecs}
              onChange={(event) =>
                setValues((current) => ({
                  ...current,
                  pollIntervalSecs: Number(event.target.value),
                }))
              }
            />
          </Field>

          <Field label={t("rss.defaultCategory")}>
            <Select
              value={values.defaultCategory || NONE_VALUE}
              onValueChange={(value) =>
                setValues((current) => ({
                  ...current,
                  defaultCategory: value === NONE_VALUE ? "" : value,
                }))
              }
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NONE_VALUE}>{t("rss.none")}</SelectItem>
                {categories.map((category) => (
                  <SelectItem key={category.id} value={category.name}>
                    {category.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </Field>

          <Field label={t("rss.username")}>
            <Input
              value={values.username}
              onChange={(event) =>
                setValues((current) => ({ ...current, username: event.target.value }))
              }
            />
          </Field>

          <Field label={t("rss.password")}>
            <Input
              type="password"
              value={values.password}
              disabled={values.clearPassword}
              placeholder={
                editing
                  ? t("rss.feedPasswordPlaceholderEdit")
                  : t("rss.feedPasswordPlaceholderAdd")
              }
              onChange={(event) =>
                setValues((current) => ({ ...current, password: event.target.value }))
              }
            />
            {editing ? (
              <label className="mt-2 flex items-center gap-2 text-xs text-muted-foreground">
                <Checkbox
                  checked={values.clearPassword}
                  onCheckedChange={(checked) =>
                    setValues((current) => ({
                      ...current,
                      clearPassword: checked === true,
                    }))
                  }
                />
                <span>{t("rss.clearStoredPassword")}</span>
              </label>
            ) : null}
          </Field>
        </div>

        <div className="flex items-center justify-between gap-4 rounded-inner border border-border p-5">
          <div>
            <div className="text-sm font-semibold text-foreground">{t("rss.feedEnabled")}</div>
            <div className="mt-1 text-[12.5px] text-muted-foreground">{t("rss.feedEnabledDesc")}</div>
          </div>
          <Switch
            checked={values.enabled}
            onCheckedChange={(enabled) =>
              setValues((current) => ({ ...current, enabled }))
            }
          />
        </div>

        <MetadataEditor
          entries={values.defaultMetadata}
          label={t("rss.defaultMetadata")}
          description={t("rss.defaultMetadataDesc")}
          onChange={(entries) =>
            setValues((current) => ({ ...current, defaultMetadata: entries }))
          }
        />

        <div className="flex flex-wrap gap-3">
          <Button
            onClick={() => void onSave(values)}
            disabled={
              !values.name.trim() ||
              !values.url.trim() ||
              values.pollIntervalSecs < 1 ||
              saving
            }
          >
            {editing ? t("settings.save") : t("rss.addFeed")}
          </Button>
          <Button variant="ghost" onClick={onCancel}>
            {t("action.cancel")}
          </Button>
        </div>
      </div>
    </SectionCard>
  );
}

function RuleFormCard({
  categories,
  editing,
  initialValues,
  saving,
  onSave,
  onCancel,
}: {
  categories: Category[];
  editing: boolean;
  initialValues: RuleFormValues;
  saving: boolean;
  onSave: (values: RuleFormValues) => Promise<void>;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  return (
    <div className="rounded-inner border border-border bg-background/40 p-5">
      <div className="mb-4">
        <div className="font-space-grotesk text-base font-bold text-foreground">
          {editing ? t("rss.editRule") : t("rss.addRule")}
        </div>
        <div className="mt-1 text-[12.5px] text-muted-foreground">{t("rss.ruleDesc")}</div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Field label={t("rss.action")}>
          <Select
            value={values.action}
            onValueChange={(value) =>
              setValues((current) => ({
                ...current,
                action: value as RssRuleAction,
              }))
            }
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ACCEPT">{t("rss.ruleAccept")}</SelectItem>
              <SelectItem value="REJECT">{t("rss.ruleReject")}</SelectItem>
            </SelectContent>
          </Select>
        </Field>

        <Field label={t("rss.sortOrder")}>
          <Input
            type="number"
            value={values.sortOrder}
            onChange={(event) =>
              setValues((current) => ({
                ...current,
                sortOrder: Number(event.target.value),
              }))
            }
          />
        </Field>

        <Field label={t("rss.categoryOverride")}>
          <Select
            value={values.categoryOverride || NONE_VALUE}
            onValueChange={(value) =>
              setValues((current) => ({
                ...current,
                categoryOverride: value === NONE_VALUE ? "" : value,
              }))
            }
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={NONE_VALUE}>{t("rss.none")}</SelectItem>
              {categories.map((category) => (
                <SelectItem key={category.id} value={category.name}>
                  {category.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>

        <Field label={t("rss.titleRegex")} description={t("rss.titleRegexDesc")}>
          <Input
            value={values.titleRegex}
            placeholder="Silver Horizon"
            onChange={(event) =>
              setValues((current) => ({ ...current, titleRegex: event.target.value }))
            }
          />
        </Field>

        <Field label={t("rss.itemCategories")} description={t("rss.itemCategoriesDesc")}>
          <Input
            value={values.itemCategories}
            placeholder="anime, tv"
            onChange={(event) =>
              setValues((current) => ({
                ...current,
                itemCategories: event.target.value,
              }))
            }
          />
        </Field>

        <Field label={t("rss.minSizeBytes")}>
          <Input
            type="number"
            min={0}
            value={values.minSizeBytes}
            onChange={(event) =>
              setValues((current) => ({
                ...current,
                minSizeBytes: event.target.value,
              }))
            }
          />
        </Field>

        <Field label={t("rss.maxSizeBytes")}>
          <Input
            type="number"
            min={0}
            value={values.maxSizeBytes}
            onChange={(event) =>
              setValues((current) => ({
                ...current,
                maxSizeBytes: event.target.value,
              }))
            }
          />
        </Field>
      </div>

      <div className="mt-4 flex items-center justify-between gap-4 rounded-inner border border-border bg-card px-4 py-3">
        <div>
          <div className="text-sm font-semibold text-foreground">{t("rss.ruleEnabled")}</div>
          <div className="mt-1 text-[12.5px] text-muted-foreground">{t("rss.ruleEnabledDesc")}</div>
        </div>
        <Switch
          checked={values.enabled}
          onCheckedChange={(enabled) =>
            setValues((current) => ({ ...current, enabled }))
          }
        />
      </div>

      <div className="mt-4">
        <MetadataEditor
          entries={values.metadata}
          label={t("rss.ruleMetadata")}
          description={t("rss.ruleMetadataDesc")}
          onChange={(entries) => setValues((current) => ({ ...current, metadata: entries }))}
        />
      </div>

      <div className="mt-4 flex flex-wrap gap-3">
        <Button onClick={() => void onSave(values)} disabled={saving}>
          {editing ? t("settings.save") : t("rss.addRule")}
        </Button>
        <Button variant="ghost" onClick={onCancel}>
          {t("action.cancel")}
        </Button>
      </div>
    </div>
  );
}

function RuleCard({
  rule,
  onEdit,
  onDelete,
}: {
  rule: RssRule;
  onEdit: () => void;
  onDelete: () => void;
}) {
  const t = useTranslate();

  return (
    <div className="rounded-inner border border-border bg-background/40 p-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={rule.action === "ACCEPT" ? "success" : "destructive"}>
              {rule.action === "ACCEPT" ? t("rss.ruleAccept") : t("rss.ruleReject")}
            </Badge>
            <Badge variant={rule.enabled ? "info" : "muted"}>
              {rule.enabled ? t("label.enabled") : t("label.disabled")}
            </Badge>
            <Badge variant="outline">
              {t("rss.sortOrder")} {rule.sortOrder}
            </Badge>
            {rule.categoryOverride ? (
              <Badge variant="outline">
                {t("rss.categoryOverride")}: {rule.categoryOverride}
              </Badge>
            ) : null}
          </div>

          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <DetailCard label={t("rss.titleRegex")}>
              {rule.titleRegex || t("rss.none")}
            </DetailCard>
            <DetailCard label={t("rss.itemCategories")}>
              {rule.itemCategories.length > 0
                ? rule.itemCategories.join(", ")
                : t("rss.none")}
            </DetailCard>
            <DetailCard label={t("rss.minSizeBytes")}>
              {rule.minSizeBytes?.toLocaleString() ?? t("rss.none")}
            </DetailCard>
            <DetailCard label={t("rss.maxSizeBytes")}>
              {rule.maxSizeBytes?.toLocaleString() ?? t("rss.none")}
            </DetailCard>
          </div>

          {rule.metadata.length > 0 ? (
            <div className="flex flex-wrap gap-2">
              {rule.metadata.map((entry) => (
                <Badge key={`${entry.key}:${entry.value}`} variant="outline">
                  {entry.key}={entry.value}
                </Badge>
              ))}
            </div>
          ) : null}
        </div>

        <div className="flex flex-wrap gap-2">
          <Button variant="ghost" size="sm" onClick={onEdit}>
            {t("action.edit")}
          </Button>
          <Button variant="ghost" size="sm" onClick={onDelete}>
            {t("action.delete")}
          </Button>
        </div>
      </div>
    </div>
  );
}

function SyncReportCard({ report }: { report: RssSyncReport }) {
  const t = useTranslate();

  return (
    <SectionCard title={t("rss.syncReport")} description={t("rss.syncSummary")}>
      <div className="space-y-4">
        <div className="grid gap-3 md:grid-cols-3 xl:grid-cols-6">
          <DetailCard label={t("rss.feedsPolled")}>{report.feedsPolled}</DetailCard>
          <DetailCard label={t("rss.itemsFetched")}>{report.itemsFetched}</DetailCard>
          <DetailCard label={t("rss.itemsNew")}>{report.itemsNew}</DetailCard>
          <DetailCard label={t("rss.itemsAccepted")}>{report.itemsAccepted}</DetailCard>
          <DetailCard label={t("rss.itemsSubmitted")}>{report.itemsSubmitted}</DetailCard>
          <DetailCard label={t("rss.itemsIgnored")}>{report.itemsIgnored}</DetailCard>
        </div>

        {report.feedResults.length > 0 ? (
          <div className="space-y-3">
            {report.feedResults.map((result) => (
              <div
                key={result.feedId}
                className="rounded-inner border border-border bg-background/40 p-5"
              >
                <div className="mb-3 text-sm font-semibold text-foreground">
                  {result.feedName}
                </div>
                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                  <DetailCard label={t("rss.itemsFetched")}>
                    {result.itemsFetched}
                  </DetailCard>
                  <DetailCard label={t("rss.itemsNew")}>{result.itemsNew}</DetailCard>
                  <DetailCard label={t("rss.itemsAccepted")}>
                    {result.itemsAccepted}
                  </DetailCard>
                  <DetailCard label={t("rss.itemsSubmitted")}>
                    {result.itemsSubmitted}
                  </DetailCard>
                  <DetailCard label={t("rss.itemsIgnored")}>
                    {result.itemsIgnored}
                  </DetailCard>
                </div>
                {result.errors.length > 0 ? (
                  <div className="mt-3 space-y-1 text-sm text-destructive">
                    {result.errors.map((message) => (
                      <div key={message}>{message}</div>
                    ))}
                  </div>
                ) : null}
              </div>
            ))}
          </div>
        ) : null}

        {report.errors.length > 0 ? (
          <div className="space-y-1 text-sm text-destructive">
            {report.errors.map((message) => (
              <div key={message}>{message}</div>
            ))}
          </div>
        ) : null}
      </div>
    </SectionCard>
  );
}

function SeenItemsCard({
  items,
  onForget,
}: {
  items: RssSeenItem[];
  onForget: (item: RssSeenItem) => Promise<void> | void;
}) {
  const t = useTranslate();

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-foreground">{t("rss.seenHistory")}</div>
          <div className="mt-1 text-[12.5px] text-muted-foreground">{t("rss.seenHistoryDesc")}</div>
        </div>
        {items.length > 0 ? <Badge variant="outline">{items.length}</Badge> : null}
      </div>

      {items.length === 0 ? (
        <div className="rounded-inner border border-dashed border-border px-4 py-6 text-sm text-muted-foreground">
          <div className="font-semibold text-foreground">{t("rss.noSeenItems")}</div>
          <p className="mt-1">{t("rss.noSeenItemsHint")}</p>
        </div>
      ) : (
        <div className="space-y-3">
          {items.slice(0, 8).map((item) => (
            <div
              key={`${item.feedId}:${item.itemId}`}
              className="rounded-inner border border-border bg-card px-4 py-4"
            >
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div className="min-w-0 space-y-3">
                  <div className="flex flex-wrap items-center gap-2">
                    <Badge variant={decisionVariant(item.decision)}>
                      {formatDecision(item.decision, t)}
                    </Badge>
                    {item.jobId ? (
                      <Link
                        to={`/jobs/${item.jobId}`}
                        className="text-sm font-medium text-primary hover:underline"
                      >
                        {t("rss.jobId")} #{item.jobId}
                      </Link>
                    ) : null}
                  </div>

                  <div>
                    <div className="text-sm font-semibold text-foreground">{item.itemTitle}</div>
                    <div className="mt-1 break-all font-mono text-xs text-muted-foreground">
                      {item.itemId}
                    </div>
                  </div>

                  <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                    <DetailCard label={t("rss.seenAt")}>
                      {formatTimestamp(item.seenAt) ?? t("rss.never")}
                    </DetailCard>
                    <DetailCard label={t("rss.publishedAt")}>
                      {formatTimestamp(item.publishedAt) ?? t("rss.none")}
                    </DetailCard>
                    <DetailCard label={t("rss.itemSize")}>
                      {item.sizeBytes != null ? formatBytes(item.sizeBytes) : t("rss.none")}
                    </DetailCard>
                    <DetailCard label={t("rss.itemUrl")}>
                      {item.itemUrl ? (
                        <a
                          href={item.itemUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="text-primary hover:underline"
                        >
                          {t("rss.openLink")}
                        </a>
                      ) : (
                        t("rss.none")
                      )}
                    </DetailCard>
                  </div>

                  {item.error ? (
                    <div className="rounded-inner border border-destructive/30 bg-destructive/5 px-3 py-3 text-sm text-destructive">
                      {item.error}
                    </div>
                  ) : null}
                </div>

                <Button variant="ghost" size="sm" onClick={() => void onForget(item)}>
                  {t("rss.forgetSeen")}
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function MetadataEditor({
  entries,
  label,
  description,
  onChange,
}: {
  entries: MetadataEntry[];
  label: string;
  description?: string;
  onChange: (entries: MetadataEntry[]) => void;
}) {
  const t = useTranslate();
  const normalized = entries.length > 0 ? entries : [];

  return (
    <div className="space-y-3">
      <div>
        <Label>{label}</Label>
        {description ? (
          <p className="mt-1 text-xs text-muted-foreground">{description}</p>
        ) : null}
      </div>

      {normalized.length > 0 ? (
        <div className="space-y-3">
          {normalized.map((entry, index) => (
            <div key={`${index}:${entry.key}:${entry.value}`} className="grid gap-3 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto]">
              <Input
                value={entry.key}
                placeholder={t("rss.metadataKey")}
                onChange={(event) =>
                  onChange(
                    normalized.map((current, currentIndex) =>
                      currentIndex === index
                        ? { ...current, key: event.target.value }
                        : current,
                    ),
                  )
                }
              />
              <Input
                value={entry.value}
                placeholder={t("rss.metadataValue")}
                onChange={(event) =>
                  onChange(
                    normalized.map((current, currentIndex) =>
                      currentIndex === index
                        ? { ...current, value: event.target.value }
                        : current,
                    ),
                  )
                }
              />
              <Button
                variant="ghost"
                type="button"
                onClick={() => onChange(normalized.filter((_, currentIndex) => currentIndex !== index))}
              >
                {t("action.delete")}
              </Button>
            </div>
          ))}
        </div>
      ) : null}

      <Button
        variant="outline"
        type="button"
        onClick={() => onChange([...normalized, { key: "", value: "" }])}
      >
        {t("rss.addMetadata")}
      </Button>
    </div>
  );
}

function Field({
  label,
  description,
  children,
}: {
  label: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <div className="space-y-2">
      <Label className="text-sm font-semibold">{label}</Label>
      {children}
      {description ? <p className="text-[12.5px] text-muted-foreground">{description}</p> : null}
    </div>
  );
}

function DetailCard({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="rounded-inner border border-border bg-card px-3 py-3">
      <div className="text-[10.5px] font-semibold uppercase tracking-[0.13em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 text-sm font-semibold text-foreground">{children}</div>
    </div>
  );
}

function StatusBanner({
  children,
  variant,
}: {
  children: ReactNode;
  variant: "success" | "destructive";
}) {
  const className =
    variant === "success"
      ? "border-status-completed/25 bg-status-completed/10 text-status-completed"
      : "border-destructive/25 bg-destructive/10 text-destructive";

  return <div className={cn("rounded-card border px-4 py-3 text-sm", className)}>{children}</div>;
}

function splitCommaList(value: string) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function serializeMetadata(entries: MetadataEntry[]) {
  return entries
    .map((entry) => ({
      key: entry.key.trim(),
      value: entry.value.trim(),
    }))
    .filter((entry) => entry.key.length > 0);
}

function parseNumberInput(value: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatTimestamp(value: number | null) {
  if (!value) {
    return null;
  }
  return new Date(value).toLocaleString();
}

function decisionVariant(decision: string) {
  switch (decision) {
    case "submitted":
      return "success" as const;
    case "accepted":
      return "info" as const;
    case "rejected":
      return "destructive" as const;
    case "error":
      return "warning" as const;
    default:
      return "muted" as const;
  }
}

function formatDecision(decision: string, t: ReturnType<typeof useTranslate>) {
  switch (decision) {
    case "submitted":
      return t("rss.decisionSubmitted");
    case "accepted":
      return t("rss.decisionAccepted");
    case "rejected":
      return t("rss.decisionRejected");
    case "error":
      return t("rss.decisionError");
    default:
      return t("rss.decisionIgnored");
  }
}
