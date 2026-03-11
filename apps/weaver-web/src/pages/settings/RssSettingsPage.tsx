import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
import {
  ADD_RSS_FEED_MUTATION,
  ADD_RSS_RULE_MUTATION,
  DELETE_RSS_FEED_MUTATION,
  DELETE_RSS_RULE_MUTATION,
  RSS_SETTINGS_QUERY,
  RUN_RSS_SYNC_MUTATION,
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

type RssSettingsData = {
  rssFeeds: RssFeed[];
  categories: Category[];
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

export function RssSettingsPage() {
  const t = useTranslate();
  const [{ data, fetching, error }, reexecuteQuery] = useQuery<RssSettingsData>({
    query: RSS_SETTINGS_QUERY,
  });
  const [, addFeed] = useMutation(ADD_RSS_FEED_MUTATION);
  const [, updateFeed] = useMutation(UPDATE_RSS_FEED_MUTATION);
  const [, deleteFeed] = useMutation(DELETE_RSS_FEED_MUTATION);
  const [, addRule] = useMutation(ADD_RSS_RULE_MUTATION);
  const [, updateRule] = useMutation(UPDATE_RSS_RULE_MUTATION);
  const [, deleteRule] = useMutation(DELETE_RSS_RULE_MUTATION);
  const [runSyncState, runSync] = useMutation(RUN_RSS_SYNC_MUTATION);

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
  const [syncTarget, setSyncTarget] = useState<number | "all" | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [syncReport, setSyncReport] = useState<RssSyncReport | null>(null);

  useEffect(() => {
    if (error) {
      setErrorMessage(error.message);
    }
  }, [error]);

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

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("settings.rss")}
        description={t("settings.rssDesc")}
        actions={
          <>
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
          onCancel={closeFeedForm}
          onSave={handleFeedSave}
        />
      ) : null}

      {fetching && !data ? (
        <Card>
          <CardContent className="py-6 text-sm text-muted-foreground">
            {t("label.loading")}
          </CardContent>
        </Card>
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
        <Card key={feed.id}>
          <CardHeader className="gap-4">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
              <div className="space-y-3">
                <div className="flex flex-wrap items-center gap-2">
                  <CardTitle>{feed.name}</CardTitle>
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
                </div>
                <CardDescription className="break-all">{feed.url}</CardDescription>
              </div>

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
                <Button variant="ghost" size="sm" onClick={() => openEditFeed(feed)}>
                  {t("action.edit")}
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setDeleteFeedId(feed.id)}>
                  {t("action.delete")}
                </Button>
              </div>
            </div>
          </CardHeader>

          <CardContent className="space-y-5">
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
                <div className="text-sm font-medium text-foreground">{t("rss.defaultMetadata")}</div>
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
              <div className="rounded-xl border border-destructive/30 bg-destructive/5 px-4 py-3">
                <div className="text-sm font-medium text-destructive">{t("rss.lastError")}</div>
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
                onCancel={closeRuleEditor}
                onSave={(values) =>
                  handleRuleSave(feed.id, ruleEditor.rule?.id ?? null, values)
                }
              />
            ) : null}

            {feed.rules.length === 0 ? (
              <div className="rounded-2xl border border-dashed border-border/80 px-4 py-6 text-sm text-muted-foreground">
                <div className="font-medium text-foreground">{t("rss.noRules")}</div>
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
          </CardContent>
        </Card>
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
    </div>
  );
}

function FeedFormCard({
  categories,
  editing,
  initialValues,
  onSave,
  onCancel,
}: {
  categories: Category[];
  editing: boolean;
  initialValues: FeedFormValues;
  onSave: (values: FeedFormValues) => Promise<void>;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>{editing ? t("rss.editFeed") : t("rss.addFeed")}</CardTitle>
        <CardDescription>{t("settings.rssDesc")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
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

        <div className="flex flex-col gap-4 rounded-2xl border border-border/70 bg-background/70 p-4 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <div className="text-sm font-medium text-foreground">{t("rss.feedEnabled")}</div>
            <div className="text-xs text-muted-foreground">{t("rss.feedEnabledDesc")}</div>
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
            disabled={!values.name.trim() || !values.url.trim() || values.pollIntervalSecs < 1}
          >
            {editing ? t("settings.save") : t("rss.addFeed")}
          </Button>
          <Button variant="ghost" onClick={onCancel}>
            {t("action.cancel")}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function RuleFormCard({
  categories,
  editing,
  initialValues,
  onSave,
  onCancel,
}: {
  categories: Category[];
  editing: boolean;
  initialValues: RuleFormValues;
  onSave: (values: RuleFormValues) => Promise<void>;
  onCancel: () => void;
}) {
  const t = useTranslate();
  const [values, setValues] = useState(initialValues);

  useEffect(() => {
    setValues(initialValues);
  }, [initialValues]);

  return (
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
      <div className="mb-4">
        <div className="text-base font-semibold text-foreground">
          {editing ? t("rss.editRule") : t("rss.addRule")}
        </div>
        <div className="text-sm text-muted-foreground">{t("rss.ruleDesc")}</div>
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
            placeholder="Frieren"
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

      <div className="mt-4 flex flex-col gap-4 rounded-2xl border border-border/70 bg-card px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <div className="text-sm font-medium text-foreground">{t("rss.ruleEnabled")}</div>
          <div className="text-xs text-muted-foreground">{t("rss.ruleEnabledDesc")}</div>
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
        <Button onClick={() => void onSave(values)}>
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
    <div className="rounded-2xl border border-border/70 bg-background/70 p-4">
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
    <Card>
      <CardHeader>
        <CardTitle>{t("rss.syncReport")}</CardTitle>
        <CardDescription>{t("rss.syncSummary")}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
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
                className="rounded-2xl border border-border/70 bg-background/70 p-4"
              >
                <div className="mb-3 text-sm font-medium text-foreground">
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
      </CardContent>
    </Card>
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
      <Label>{label}</Label>
      {children}
      {description ? <p className="text-xs text-muted-foreground">{description}</p> : null}
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
    <div className="rounded-xl border border-border/70 bg-card px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.16em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 text-sm font-medium text-foreground">{children}</div>
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
      ? "border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
      : "border-destructive/25 bg-destructive/10 text-destructive";

  return <div className={`rounded-2xl border px-4 py-3 text-sm ${className}`}>{children}</div>;
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
