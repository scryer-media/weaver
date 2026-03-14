import { useEffect, useMemo, useState } from "react";
import { Trash2 } from "lucide-react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { formatBytes } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  DELETE_ALL_HISTORY_MUTATION,
  DELETE_HISTORY_MUTATION,
  HISTORY_JOBS_QUERY,
} from "@/graphql/queries";
import { useTranslate } from "@/lib/context/translate-context";
import { cn } from "@/lib/utils";

type HistoryJob = {
  id: number;
  name: string;
  displayTitle: string;
  originalTitle: string;
  status: string;
  totalBytes: number;
  downloadedBytes: number;
  failedBytes: number;
  health: number;
  hasPassword: boolean;
  category: string | null;
  outputDir: string | null;
  createdAt: number | null;
};

type HistoryFilter = "all" | "success" | "failure";

export function History() {
  const t = useTranslate();
  const [{ data, fetching }] = useQuery({ query: HISTORY_JOBS_QUERY });
  const [deleteState, deleteHistory] = useMutation(DELETE_HISTORY_MUTATION);
  const [deleteAllState, deleteAllHistory] = useMutation(DELETE_ALL_HISTORY_MUTATION);

  const [jobs, setJobs] = useState<HistoryJob[]>([]);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [deleteAllConfirm, setDeleteAllConfirm] = useState(false);
  const [deleteFiles, setDeleteFiles] = useState(false);
  const [filter, setFilter] = useState<HistoryFilter>("all");
  const [search, setSearch] = useState("");

  useEffect(() => {
    if (data?.jobs) {
      setJobs(data.jobs);
    }
  }, [data?.jobs]);

  useEffect(() => {
    if (deleteState.data?.deleteHistory) {
      setJobs(deleteState.data.deleteHistory);
    }
  }, [deleteState.data]);

  useEffect(() => {
    if (deleteAllState.data?.deleteAllHistory) {
      setJobs(deleteAllState.data.deleteAllHistory);
    }
  }, [deleteAllState.data]);

  const counts = useMemo(
    () => ({
      all: jobs.length,
      success: jobs.filter((job) => job.status === "COMPLETE").length,
      failure: jobs.filter((job) => job.status === "FAILED").length,
    }),
    [jobs],
  );
  const sortedJobs = useMemo(
    () =>
      [...jobs].sort(
        (left, right) =>
          (right.createdAt ?? 0) - (left.createdAt ?? 0) || right.id - left.id,
      ),
    [jobs],
  );
  const timestampFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      }),
    [],
  );

  const normalizedSearch = search.trim().toLowerCase();
  const filteredJobs = useMemo(
    () =>
      sortedJobs.filter((job) => {
        if (filter === "success" && job.status !== "COMPLETE") {
          return false;
        }
        if (filter === "failure" && job.status !== "FAILED") {
          return false;
        }
        if (!normalizedSearch) {
          return true;
        }
        return [job.name, job.category ?? "", job.outputDir ?? ""]
          .join(" ")
          .toLowerCase()
          .includes(normalizedSearch);
      }),
    [filter, normalizedSearch, sortedJobs],
  );

  return (
    <div className="space-y-6">
      <PageHeader
        title={t("history.title")}
        description={t("history.empty")}
        actions={
          jobs.length > 0 ? (
            <Button variant="outline" onClick={() => setDeleteAllConfirm(true)}>
              {t("action.deleteAll")}
            </Button>
          ) : undefined
        }
      />

      {jobs.length > 0 ? (
        <Card>
          <CardContent className="space-y-4 pt-6">
            <div className="flex flex-wrap gap-2">
              <FilterButton
                active={filter === "all"}
                label={t("history.filterAll")}
                count={counts.all}
                onClick={() => setFilter("all")}
              />
              <FilterButton
                active={filter === "success"}
                label={t("history.filterSuccess")}
                count={counts.success}
                onClick={() => setFilter("success")}
              />
              <FilterButton
                active={filter === "failure"}
                label={t("history.filterFailure")}
                count={counts.failure}
                onClick={() => setFilter("failure")}
              />
            </div>

            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="max-w-md flex-1">
                <Input
                  value={search}
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder={t("history.searchPlaceholder")}
                />
              </div>
              {search || filter !== "all" ? (
                <Button
                  variant="ghost"
                  onClick={() => {
                    setFilter("all");
                    setSearch("");
                  }}
                >
                  {t("action.clearFilters")}
                </Button>
              ) : null}
            </div>
          </CardContent>
        </Card>
      ) : null}

      {fetching && jobs.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            {t("label.loading")}
          </CardContent>
        </Card>
      ) : jobs.length === 0 ? (
        <EmptyState title={t("history.title")} description={t("history.empty")} />
      ) : filteredJobs.length === 0 ? (
        <Card>
          <CardContent className="space-y-3 py-12 text-center">
            <div className="text-sm text-muted-foreground">{t("history.noMatches")}</div>
            <div>
              <Button
                variant="outline"
                onClick={() => {
                  setFilter("all");
                  setSearch("");
                }}
              >
                {t("action.clearFilters")}
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : (
        <>
          <Card className="hidden lg:block">
            <CardContent className="px-0 pb-0">
              <Table className="table-fixed">
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="h-7 w-[34%] px-2 text-[9px]">{t("table.name")}</TableHead>
                    <TableHead className="h-7 w-[12%] px-2 text-[9px]">{t("table.status")}</TableHead>
                    <TableHead className="h-7 w-[18%] px-2 text-[9px]">{t("table.time")}</TableHead>
                    <TableHead className="h-7 w-[8%] px-2 text-right text-[9px]">{t("table.health")}</TableHead>
                    <TableHead className="h-7 w-[12%] px-2 text-right text-[9px]">{t("table.size")}</TableHead>
                    <TableHead className="h-7 w-[10%] px-2 text-[9px]">{t("table.category")}</TableHead>
                    <TableHead className="h-7 w-[6%] px-2 text-right text-[9px]">{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredJobs.map((job) => (
                    <TableRow key={job.id} className="text-[11px]">
                      <TableCell className="min-w-0 px-2 py-1.5">
                        <Link
                          to={`/jobs/${job.id}`}
                          title={job.originalTitle}
                          className="block min-w-0 truncate text-[11px] font-medium leading-tight text-foreground transition hover:text-primary"
                        >
                          {job.displayTitle}
                        </Link>
                        {job.hasPassword ? (
                          <span className="ml-2 shrink-0 text-[9px] text-amber-500">
                            {t("jobs.passwordProtected")}
                          </span>
                        ) : null}
                      </TableCell>
                      <TableCell className="overflow-hidden px-2 py-1.5">
                        <JobStatusBadge status={job.status} compact className="px-1.5" />
                      </TableCell>
                      <TableCell
                        className="px-2 py-1.5 text-[10px] text-muted-foreground"
                        title={formatHistoryTimestamp(job.createdAt)}
                      >
                        {formatHistoryTimestamp(job.createdAt, timestampFormatter)}
                      </TableCell>
                      <TableCell className="px-2 py-1.5 text-right text-[10px] text-muted-foreground">
                        {(job.health / 10).toFixed(1)}%
                      </TableCell>
                      <TableCell className="px-2 py-1.5 text-right text-[10px] text-muted-foreground">
                        {formatBytes(job.totalBytes)}
                      </TableCell>
                      <TableCell className="truncate px-2 py-1.5 text-[10px] text-muted-foreground" title={job.category ?? "\u2014"}>
                        {job.category ?? "\u2014"}
                      </TableCell>
                      <TableCell className="px-2 py-1.5">
                        <div className="flex justify-end gap-0.5">
                          <Button
                            variant="ghost"
                            size="icon"
                            title={t("action.delete")}
                            aria-label={t("action.delete")}
                            className="size-6"
                            onClick={() => setDeleteConfirmId(job.id)}
                          >
                            <Trash2 className="size-3.5" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <div className="space-y-3 lg:hidden">
            {filteredJobs.map((job) => (
              <Card key={job.id}>
                <CardContent className="space-y-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <Link
                        to={`/jobs/${job.id}`}
                        className="block truncate font-medium text-foreground transition hover:text-primary"
                      >
                        {job.displayTitle}
                      </Link>
                      <div className="mt-2 flex flex-wrap gap-2 text-xs text-muted-foreground">
                        <span>{formatHistoryTimestamp(job.createdAt, timestampFormatter)}</span>
                        <span>{formatBytes(job.totalBytes)}</span>
                        <span>Health {(job.health / 10).toFixed(1)}%</span>
                        {job.category ? <span>{job.category}</span> : null}
                      </div>
                    </div>
                    <JobStatusBadge status={job.status} compact />
                  </div>
                  <div className="flex justify-end">
                    <Button variant="ghost" size="sm" onClick={() => setDeleteConfirmId(job.id)}>
                      {t("action.delete")}
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </>
      )}

      <ConfirmDialog
        open={deleteConfirmId != null}
        title={t("confirm.deleteHistory")}
        message={t("confirm.deleteHistoryMessage")}
        confirmLabel={t("confirm.deleteHistoryConfirm")}
        cancelLabel={t("confirm.deleteHistoryDismiss")}
        onConfirm={() => {
          if (deleteConfirmId != null) {
            void deleteHistory({ id: deleteConfirmId, deleteFiles });
          }
          setDeleteConfirmId(null);
          setDeleteFiles(false);
        }}
        onCancel={() => { setDeleteConfirmId(null); setDeleteFiles(false); }}
      >
        <label className="flex items-center gap-2">
          <Checkbox checked={deleteFiles} onCheckedChange={(v) => setDeleteFiles(v === true)} />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
      </ConfirmDialog>

      <ConfirmDialog
        open={deleteAllConfirm}
        title={t("confirm.deleteAllHistory")}
        message={t("confirm.deleteAllHistoryMessage")}
        confirmLabel={t("confirm.deleteAllHistoryConfirm")}
        cancelLabel={t("confirm.deleteAllHistoryDismiss")}
        onConfirm={() => {
          void deleteAllHistory({ deleteFiles });
          setDeleteAllConfirm(false);
          setDeleteFiles(false);
        }}
        onCancel={() => { setDeleteAllConfirm(false); setDeleteFiles(false); }}
      >
        <label className="flex items-center gap-2">
          <Checkbox checked={deleteFiles} onCheckedChange={(v) => setDeleteFiles(v === true)} />
          <span className="text-sm">{t("confirm.deleteFiles")}</span>
        </label>
      </ConfirmDialog>
    </div>
  );
}

function formatHistoryTimestamp(
  timestamp: number | null,
  formatter = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }),
) {
  if (!timestamp) {
    return "-";
  }

  return formatter.format(new Date(timestamp));
}

function FilterButton({
  active,
  label,
  count,
  onClick,
}: {
  active: boolean;
  label: string;
  count: number;
  onClick: () => void;
}) {
  return (
    <Button variant={active ? "default" : "outline"} size="sm" onClick={onClick}>
      <span>{label}</span>
      <span
        className={cn(
          "ml-1.5 rounded-full px-1.5 py-0.5 text-[11px] leading-none",
          active
            ? "bg-primary-foreground/18 text-primary-foreground"
            : "bg-muted text-muted-foreground",
        )}
      >
        {count}
      </span>
    </Button>
  );
}
