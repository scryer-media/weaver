import { useState } from "react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import { DELETE_HISTORY_MUTATION, DELETE_ALL_HISTORY_MUTATION, HISTORY_JOBS_QUERY } from "@/graphql/queries";
import { StatusBadge } from "@/components/StatusBadge";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { formatBytes } from "@/components/SpeedDisplay";
import { useTranslate } from "@/lib/context/translate-context";

export function History() {
  const t = useTranslate();
  const [{ data, fetching }] = useQuery({ query: HISTORY_JOBS_QUERY });
  const [deleteResult, deleteHistory] = useMutation(DELETE_HISTORY_MUTATION);
  const [, deleteAllHistory] = useMutation(DELETE_ALL_HISTORY_MUTATION);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [deleteAllConfirm, setDeleteAllConfirm] = useState(false);

  type HistoryJob = {
    id: number;
    name: string;
    status: string;
    totalBytes: number;
    downloadedBytes: number;
    failedBytes: number;
    health: number;
    hasPassword: boolean;
    category: string | null;
    outputDir: string | null;
  };

  // After a delete, use the mutation response; otherwise use the query data.
  const jobs: HistoryJob[] = deleteResult.data?.deleteHistory ?? data?.jobs ?? [];

  return (
    <div className="p-4 sm:p-6">
      <div className="mb-4 flex items-center justify-between sm:mb-6">
        <h1 className="text-xl font-bold text-foreground sm:text-2xl">
          {t("history.title")}
        </h1>
        {jobs.length > 0 && (
          <button
            onClick={() => setDeleteAllConfirm(true)}
            className="rounded-md px-3 py-1.5 text-sm font-medium text-destructive hover:bg-accent"
          >
            {t("action.deleteAll")}
          </button>
        )}
      </div>

      {fetching ? (
        <div className="py-12 text-center text-muted-foreground">
          {t("label.loading")}
        </div>
      ) : jobs.length === 0 ? (
        <div className="py-12 text-center text-muted-foreground">
          {t("history.empty")}
        </div>
      ) : (
        <>
          {/* Desktop table */}
          <div className="hidden overflow-hidden rounded-lg border border-border md:block">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border bg-card text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  <th className="px-4 py-3">{t("table.name")}</th>
                  <th className="w-28 px-4 py-3">{t("table.status")}</th>
                  <th className="w-24 px-4 py-3 text-right">
                    {t("table.health")}
                  </th>
                  <th className="w-36 px-4 py-3 text-right">
                    {t("table.size")}
                  </th>
                  <th className="w-32 px-4 py-3">{t("table.category")}</th>
                  <th className="w-20 px-4 py-3 text-right">{t("table.actions")}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {jobs.map((job) => (
                  <tr key={job.id} className="hover:bg-accent/30">
                    <td className="px-4 py-3">
                      <Link
                        to={`/jobs/${job.id}`}
                        className="text-sm font-medium text-foreground hover:text-primary hover:underline"
                      >
                        {job.name}
                      </Link>
                      {job.hasPassword && (
                        <span className="ml-2 text-xs text-yellow-500">
                          [PW]
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={job.status} />
                    </td>
                    <td className="px-4 py-3 text-right text-sm text-muted-foreground">
                      {(job.health / 10).toFixed(1)}%
                    </td>
                    <td className="px-4 py-3 text-right text-sm text-muted-foreground">
                      {formatBytes(job.totalBytes)}
                    </td>
                    <td className="px-4 py-3 text-sm text-muted-foreground">
                      {job.category ?? "—"}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <button
                        onClick={() => setDeleteConfirmId(job.id)}
                        className="rounded px-2 py-1 text-xs text-destructive hover:bg-accent"
                      >
                        {t("action.delete")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Mobile card layout */}
          <div className="space-y-3 md:hidden">
            {jobs.map((job) => (
              <div key={job.id} className="rounded-lg border border-border bg-card p-4">
                <div className="mb-2 flex items-start justify-between gap-2">
                  <Link
                    to={`/jobs/${job.id}`}
                    className="min-w-0 flex-1 truncate text-sm font-medium text-foreground hover:text-primary hover:underline"
                  >
                    {job.name}
                  </Link>
                  <StatusBadge status={job.status} />
                </div>
                <div className="mb-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                  <span>{formatBytes(job.totalBytes)}</span>
                  <span>Health: {(job.health / 10).toFixed(1)}%</span>
                  {job.category && <span>{job.category}</span>}
                  {job.hasPassword && (
                    <span className="text-yellow-500">[PW]</span>
                  )}
                </div>
                <div className="flex items-center">
                  <button
                    onClick={() => setDeleteConfirmId(job.id)}
                    className="rounded-md px-3 py-1.5 text-xs font-medium text-destructive hover:bg-accent"
                  >
                    {t("action.delete")}
                  </button>
                </div>
              </div>
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
            deleteHistory({ id: deleteConfirmId });
          }
          setDeleteConfirmId(null);
        }}
        onCancel={() => setDeleteConfirmId(null)}
      />

      <ConfirmDialog
        open={deleteAllConfirm}
        title={t("confirm.deleteAllHistory")}
        message={t("confirm.deleteAllHistoryMessage")}
        confirmLabel={t("confirm.deleteAllHistoryConfirm")}
        cancelLabel={t("confirm.deleteAllHistoryDismiss")}
        onConfirm={() => {
          deleteAllHistory({});
          setDeleteAllConfirm(false);
        }}
        onCancel={() => setDeleteAllConfirm(false)}
      />
    </div>
  );
}
