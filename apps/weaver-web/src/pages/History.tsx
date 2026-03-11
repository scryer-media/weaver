import { useEffect, useState } from "react";
import { Link } from "react-router";
import { useMutation, useQuery } from "urql";
import { ConfirmDialog } from "@/components/ConfirmDialog";
import { EmptyState } from "@/components/EmptyState";
import { PageHeader } from "@/components/PageHeader";
import { JobStatusBadge } from "@/components/JobStatusBadge";
import { formatBytes } from "@/components/SpeedDisplay";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
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

export function History() {
  const t = useTranslate();
  const [{ data, fetching }] = useQuery({ query: HISTORY_JOBS_QUERY });
  const [deleteState, deleteHistory] = useMutation(DELETE_HISTORY_MUTATION);
  const [deleteAllState, deleteAllHistory] = useMutation(DELETE_ALL_HISTORY_MUTATION);

  const [jobs, setJobs] = useState<HistoryJob[]>([]);
  const [deleteConfirmId, setDeleteConfirmId] = useState<number | null>(null);
  const [deleteAllConfirm, setDeleteAllConfirm] = useState(false);

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

      {fetching && jobs.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center text-muted-foreground">
            {t("label.loading")}
          </CardContent>
        </Card>
      ) : jobs.length === 0 ? (
        <EmptyState title={t("history.title")} description={t("history.empty")} />
      ) : (
        <>
          <Card className="hidden md:block">
            <CardContent className="px-0 pb-0">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>{t("table.name")}</TableHead>
                    <TableHead>{t("table.status")}</TableHead>
                    <TableHead className="text-right">{t("table.health")}</TableHead>
                    <TableHead className="text-right">{t("table.size")}</TableHead>
                    <TableHead>{t("table.category")}</TableHead>
                    <TableHead className="text-right">{t("table.actions")}</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {jobs.map((job) => (
                    <TableRow key={job.id}>
                      <TableCell>
                        <Link
                          to={`/jobs/${job.id}`}
                          className="font-medium text-foreground transition hover:text-primary"
                        >
                          {job.name}
                        </Link>
                        {job.hasPassword ? (
                          <span className="ml-2 text-xs text-amber-500">[PW]</span>
                        ) : null}
                      </TableCell>
                      <TableCell>
                        <JobStatusBadge status={job.status} />
                      </TableCell>
                      <TableCell className="text-right text-muted-foreground">
                        {(job.health / 10).toFixed(1)}%
                      </TableCell>
                      <TableCell className="text-right text-muted-foreground">
                        {formatBytes(job.totalBytes)}
                      </TableCell>
                      <TableCell className="text-muted-foreground">
                        {job.category ?? "\u2014"}
                      </TableCell>
                      <TableCell className="text-right">
                        <Button variant="ghost" size="sm" onClick={() => setDeleteConfirmId(job.id)}>
                          {t("action.delete")}
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <div className="space-y-3 md:hidden">
            {jobs.map((job) => (
              <Card key={job.id}>
                <CardContent className="space-y-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <Link
                        to={`/jobs/${job.id}`}
                        className="block truncate font-medium text-foreground transition hover:text-primary"
                      >
                        {job.name}
                      </Link>
                      <div className="mt-2 flex flex-wrap gap-2 text-xs text-muted-foreground">
                        <span>{formatBytes(job.totalBytes)}</span>
                        <span>Health {(job.health / 10).toFixed(1)}%</span>
                        {job.category ? <span>{job.category}</span> : null}
                      </div>
                    </div>
                    <JobStatusBadge status={job.status} />
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
            void deleteHistory({ id: deleteConfirmId });
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
          void deleteAllHistory({});
          setDeleteAllConfirm(false);
        }}
        onCancel={() => setDeleteAllConfirm(false)}
      />
    </div>
  );
}
