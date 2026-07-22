import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "urql";
import { PageHeader } from "@/components/PageHeader";
import { SectionCard } from "@/components/SectionCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
  APPROVE_POST_PROCESSING_REVISION_MUTATION,
  ASSIGN_CATEGORY_POST_PROCESSING_PROFILE_MUTATION,
  ASSIGN_GLOBAL_POST_PROCESSING_PROFILE_MUTATION,
  CANCEL_JOB_POST_PROCESSING_MUTATION,
  DELETE_POST_PROCESSING_PROFILE_MUTATION,
  DISABLE_POST_PROCESSING_REVISION_MUTATION,
  DISCOVER_POST_PROCESSING_EXTENSIONS_MUTATION,
  PAUSE_POST_PROCESSING_QUEUE_MUTATION,
  POST_PROCESSING_ATTEMPTS_QUERY,
  POST_PROCESSING_JOB_QUERY,
  POST_PROCESSING_LOGS_QUERY,
  POST_PROCESSING_QUEUE_QUERY,
  POST_PROCESSING_SETTINGS_QUERY,
  REORDER_POST_PROCESSING_QUEUE_MUTATION,
  RERUN_POST_PROCESSING_MUTATION,
  RESUME_POST_PROCESSING_QUEUE_MUTATION,
  REVOKE_POST_PROCESSING_REVISION_MUTATION,
  SAVE_POST_PROCESSING_PROFILE_MUTATION,
  SET_JOB_POST_PROCESSING_SELECTION_MUTATION,
  UPDATE_POST_PROCESSING_SETTINGS_MUTATION,
} from "@/graphql/queries";

type PostProcessingSettings = {
  discoveryEnabled: boolean;
  executionEnabled: boolean;
  concurrency: number;
  terminationGraceSeconds: number;
  pythonInterpreter?: string | null;
  powershellInterpreter?: string | null;
  batchInterpreter?: string | null;
  webhooksEnabled: boolean;
  allowedRoots: string[];
};

type ExtensionRevision = {
  extensionId: string;
  revisionId: string;
  declaredVersion: string;
  digest: string;
  adapter: string;
  displayName: string;
  trustState: string;
  managed: boolean;
  sourcePath?: string | null;
  discoveredAtEpochMs: number;
  approvedAtEpochMs?: number | null;
  manifest: unknown;
};

type ProfileRecord = {
  profileId: string;
  name: string;
  enabled: boolean;
  createdAtEpochMs: number;
  updatedAtEpochMs: number;
  definition: unknown;
};

type QueryData = {
  postProcessingSettings: PostProcessingSettings;
  postProcessingRevisions: ExtensionRevision[];
  postProcessingProfiles: ProfileRecord[];
};

type ProfileStep = {
  extensionId: string;
  revisionId: string;
  runWhen: "ALWAYS" | "SUCCESS" | "FAILURE";
  onFailure: "STOP" | "CONTINUE";
  outcomeImpact: "WARN" | "FAIL_JOB";
  timeoutSeconds: string;
  unlimitedTimeout: boolean;
  approvedRoots: string;
  requiredArtifactSuffixes: string;
  minimumArtifactCount: string;
  options: string;
};

type PostProcessingRun = {
  runId: string;
  jobId: number;
  status: string;
  pipelineOutcome: unknown;
  summary: string;
  terminalIntent: string;
  plan: unknown;
  rerunOfRunId?: string | null;
  queuedAtEpochMs: number;
  queuePosition: number;
  startedAtEpochMs?: number | null;
  finishedAtEpochMs?: number | null;
};

type PostProcessingAttempt = {
  attemptId: string;
  runId: string;
  stepIndex: number;
  status: string;
  extensionId: string;
  revisionId: string;
  adapter: string;
  workingDirectory?: string | null;
  exitCode?: number | null;
  errorMessage?: string | null;
  progress?: unknown;
  outputTruncated: boolean;
  queuedAtEpochMs: number;
  startedAtEpochMs?: number | null;
  finishedAtEpochMs?: number | null;
};

type QueueData = { postProcessingQueue: PostProcessingRun[] };
type JobData = {
  postProcessingJobPlan?: { jobId: number; definition: unknown } | null;
  postProcessingRuns: PostProcessingRun[];
};
type AttemptsData = { postProcessingAttempts: PostProcessingAttempt[] };
type LogsData = {
  postProcessingLogs: {
    chunks: Array<{ sequence: number; stream: string; text: string; createdAtEpochMs: number }>;
    nextCursor?: number | null;
    truncated: boolean;
  };
};

const EMPTY_SETTINGS: PostProcessingSettings = {
  discoveryEnabled: false,
  executionEnabled: false,
  concurrency: 1,
  terminationGraceSeconds: 10,
  pythonInterpreter: "",
  powershellInterpreter: "",
  batchInterpreter: "",
  webhooksEnabled: false,
  allowedRoots: [],
};

function optionalValue(value: string | null | undefined) {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function emptyStep(revision?: ExtensionRevision): ProfileStep {
  return {
    extensionId: revision?.extensionId ?? "",
    revisionId: revision?.revisionId ?? "",
    runWhen: "ALWAYS",
    onFailure: "STOP",
    outcomeImpact: "WARN",
    timeoutSeconds: "86400",
    unlimitedTimeout: false,
    approvedRoots: "",
    requiredArtifactSuffixes: "",
    minimumArtifactCount: "0",
    options: "[]",
  };
}

function parseLines(value: string) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

export function PostProcessingSettingsPage() {
  const [{ data, fetching, error }, refresh] = useQuery<QueryData>({
    query: POST_PROCESSING_SETTINGS_QUERY,
    requestPolicy: "cache-and-network",
  });
  const [settings, setSettings] = useState(EMPTY_SETTINGS);
  const [allowedRoots, setAllowedRoots] = useState("");
  const [profileId, setProfileId] = useState("");
  const [profileName, setProfileName] = useState("");
  const [profileEnabled, setProfileEnabled] = useState(true);
  const [steps, setSteps] = useState<ProfileStep[]>([]);
  const [category, setCategory] = useState("");
  const [assignmentProfileId, setAssignmentProfileId] = useState("__none__");
  const [globalProfileId, setGlobalProfileId] = useState("__none__");
  const [notice, setNotice] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [jobIdInput, setJobIdInput] = useState("");
  const [inspectedJobId, setInspectedJobId] = useState<number | null>(null);
  const [selectedRunId, setSelectedRunId] = useState("");
  const [selectedAttemptId, setSelectedAttemptId] = useState("");
  const [jobSelectionMode, setJobSelectionMode] = useState<
    "INHERIT" | "DISABLED" | "PROFILE" | "EXTENSIONS"
  >("INHERIT");
  const [jobSelectionProfileId, setJobSelectionProfileId] = useState("__none__");
  const [jobSelectionExtensions, setJobSelectionExtensions] = useState("");

  const [{ data: queueData, error: queueError }, refreshQueue] = useQuery<QueueData>({
    query: POST_PROCESSING_QUEUE_QUERY,
    requestPolicy: "network-only",
  });
  const [{ data: jobData, error: jobError }, refreshJob] = useQuery<JobData>({
    query: POST_PROCESSING_JOB_QUERY,
    variables: { jobId: inspectedJobId ?? 0 },
    pause: inspectedJobId === null,
    requestPolicy: "network-only",
  });
  const [{ data: attemptsData, error: attemptsError }] = useQuery<AttemptsData>({
    query: POST_PROCESSING_ATTEMPTS_QUERY,
    variables: { runId: selectedRunId },
    pause: !selectedRunId,
    requestPolicy: "network-only",
  });
  const [{ data: logsData, error: logsError }] = useQuery<LogsData>({
    query: POST_PROCESSING_LOGS_QUERY,
    variables: { attemptId: selectedAttemptId, cursor: null, limit: 500 },
    pause: !selectedAttemptId,
    requestPolicy: "network-only",
  });

  const [saveSettingsState, saveSettings] = useMutation(
    UPDATE_POST_PROCESSING_SETTINGS_MUTATION,
  );
  const [discoverState, discover] = useMutation(
    DISCOVER_POST_PROCESSING_EXTENSIONS_MUTATION,
  );
  const [, approveRevision] = useMutation(APPROVE_POST_PROCESSING_REVISION_MUTATION);
  const [, disableRevision] = useMutation(DISABLE_POST_PROCESSING_REVISION_MUTATION);
  const [, revokeRevision] = useMutation(REVOKE_POST_PROCESSING_REVISION_MUTATION);
  const [saveProfileState, saveProfile] = useMutation(SAVE_POST_PROCESSING_PROFILE_MUTATION);
  const [, deleteProfile] = useMutation(DELETE_POST_PROCESSING_PROFILE_MUTATION);
  const [, assignGlobalProfile] = useMutation(
    ASSIGN_GLOBAL_POST_PROCESSING_PROFILE_MUTATION,
  );
  const [, assignCategoryProfile] = useMutation(
    ASSIGN_CATEGORY_POST_PROCESSING_PROFILE_MUTATION,
  );
  const [, pauseQueue] = useMutation(PAUSE_POST_PROCESSING_QUEUE_MUTATION);
  const [, resumeQueue] = useMutation(RESUME_POST_PROCESSING_QUEUE_MUTATION);
  const [, reorderQueue] = useMutation(REORDER_POST_PROCESSING_QUEUE_MUTATION);
  const [, cancelJobPostProcessing] = useMutation(CANCEL_JOB_POST_PROCESSING_MUTATION);
  const [rerunState, rerunPostProcessing] = useMutation(RERUN_POST_PROCESSING_MUTATION);
  const [, setJobSelection] = useMutation(SET_JOB_POST_PROCESSING_SELECTION_MUTATION);

  useEffect(() => {
    if (!data?.postProcessingSettings) return;
    setSettings(data.postProcessingSettings);
    setAllowedRoots(data.postProcessingSettings.allowedRoots.join("\n"));
  }, [data?.postProcessingSettings]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      refreshQueue({ requestPolicy: "network-only" });
      if (inspectedJobId !== null) refreshJob({ requestPolicy: "network-only" });
    }, 3000);
    return () => window.clearInterval(timer);
  }, [inspectedJobId, refreshJob, refreshQueue]);

  useEffect(() => {
    const runs = jobData?.postProcessingRuns ?? [];
    if (!runs.length) {
      setSelectedRunId("");
      return;
    }
    if (!runs.some((run) => run.runId === selectedRunId)) setSelectedRunId(runs[0].runId);
  }, [jobData?.postProcessingRuns, selectedRunId]);

  useEffect(() => {
    const attempts = attemptsData?.postProcessingAttempts ?? [];
    if (!attempts.length) {
      setSelectedAttemptId("");
      return;
    }
    if (!attempts.some((attempt) => attempt.attemptId === selectedAttemptId)) {
      setSelectedAttemptId(attempts[attempts.length - 1].attemptId);
    }
  }, [attemptsData?.postProcessingAttempts, selectedAttemptId]);

  const revisions = useMemo(
    () =>
      [...(data?.postProcessingRevisions ?? [])].sort((left, right) =>
        left.displayName.localeCompare(right.displayName),
      ),
    [data?.postProcessingRevisions],
  );
  const approvedRevisions = revisions.filter((revision) => revision.trustState === "APPROVED");
  const profiles = data?.postProcessingProfiles ?? [];

  const resetFeedback = () => {
    setNotice(null);
    setActionError(null);
  };

  const reload = () => refresh({ requestPolicy: "network-only" });

  const handleSaveSettings = async () => {
    resetFeedback();
    const result = await saveSettings({
      input: {
        discoveryEnabled: settings.discoveryEnabled,
        executionEnabled: settings.executionEnabled,
        concurrency: Math.max(1, Math.min(8, Math.round(settings.concurrency))),
        terminationGraceSeconds: Math.max(0, Math.round(settings.terminationGraceSeconds)),
        pythonInterpreter: optionalValue(settings.pythonInterpreter),
        powershellInterpreter: optionalValue(settings.powershellInterpreter),
        batchInterpreter: optionalValue(settings.batchInterpreter),
        webhooksEnabled: settings.webhooksEnabled,
        allowedRoots: parseLines(allowedRoots),
      },
    });
    if (result.error) {
      setActionError(result.error.message);
      return;
    }
    setNotice(
      "Post-processing settings saved. A restart applies concurrency and termination-grace changes.",
    );
    reload();
  };

  const handleDiscover = async () => {
    resetFeedback();
    const result = await discover({});
    if (result.error) {
      setActionError(result.error.message);
      return;
    }
    setNotice(`Discovery found ${result.data?.discoverPostProcessingExtensions?.length ?? 0} package(s).`);
    reload();
  };

  const handleRevisionAction = async (
    action: "approve" | "disable" | "revoke",
    revision: ExtensionRevision,
  ) => {
    resetFeedback();
    const variables = {
      extensionId: revision.extensionId,
      revisionId: revision.revisionId,
    };
    const result =
      action === "approve"
        ? await approveRevision(variables)
        : action === "disable"
          ? await disableRevision(variables)
          : await revokeRevision(variables);
    if (result.error) {
      setActionError(result.error.message);
      return;
    }
    setNotice(`${revision.displayName} revision ${action}d.`);
    reload();
  };

  const addStep = () => setSteps((current) => [...current, emptyStep(approvedRevisions[0])]);

  const updateStep = (index: number, patch: Partial<ProfileStep>) =>
    setSteps((current) =>
      current.map((step, stepIndex) => (stepIndex === index ? { ...step, ...patch } : step)),
    );

  const moveStep = (index: number, direction: -1 | 1) => {
    const target = index + direction;
    if (target < 0 || target >= steps.length) return;
    setSteps((current) => {
      const next = [...current];
      [next[index], next[target]] = [next[target], next[index]];
      return next;
    });
  };

  const handleSaveProfile = async () => {
    resetFeedback();
    if (!profileId.trim() || !profileName.trim()) {
      setActionError("Profile ID and name are required.");
      return;
    }
    try {
      const inputSteps = steps.map((step) => ({
        extensionId: step.extensionId,
        revisionId: step.revisionId || null,
        runWhen: step.runWhen,
        onFailure: step.onFailure,
        outcomeImpact: step.outcomeImpact,
        timeoutSeconds: step.unlimitedTimeout
          ? null
          : Math.max(1, Number.parseInt(step.timeoutSeconds, 10) || 86400),
        unlimitedTimeout: step.unlimitedTimeout,
        approvedRoots: parseLines(step.approvedRoots),
        requiredArtifactSuffixes: parseLines(step.requiredArtifactSuffixes),
        minimumArtifactCount: Math.max(
          0,
          Number.parseInt(step.minimumArtifactCount, 10) || 0,
        ),
        options: JSON.parse(step.options || "[]"),
      }));
      const result = await saveProfile({
        input: {
          profileId: profileId.trim(),
          name: profileName.trim(),
          enabled: profileEnabled,
          steps: inputSteps,
        },
      });
      if (result.error) {
        setActionError(result.error.message);
        return;
      }
      setNotice(`Profile ${profileName.trim()} saved.`);
      setProfileId("");
      setProfileName("");
      setSteps([]);
      reload();
    } catch (parseError) {
      setActionError(
        parseError instanceof Error ? `Invalid options JSON: ${parseError.message}` : "Invalid options JSON.",
      );
    }
  };

  const handleDeleteProfile = async (profile: ProfileRecord) => {
    resetFeedback();
    if (!window.confirm(`Delete post-processing profile “${profile.name}”?`)) return;
    const result = await deleteProfile({ profileId: profile.profileId });
    if (result.error) {
      setActionError(result.error.message);
      return;
    }
    setNotice(`Profile ${profile.name} deleted.`);
    reload();
  };

  const handleAssignGlobal = async () => {
    resetFeedback();
    const result = await assignGlobalProfile({
      profileId: globalProfileId === "__none__" ? null : globalProfileId,
    });
    if (result.error) setActionError(result.error.message);
    else setNotice("Global default profile assignment saved.");
  };

  const handleAssignCategory = async () => {
    resetFeedback();
    if (!category.trim()) {
      setActionError("Enter a category name.");
      return;
    }
    const result = await assignCategoryProfile({
      category: category.trim(),
      profileId: assignmentProfileId === "__none__" ? null : assignmentProfileId,
    });
    if (result.error) setActionError(result.error.message);
    else setNotice(`Assignment for category ${category.trim()} saved.`);
  };

  const inspectJob = (jobId?: number) => {
    resetFeedback();
    const parsed = jobId ?? Number.parseInt(jobIdInput, 10);
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      setActionError("Enter a valid job ID.");
      return;
    }
    setJobIdInput(String(parsed));
    setInspectedJobId(parsed);
    setSelectedRunId("");
    setSelectedAttemptId("");
  };

  const handleQueueControl = async (action: "pause" | "resume") => {
    resetFeedback();
    const result = action === "pause" ? await pauseQueue({}) : await resumeQueue({});
    if (result.error) setActionError(result.error.message);
    else setNotice(`Post-processing queue ${action === "pause" ? "paused" : "resumed"}.`);
    refreshQueue({ requestPolicy: "network-only" });
  };

  const handleMoveQueuedRun = async (runId: string, offset: -1 | 1) => {
    const queued = (queueData?.postProcessingQueue ?? []).filter((run) => run.status === "QUEUED");
    const currentIndex = queued.findIndex((run) => run.runId === runId);
    const targetIndex = currentIndex + offset;
    if (currentIndex < 0 || targetIndex < 0 || targetIndex >= queued.length) return;
    const reordered = [...queued];
    [reordered[currentIndex], reordered[targetIndex]] = [
      reordered[targetIndex],
      reordered[currentIndex],
    ];
    resetFeedback();
    const result = await reorderQueue({ runIds: reordered.map((run) => run.runId) });
    if (result.error) setActionError(result.error.message);
    else setNotice("Post-processing queue order updated.");
    refreshQueue({ requestPolicy: "network-only" });
  };

  const handleCancelPostProcessing = async (jobId: number) => {
    resetFeedback();
    const result = await cancelJobPostProcessing({ jobId });
    if (result.error) setActionError(result.error.message);
    else setNotice(`Cancellation requested for job ${jobId}.`);
    refreshQueue({ requestPolicy: "network-only" });
  };

  const handleRerun = async (mode: "ALL" | "FAILED_AND_LATER") => {
    if (!selectedRunId) return;
    resetFeedback();
    const result = await rerunPostProcessing({
      input: {
        runId: selectedRunId,
        mode,
        stepIndexes: [],
        rebindToLatestApproved: false,
      },
    });
    if (result.error) {
      setActionError(result.error.message);
      return;
    }
    setNotice(`Created ${mode === "ALL" ? "full" : "failed-and-later"} script-only rerun.`);
    refreshJob({ requestPolicy: "network-only" });
    refreshQueue({ requestPolicy: "network-only" });
  };

  const handleSetJobSelection = async () => {
    if (inspectedJobId === null) return;
    resetFeedback();
    const extensionIds = jobSelectionExtensions
      .split(/[\n,]/)
      .map((value) => value.trim())
      .filter(Boolean);
    const result = await setJobSelection({
      jobId: inspectedJobId,
      selection: {
        mode: jobSelectionMode,
        profileId:
          jobSelectionMode === "PROFILE" && jobSelectionProfileId !== "__none__"
            ? jobSelectionProfileId
            : null,
        extensionIds: jobSelectionMode === "EXTENSIONS" ? extensionIds : [],
      },
    });
    if (result.error) setActionError(result.error.message);
    else {
      setNotice(`Frozen selection updated for job ${inspectedJobId}.`);
      refreshJob({ requestPolicy: "network-only" });
    }
  };

  const selectedRun = jobData?.postProcessingRuns.find((run) => run.runId === selectedRunId);
  const selectedAttempt = attemptsData?.postProcessingAttempts.find(
    (attempt) => attempt.attemptId === selectedAttemptId,
  );
  const inspectorError = queueError ?? jobError ?? attemptsError ?? logsError;
  const queueRuns = queueData?.postProcessingQueue ?? [];
  const queuedRuns = queueRuns.filter((run) => run.status === "QUEUED");

  return (
    <div className="max-w-[1180px] space-y-6">
      <PageHeader
        title="Post-processing"
        description="Discover, approve, and run terminal extensions after Weaver’s built-in pipeline."
      />

      {error || actionError || inspectorError ? (
        <div className="rounded-md border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {actionError ?? error?.message ?? inspectorError?.message}
        </div>
      ) : null}
      {notice ? (
        <div className="rounded-md border border-status-completed/40 bg-status-completed/10 px-4 py-3 text-sm text-status-completed">
          {notice}
        </div>
      ) : null}

      <SectionCard
        title="Execution and discovery"
        description="Host extensions are administrator-approved code. Discovery alone never approves or assigns a package."
      >
        <div className="space-y-5">
          <div className="flex items-center justify-between gap-4">
            <div>
              <Label>Discover packages in the data-directory scripts folder</Label>
              <p className="text-sm text-muted-foreground">Disabled by default after upgrades.</p>
            </div>
            <Switch
              checked={settings.discoveryEnabled}
              onCheckedChange={(checked) =>
                setSettings((current) => ({ ...current, discoveryEnabled: checked }))
              }
            />
          </div>
          <div className="flex items-center justify-between gap-4">
            <div>
              <Label>Execute approved post-processing plans</Label>
              <p className="text-sm text-muted-foreground">
                Existing jobs retain their frozen revision and options.
              </p>
            </div>
            <Switch
              checked={settings.executionEnabled}
              onCheckedChange={(checked) =>
                setSettings((current) => ({ ...current, executionEnabled: checked }))
              }
            />
          </div>
          {([
            ["webhooksEnabled", "Enable webhook extensions", "Allows approved extensions to send signed, retryable HTTP callbacks."],
          ] as const).map(([field, label, description]) => (
            <div className="flex items-center justify-between gap-4" key={field}>
              <div>
                <Label>{label}</Label>
                <p className="text-sm text-muted-foreground">{description}</p>
              </div>
              <Switch
                checked={settings[field]}
                onCheckedChange={(checked) =>
                  setSettings((current) => ({ ...current, [field]: checked }))
                }
              />
            </div>
          ))}
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="pp-concurrency">Concurrent attempts (1–8)</Label>
              <Input
                id="pp-concurrency"
                type="number"
                min={1}
                max={8}
                value={settings.concurrency}
                onChange={(event) =>
                  setSettings((current) => ({ ...current, concurrency: Number(event.target.value) }))
                }
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="pp-grace">Termination grace (seconds)</Label>
              <Input
                id="pp-grace"
                type="number"
                min={1}
                value={settings.terminationGraceSeconds}
                onChange={(event) =>
                  setSettings((current) => ({
                    ...current,
                    terminationGraceSeconds: Math.max(1, Number(event.target.value)),
                  }))
                }
              />
            </div>
            {([
              ["pythonInterpreter", "Python interpreter"],
              ["powershellInterpreter", "PowerShell interpreter"],
              ["batchInterpreter", "Batch interpreter"],
            ] as const).map(([field, label]) => (
              <div className="space-y-2" key={field}>
                <Label htmlFor={`pp-${field}`}>{label}</Label>
                <Input
                  id={`pp-${field}`}
                  value={settings[field] ?? ""}
                  placeholder="Use platform default"
                  onChange={(event) =>
                    setSettings((current) => ({ ...current, [field]: event.target.value }))
                  }
                />
              </div>
            ))}
          </div>
          <div className="space-y-2">
            <Label htmlFor="pp-roots">Administrator-approved filesystem roots</Label>
            <textarea
              id="pp-roots"
              className="min-h-24 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm"
              value={allowedRoots}
              onChange={(event) => setAllowedRoots(event.target.value)}
              placeholder="One absolute path per line"
            />
          </div>
          <div className="flex flex-wrap gap-2">
            <Button onClick={handleSaveSettings} disabled={saveSettingsState.fetching || fetching}>
              Save settings
            </Button>
            <Button
              variant="outline"
              onClick={handleDiscover}
              disabled={!settings.discoveryEnabled || discoverState.fetching}
            >
              Scan scripts folder
            </Button>
          </div>
        </div>
      </SectionCard>

      <SectionCard
        title="Discovered revisions"
        description="Approval imports an immutable digest-bound copy into Weaver’s managed store."
      >
        <div className="space-y-3">
          {revisions.length === 0 ? (
            <p className="text-sm text-muted-foreground">No extension revisions discovered.</p>
          ) : (
            revisions.map((revision) => (
              <div
                key={`${revision.extensionId}:${revision.revisionId}`}
                className="flex flex-col gap-3 rounded-md border border-border/70 p-4 md:flex-row md:items-center md:justify-between"
              >
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium">{revision.displayName}</span>
                    <Badge variant="outline">{revision.adapter}</Badge>
                    <Badge variant="outline">{revision.trustState}</Badge>
                  </div>
                  <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
                    {revision.extensionId} · {revision.declaredVersion} · {revision.digest.slice(0, 16)}…
                  </p>
                  {revision.sourcePath ? (
                    <p className="mt-1 truncate text-xs text-muted-foreground">{revision.sourcePath}</p>
                  ) : null}
                </div>
                <div className="flex shrink-0 flex-wrap gap-2">
                  {revision.trustState !== "APPROVED" ? (
                    <Button size="sm" onClick={() => handleRevisionAction("approve", revision)}>
                      Approve immutable revision
                    </Button>
                  ) : (
                    <>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => handleRevisionAction("disable", revision)}
                      >
                        Disable
                      </Button>
                      <Button
                        size="sm"
                        variant="destructive"
                        onClick={() => handleRevisionAction("revoke", revision)}
                      >
                        Revoke trust
                      </Button>
                    </>
                  )}
                </div>
              </div>
            ))
          )}
        </div>
      </SectionCard>

      <SectionCard
        title="Profiles"
        description="Steps run in order. Secret option values are encrypted and never returned by the API."
      >
        <div className="space-y-5">
          {profiles.map((profile) => (
            <div key={profile.profileId} className="flex items-center justify-between gap-4 rounded-md border p-3">
              <div>
                <div className="flex items-center gap-2">
                  <span className="font-medium">{profile.name}</span>
                  <Badge variant="outline">{profile.enabled ? "ENABLED" : "DISABLED"}</Badge>
                </div>
                <p className="font-mono text-xs text-muted-foreground">{profile.profileId}</p>
              </div>
              <Button variant="destructive" size="sm" onClick={() => handleDeleteProfile(profile)}>
                Delete
              </Button>
            </div>
          ))}

          <div className="rounded-md border border-border/70 p-4">
            <h3 className="font-medium">Create or replace a profile</h3>
            <div className="mt-4 grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="pp-profile-id">Profile ID</Label>
                <Input id="pp-profile-id" value={profileId} onChange={(e) => setProfileId(e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label htmlFor="pp-profile-name">Display name</Label>
                <Input
                  id="pp-profile-name"
                  value={profileName}
                  onChange={(e) => setProfileName(e.target.value)}
                />
              </div>
            </div>
            <div className="mt-4 flex items-center gap-3">
              <Switch checked={profileEnabled} onCheckedChange={setProfileEnabled} />
              <Label>Profile enabled</Label>
            </div>

            <div className="mt-5 space-y-4">
              {steps.map((step, index) => (
                <div key={`${index}:${step.extensionId}:${step.revisionId}`} className="rounded-md border p-4">
                  <div className="mb-4 flex items-center justify-between">
                    <span className="font-medium">Step {index + 1}</span>
                    <div className="flex gap-2">
                      <Button size="sm" variant="outline" onClick={() => moveStep(index, -1)} disabled={index === 0}>
                        Up
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => moveStep(index, 1)}
                        disabled={index === steps.length - 1}
                      >
                        Down
                      </Button>
                      <Button
                        size="sm"
                        variant="destructive"
                        onClick={() => setSteps((current) => current.filter((_, stepIndex) => stepIndex !== index))}
                      >
                        Remove
                      </Button>
                    </div>
                  </div>
                  <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                    <div className="space-y-2 md:col-span-2 lg:col-span-3">
                      <Label>Approved immutable revision</Label>
                      <Select
                        value={`${step.extensionId}|${step.revisionId}`}
                        onValueChange={(value) => {
                          const [extensionId, revisionId] = value.split("|", 2);
                          updateStep(index, { extensionId, revisionId });
                        }}
                      >
                        <SelectTrigger><SelectValue placeholder="Select an approved revision" /></SelectTrigger>
                        <SelectContent>
                          {approvedRevisions.map((revision) => (
                            <SelectItem
                              key={`${revision.extensionId}:${revision.revisionId}`}
                              value={`${revision.extensionId}|${revision.revisionId}`}
                            >
                              {revision.displayName} · {revision.declaredVersion} · {revision.revisionId.slice(0, 8)}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Run when</Label>
                      <Select value={step.runWhen} onValueChange={(value) => updateStep(index, { runWhen: value as ProfileStep["runWhen"] })}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="ALWAYS">Always</SelectItem>
                          <SelectItem value="SUCCESS">Pipeline succeeded</SelectItem>
                          <SelectItem value="FAILURE">Pipeline failed</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>After failure</Label>
                      <Select value={step.onFailure} onValueChange={(value) => updateStep(index, { onFailure: value as ProfileStep["onFailure"] })}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="STOP">Stop later steps</SelectItem>
                          <SelectItem value="CONTINUE">Continue</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Outcome impact</Label>
                      <Select value={step.outcomeImpact} onValueChange={(value) => updateStep(index, { outcomeImpact: value as ProfileStep["outcomeImpact"] })}>
                        <SelectTrigger><SelectValue /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="WARN">Warning</SelectItem>
                          <SelectItem value="FAIL_JOB">Fail successful job</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label>Timeout seconds</Label>
                      <Input
                        type="number"
                        min={1}
                        disabled={step.unlimitedTimeout}
                        value={step.timeoutSeconds}
                        onChange={(event) => updateStep(index, { timeoutSeconds: event.target.value })}
                      />
                    </div>
                    <div className="flex items-end gap-3 pb-2">
                      <Switch
                        checked={step.unlimitedTimeout}
                        onCheckedChange={(checked) => updateStep(index, { unlimitedTimeout: checked })}
                      />
                      <Label>Unlimited timeout</Label>
                    </div>
                    <div className="space-y-2 md:col-span-2 lg:col-span-3">
                      <Label>Step-approved roots (one per line)</Label>
                      <textarea
                        className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm"
                        value={step.approvedRoots}
                        onChange={(event) => updateStep(index, { approvedRoots: event.target.value })}
                      />
                    </div>
                    <div className="space-y-2 md:col-span-2">
                      <Label>Required artifact suffixes (one per line)</Label>
                      <textarea
                        className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm"
                        value={step.requiredArtifactSuffixes}
                        onChange={(event) =>
                          updateStep(index, { requiredArtifactSuffixes: event.target.value })
                        }
                        placeholder=".mkv"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Minimum artifact count</Label>
                      <Input
                        type="number"
                        min={0}
                        value={step.minimumArtifactCount}
                        onChange={(event) =>
                          updateStep(index, { minimumArtifactCount: event.target.value })
                        }
                      />
                    </div>
                    <div className="space-y-2 md:col-span-2 lg:col-span-3">
                      <Label>Options JSON</Label>
                      <textarea
                        className="min-h-24 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm"
                        value={step.options}
                        onChange={(event) => updateStep(index, { options: event.target.value })}
                        placeholder='[{"name":"TOKEN","kind":"SECRET","value":"..."}]'
                      />
                    </div>
                  </div>
                </div>
              ))}
              <div className="flex flex-wrap gap-2">
                <Button variant="outline" onClick={addStep} disabled={approvedRevisions.length === 0}>
                  Add step
                </Button>
                <Button onClick={handleSaveProfile} disabled={saveProfileState.fetching}>
                  Save profile
                </Button>
              </div>
            </div>
          </div>
        </div>
      </SectionCard>

      <SectionCard
        title="Default assignments"
        description="Submission precedence is explicit job selection, then category profile, then global default, then an empty plan."
      >
        <div className="grid gap-5 md:grid-cols-2">
          <div className="space-y-3">
            <Label>Global default</Label>
            <Select value={globalProfileId} onValueChange={setGlobalProfileId}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__">No global default</SelectItem>
                {profiles.map((profile) => (
                  <SelectItem key={profile.profileId} value={profile.profileId}>{profile.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button onClick={handleAssignGlobal}>Save global default</Button>
          </div>
          <div className="space-y-3">
            <Label htmlFor="pp-category">Category assignment</Label>
            <Input id="pp-category" value={category} onChange={(event) => setCategory(event.target.value)} placeholder="movies" />
            <Select value={assignmentProfileId} onValueChange={setAssignmentProfileId}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="__none__">Inherit global default</SelectItem>
                {profiles.map((profile) => (
                  <SelectItem key={profile.profileId} value={profile.profileId}>{profile.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button onClick={handleAssignCategory}>Save category assignment</Button>
          </div>
        </div>
      </SectionCard>

      <SectionCard
        title="Post-processing queue"
        description="Queued and active runs refresh every three seconds. Pausing stops new attempts without killing the active one."
      >
        <div className="space-y-4">
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => handleQueueControl("pause")}>
              Pause queue
            </Button>
            <Button variant="outline" onClick={() => handleQueueControl("resume")}>
              Resume queue
            </Button>
            <Button variant="outline" onClick={() => refreshQueue({ requestPolicy: "network-only" })}>
              Refresh
            </Button>
          </div>
          {queueRuns.length === 0 ? (
            <p className="text-sm text-muted-foreground">No queued or active extension runs.</p>
          ) : (
            queueRuns.map((run) => (
              <div
                key={run.runId}
                className="flex flex-col gap-3 rounded-md border p-4 md:flex-row md:items-center md:justify-between"
              >
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium">Job {run.jobId}</span>
                    <Badge variant="outline">{run.status}</Badge>
                    <Badge variant="outline">{run.summary}</Badge>
                  </div>
                  <p className="mt-1 font-mono text-xs text-muted-foreground">{run.runId}</p>
                </div>
                <div className="flex flex-wrap gap-2">
                  {run.status === "QUEUED" ? (
                    <>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={queuedRuns[0]?.runId === run.runId}
                        onClick={() => handleMoveQueuedRun(run.runId, -1)}
                      >
                        Move up
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={queuedRuns.at(-1)?.runId === run.runId}
                        onClick={() => handleMoveQueuedRun(run.runId, 1)}
                      >
                        Move down
                      </Button>
                    </>
                  ) : null}
                  <Button size="sm" variant="outline" onClick={() => inspectJob(run.jobId)}>
                    Inspect
                  </Button>
                  <Button
                    size="sm"
                    variant="destructive"
                    onClick={() => handleCancelPostProcessing(run.jobId)}
                  >
                    Cancel attempt
                  </Button>
                </div>
              </div>
            ))
          )}
        </div>
      </SectionCard>

      <SectionCard
        title="Job plan and run inspector"
        description="Inspect frozen plans, attempts, bounded redacted logs, and script-only reruns for active or historical jobs."
      >
        <div className="space-y-5">
          <div className="flex max-w-md gap-2">
            <Input
              value={jobIdInput}
              onChange={(event) => setJobIdInput(event.target.value)}
              placeholder="Job ID"
              inputMode="numeric"
            />
            <Button onClick={() => inspectJob()}>Inspect</Button>
          </div>

          {inspectedJobId !== null ? (
            <>
              <div className="rounded-md border p-4">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <h3 className="font-medium">Frozen plan for job {inspectedJobId}</h3>
                  <Badge variant="outline">
                    {jobData?.postProcessingJobPlan ? "FROZEN" : "NO PLAN"}
                  </Badge>
                </div>
                <pre className="mt-3 max-h-72 overflow-auto rounded-md bg-muted/40 p-3 text-xs">
                  {JSON.stringify(jobData?.postProcessingJobPlan?.definition ?? null, null, 2)}
                </pre>
              </div>

              <div className="rounded-md border p-4">
                <h3 className="font-medium">Replace selection before execution starts</h3>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Selection mode</Label>
                    <Select
                      value={jobSelectionMode}
                      onValueChange={(value) =>
                        setJobSelectionMode(value as typeof jobSelectionMode)
                      }
                    >
                      <SelectTrigger><SelectValue /></SelectTrigger>
                      <SelectContent>
                        <SelectItem value="INHERIT">Inherit category/global default</SelectItem>
                        <SelectItem value="DISABLED">Disable for this job</SelectItem>
                        <SelectItem value="PROFILE">Profile</SelectItem>
                        <SelectItem value="EXTENSIONS">Ordered extension IDs</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {jobSelectionMode === "PROFILE" ? (
                    <div className="space-y-2">
                      <Label>Profile</Label>
                      <Select value={jobSelectionProfileId} onValueChange={setJobSelectionProfileId}>
                        <SelectTrigger><SelectValue placeholder="Select profile" /></SelectTrigger>
                        <SelectContent>
                          <SelectItem value="__none__">Select profile</SelectItem>
                          {profiles.map((profile) => (
                            <SelectItem key={profile.profileId} value={profile.profileId}>
                              {profile.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  ) : null}
                  {jobSelectionMode === "EXTENSIONS" ? (
                    <div className="space-y-2 md:col-span-2">
                      <Label>Extension IDs in execution order</Label>
                      <textarea
                        className="min-h-20 w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-sm"
                        value={jobSelectionExtensions}
                        onChange={(event) => setJobSelectionExtensions(event.target.value)}
                        placeholder="One extension ID per line"
                      />
                    </div>
                  ) : null}
                </div>
                <Button className="mt-4" onClick={handleSetJobSelection}>
                  Freeze replacement selection
                </Button>
              </div>

              <div className="grid gap-5 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
                <div className="space-y-3">
                  <h3 className="font-medium">Runs</h3>
                  {(jobData?.postProcessingRuns ?? []).length === 0 ? (
                    <p className="text-sm text-muted-foreground">No post-processing runs recorded.</p>
                  ) : (
                    (jobData?.postProcessingRuns ?? []).map((run) => (
                      <button
                        type="button"
                        key={run.runId}
                        onClick={() => {
                          setSelectedRunId(run.runId);
                          setSelectedAttemptId("");
                        }}
                        className={`w-full rounded-md border p-3 text-left ${
                          run.runId === selectedRunId ? "border-primary bg-primary/5" : "border-border"
                        }`}
                      >
                        <div className="flex flex-wrap items-center gap-2">
                          <Badge variant="outline">{run.status}</Badge>
                          <Badge variant="outline">{run.summary}</Badge>
                        </div>
                        <p className="mt-2 truncate font-mono text-xs text-muted-foreground">
                          {run.runId}
                        </p>
                      </button>
                    ))
                  )}
                  {selectedRun ? (
                    <div className="flex flex-wrap gap-2">
                      <Button
                        size="sm"
                        onClick={() => handleRerun("ALL")}
                        disabled={rerunState.fetching}
                      >
                        Rerun all scripts
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => handleRerun("FAILED_AND_LATER")}
                        disabled={rerunState.fetching}
                      >
                        Rerun failed and later
                      </Button>
                    </div>
                  ) : null}
                </div>

                <div className="space-y-3">
                  <h3 className="font-medium">Attempts and live progress</h3>
                  {(attemptsData?.postProcessingAttempts ?? []).map((attempt) => (
                    <button
                      type="button"
                      key={attempt.attemptId}
                      onClick={() => setSelectedAttemptId(attempt.attemptId)}
                      className={`w-full rounded-md border p-3 text-left ${
                        attempt.attemptId === selectedAttemptId
                          ? "border-primary bg-primary/5"
                          : "border-border"
                      }`}
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-medium">Step {attempt.stepIndex + 1}</span>
                        <Badge variant="outline">{attempt.adapter}</Badge>
                        <Badge variant="outline">{attempt.status}</Badge>
                        {attempt.outputTruncated ? <Badge variant="outline">TRUNCATED</Badge> : null}
                      </div>
                      <p className="mt-1 truncate text-xs text-muted-foreground">
                        {attempt.extensionId} · exit {attempt.exitCode ?? "—"}
                      </p>
                      {attempt.progress ? (
                        <pre className="mt-2 overflow-auto text-xs">
                          {JSON.stringify(attempt.progress, null, 2)}
                        </pre>
                      ) : null}
                      {attempt.errorMessage ? (
                        <p className="mt-2 text-sm text-destructive">{attempt.errorMessage}</p>
                      ) : null}
                    </button>
                  ))}
                </div>
              </div>

              {selectedAttempt ? (
                <div className="rounded-md border p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <h3 className="font-medium">Redacted bounded log</h3>
                    <span className="font-mono text-xs text-muted-foreground">
                      {selectedAttempt.attemptId}
                    </span>
                  </div>
                  <pre className="mt-3 max-h-[32rem] overflow-auto whitespace-pre-wrap rounded-md bg-black/90 p-4 text-xs text-white">
                    {(logsData?.postProcessingLogs.chunks ?? [])
                      .map((chunk) => `[${chunk.stream}] ${chunk.text}`)
                      .join("") || "No log output."}
                  </pre>
                  {logsData?.postProcessingLogs.truncated ? (
                    <p className="mt-2 text-xs text-muted-foreground">
                      Output exceeded the persisted cap; the header and rolling tail were retained.
                    </p>
                  ) : null}
                </div>
              ) : null}
            </>
          ) : null}
        </div>
      </SectionCard>
    </div>
  );
}
