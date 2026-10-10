/**
 * The script documents the web app sends, copied as they are written in the
 * web app's GraphQL module, so the post-processing flow runs the same shapes
 * against a live server that the settings screens do. graphql-documents
 * checks them against the exported schema like every other document here.
 * When the web app changes one of these, change it here too.
 */

// Each fragment opens with a comment: graphql-documents validates a literal
// that starts with an operation keyword on its own, and a fragment is only
// ever sent spread into the documents below.
const POST_PROCESSING_SETTINGS_FIELDS = `
  # spread only
  fragment PostProcessingSettingsFields on PostProcessingSettingsGql {
    scriptDirectory
    executionEnabled
    concurrency
    eventScriptConcurrency
    eventScriptTimeoutSeconds
    fileDownloadedEventInterval
    scriptOutputRunsPerJob
    scriptOutputFailedRunsPerJob
    terminationGraceSeconds
    pythonInterpreter
    powershellInterpreter
    batchInterpreter
    goInterpreter
    unacceptableExtensions
    strictSecurityRefusesExecution
    globalScriptsRun
  }
`;

const SCRIPT_INSTANCE_FIELDS = `
  # spread only
  fragment ScriptInstanceFields on ScriptInstance {
    id
    name
    script
    trigger
    queueEvent
    inputs {
      name
      value
      sealed
      secret {
        id
        name
      }
    }
    categories
    enabled
    blocking
    timeoutSeconds
    schedule {
      days
      times
      runAtStartup
    }
    runOrder
    scriptProblem
    headerDrift
  }
`;

const SCRIPT_TEST_RUN_FIELDS = `
  # spread only
  fragment ScriptTestRunFields on ScriptTestRun {
    id
    instanceId
    instanceName
    script
    event
    kind
    adapter
    startedAtEpochMs
    timeoutSeconds
    running
    status
    exitCode
    durationMs
    errorMessage
    log
    logTruncated
    inputs {
      name
      value
    }
    arguments
    commands
    commandsTruncated
  }
`;

const SECRET_FIELDS = `
  # spread only
  fragment SecretFields on Secret {
    id
    name
    createdAt
    updatedAt
    usedBy {
      id
      name
    }
  }
`;

export const POST_PROCESSING_SETTINGS_QUERY = `
  query PostProcessingSettings {
    postProcessingSettings {
      ...PostProcessingSettingsFields
    }
  }
  ${POST_PROCESSING_SETTINGS_FIELDS}
`;

export const SCRIPT_INSTANCES_QUERY = `
  query ScriptInstances {
    postProcessingSettings {
      scriptDirectory
      globalScriptsRun
    }
    scriptInstances {
      ...ScriptInstanceFields
    }
    categories {
      id
      name
    }
  }
  ${SCRIPT_INSTANCE_FIELDS}
`;

export const DISCOVERED_SCRIPTS_QUERY = `
  query DiscoveredScripts {
    discoveredScripts {
      scripts {
        name
        displayName
        adapter
        kinds
        queueEvents
        taskTimes
        version
        options {
          name
          section
          optionType
          displayName
          description
          select
          required
          defaultValue
        }
        preset {
          triggers {
            trigger
            queueEvent
          }
          taskTimes
          inputs {
            name
            value
            secret
          }
        }
      }
      problems {
        name
        message
      }
    }
  }
`;

export const CREATE_SCRIPT_INSTANCE_MUTATION = `
  mutation CreateScriptInstance($input: ScriptInstanceInput!) {
    createScriptInstance(input: $input) {
      ...ScriptInstanceFields
    }
  }
  ${SCRIPT_INSTANCE_FIELDS}
`;

export const UPDATE_SCRIPT_INSTANCE_MUTATION = `
  mutation UpdateScriptInstance($id: String!, $input: ScriptInstanceInput!) {
    updateScriptInstance(id: $id, input: $input) {
      ...ScriptInstanceFields
    }
  }
  ${SCRIPT_INSTANCE_FIELDS}
`;

export const SECRETS_QUERY = `
  query Secrets {
    secrets {
      ...SecretFields
    }
  }
  ${SECRET_FIELDS}
`;

export const CREATE_SECRET_MUTATION = `
  mutation CreateSecret($name: String!, $value: String!) {
    createSecret(name: $name, value: $value) {
      ...SecretFields
    }
  }
  ${SECRET_FIELDS}
`;

export const UPDATE_SECRET_MUTATION = `
  mutation UpdateSecret($id: String!, $name: String, $value: String) {
    updateSecret(id: $id, name: $name, value: $value) {
      ...SecretFields
    }
  }
  ${SECRET_FIELDS}
`;

export const DELETE_SECRET_MUTATION = `
  mutation DeleteSecret($id: String!) {
    deleteSecret(id: $id)
  }
`;

export const DELETE_SCRIPT_INSTANCE_MUTATION = `
  mutation DeleteScriptInstance($id: String!) {
    deleteScriptInstance(id: $id)
  }
`;

export const REORDER_SCRIPT_INSTANCES_MUTATION = `
  mutation ReorderScriptInstances($trigger: ScriptKind!, $ids: [String!]!) {
    reorderScriptInstances(trigger: $trigger, ids: $ids) {
      ...ScriptInstanceFields
    }
  }
  ${SCRIPT_INSTANCE_FIELDS}
`;

export const SET_UP_SCRIPT_FROM_HEADER_MUTATION = `
  mutation SetUpScriptFromHeader($script: String!) {
    setUpScriptFromHeader(script: $script) {
      ...ScriptInstanceFields
    }
  }
  ${SCRIPT_INSTANCE_FIELDS}
`;

export const REAPPLY_SCRIPT_HEADER_MUTATION = `
  mutation ReapplyScriptHeader($id: String!) {
    reapplyScriptHeader(id: $id) {
      ...ScriptInstanceFields
    }
  }
  ${SCRIPT_INSTANCE_FIELDS}
`;

export const TEST_SCRIPT_INSTANCE_MUTATION = `
  mutation TestScriptInstance($id: String!) {
    testScriptInstance(id: $id) {
      ...ScriptTestRunFields
    }
  }
  ${SCRIPT_TEST_RUN_FIELDS}
`;

export const SCRIPT_TEST_RUN_QUERY = `
  query ScriptTestRun($id: String!) {
    scriptTestRun(id: $id) {
      ...ScriptTestRunFields
    }
  }
  ${SCRIPT_TEST_RUN_FIELDS}
`;

export const CANCEL_SCRIPT_TEST_MUTATION = `
  mutation CancelScriptTest($id: String!) {
    cancelScriptTest(id: $id)
  }
`;

export const POST_PROCESSING_RESULTS_QUERY = `
  query PostProcessingResults($jobId: Int!) {
    postProcessingResults(jobId: $jobId) {
      script
      instanceId
      instanceName
      event
      adapter
      status
      exitCode
      durationMs
      outputTail
      outputId
      outputRetained
      outputTruncated
      errorMessage
      finishedAtEpochMs
      background
    }
  }
`;

export const SCRIPT_RUNS_QUERY = `
  query ScriptRuns(
    $limit: Int
    $before: String
    $kind: ScriptKind
    $script: String
    $jobId: Int
    $status: ScriptStatusGql
  ) {
    scriptRuns(limit: $limit, before: $before, kind: $kind, script: $script, jobId: $jobId, status: $status) {
      runs {
        id
        jobId
        jobName
        script
        instanceId
        instanceName
        event
        kind
        background
        adapter
        status
        exitCode
        durationMs
        outputTail
        outputTruncated
        outputRetained
        errorMessage
        finishedAtEpochMs
      }
      nextBefore
      total
      statusCounts {
        status
        count
      }
    }
  }
`;

/** The Runs screen reads a run's whole retained output with this. */
export const SCRIPT_RUN_OUTPUT_QUERY = `
  query ScriptRunOutput($outputId: String!) {
    scriptOutput(outputId: $outputId)
  }
`;

export const RERUN_POST_PROCESSING_MUTATION = `
  mutation RerunPostProcessing($jobId: Int!) {
    rerunPostProcessing(jobId: $jobId)
  }
`;

export const CANCEL_JOB_POST_PROCESSING_MUTATION = `
  mutation CancelJobPostProcessing($jobId: Int!) {
    cancelJobPostProcessing(jobId: $jobId)
  }
`;

export type ScriptRun = {
  id: string; jobId: number | null; jobName: string | null; script: string; instanceId: string | null;
  instanceName: string | null; event: string; kind: string; background: boolean; adapter: string; status: string;
  exitCode: number | null; durationMs: number; outputTail: string; outputTruncated: boolean; outputRetained: boolean;
  errorMessage: string | null; finishedAtEpochMs: number;
};

export type ScriptRunPage = {
  runs: ScriptRun[]; nextBefore: string | null; total: number; statusCounts: Array<{ status: string; count: number }>;
};

export type ScriptTestRun = {
  id: string; instanceId: string; instanceName: string; script: string; event: string; kind: string; adapter: string;
  startedAtEpochMs: number; timeoutSeconds: number; running: boolean; status: string | null; exitCode: number | null;
  durationMs: number | null; errorMessage: string | null; log: string; logTruncated: boolean;
  inputs: Array<{ name: string; value: string }>; arguments: string[]; commands: string[]; commandsTruncated: boolean;
};
