import fs from "node:fs";
import path from "node:path";

/**
 * Script fixtures for the event-scripts, scan-scripts and scheduling flows.
 *
 * Weaver runs scripts from `data_dir/scripts`; Playwright shares that volume
 * at /weaver-data. Every fixture starts with the same recorder: it writes the
 * environment it was given, its working directory and its argument count to
 * `/data/script-records/<id>.env` (renamed into place, so a reader never sees
 * half a record). Assertions read those records instead of trusting output.
 *
 * A gated fixture blocks on a FIFO named after the script and job until the
 * test writes to it, which is how a test holds a run in `started` without a
 * sleep on either side.
 */
export const SCRIPTS_DIR = "/weaver-data/scripts";
export const RECORDS_DIR = "/weaver-data/script-records";
const WEAVER_RECORDS_DIR = "/data/script-records";
const GATES_DIR = "/weaver-data/script-gates";
const WEAVER_GATES_DIR = "/data/script-gates";

export type ScriptKind = "POST-PROCESSING" | "QUEUE" | "SCAN" | "SCHEDULER" | "FEED";

export type ScriptRecord = {
  id: string;
  script: string;
  cwd: string;
  argc: number;
  env: Record<string, string>;
};

export type FixtureOptions = {
  /** Header kinds, joined with "/" as NZBGet does. */
  kinds: ScriptKind[];
  queueEvents?: string[];
  taskTimes?: string[];
  /** Block on a FIFO keyed by script name and job id before the body runs. */
  gate?: boolean;
  /** Shell run after the record is written and the gate (if any) opens. */
  body?: string;
  exitCode?: number;
};

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

/** The recorder every fixture starts with. */
function recorder(name: string): string {
  return `record_dir=${WEAVER_RECORDS_DIR}
mkdir -p "$record_dir"
record_id="$(date +%s)-$$-$(od -An -N4 -tu4 /dev/urandom | tr -d ' \\n')"
{
  printf '@SCRIPT=%s\\n' ${shellQuote(name)}
  printf '@CWD=%s\\n' "$(pwd)"
  printf '@ARGC=%s\\n' "$#"
  env
} > "$record_dir/.$record_id"
mv "$record_dir/.$record_id" "$record_dir/$record_id.env"
`;
}

function gate(name: string): string {
  return `job_id="\${NZBNA_NZBID:-\${NZBPP_NZBID:-\${NZBNP_NZBNAME:-none}}}"
mkdir -p ${WEAVER_GATES_DIR}
gate=${WEAVER_GATES_DIR}/${name}-"$job_id"
mkfifo "$gate"
read -r release < "$gate"
rm -f "$gate"
`;
}

/** A bare NZBGet-style script with the given header declarations. */
export function writeFixtureScript(name: string, options: FixtureOptions): string {
  if (!/^[A-Za-z0-9._-]+$/.test(name)) throw new Error(`invalid fixture script name ${name}`);
  const header = [
    "#!/bin/sh",
    `### NZBGET ${options.kinds.join("/")} SCRIPT ###`,
    ...(options.queueEvents ? [`### QUEUE EVENTS: ${options.queueEvents.join(", ")} ###`] : []),
    ...(options.taskTimes ? [`### TASK TIME: ${options.taskTimes.join(";")} ###`] : []),
    "",
  ].join("\n");
  const source = `${header}${recorder(name)}${options.gate ? gate(name) : ""}${options.body ?? ""}
exit ${options.exitCode ?? 0}
`;
  const target = path.join(SCRIPTS_DIR, name);
  fs.mkdirSync(SCRIPTS_DIR, { recursive: true });
  fs.rmSync(target, { recursive: true, force: true });
  fs.writeFileSync(`${target}.tmp`, source, { mode: 0o755 });
  fs.renameSync(`${target}.tmp`, target);
  return name;
}

/** A manifest package (`manifest.json` + `run.sh`) with NZBGet options. */
export function writeFixturePackage(
  name: string,
  options: FixtureOptions & { scriptOptions?: Array<{ name: string; value: string; secret?: boolean }> },
): string {
  const directory = path.join(SCRIPTS_DIR, name);
  fs.rmSync(directory, { recursive: true, force: true });
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, "manifest.json"), `${JSON.stringify({
    main: "run.sh",
    name,
    kind: options.kinds.join("/"),
    displayName: name,
    version: "1.0.0",
    author: "Weaver e2e",
    homepage: "https://example.invalid",
    license: "GNU",
    about: "Records the environment Weaver gives it.",
    description: ["e2e fixture"],
    requirements: [],
    queueEvents: (options.queueEvents ?? []).join(", "),
    taskTime: (options.taskTimes ?? []).join(";"),
    sections: [],
    commands: [],
    options: (options.scriptOptions ?? []).map(option => ({
      name: option.name, displayName: option.name, value: option.value,
      description: ["e2e option"], select: [], ...(option.secret ? { secret: true } : {}),
    })),
  }, null, 2)}\n`);
  fs.writeFileSync(path.join(directory, "run.sh"), `#!/bin/sh
${recorder(name)}${options.gate ? gate(name) : ""}${options.body ?? ""}
exit ${options.exitCode ?? 0}
`, { mode: 0o755 });
  return name;
}

/** A bare script with no NZBGet header, which Weaver runs with the SABnzbd adapter. */
export function writeBareScript(name: string, options: { body?: string; exitCode?: number } = {}): string {
  if (!/^[A-Za-z0-9._-]+$/.test(name)) throw new Error(`invalid fixture script name ${name}`);
  const target = path.join(SCRIPTS_DIR, name);
  fs.mkdirSync(SCRIPTS_DIR, { recursive: true });
  fs.rmSync(target, { recursive: true, force: true });
  fs.writeFileSync(`${target}.tmp`, `#!/bin/sh\n${recorder(name)}${options.body ?? ""}\nexit ${options.exitCode ?? 0}\n`, { mode: 0o755 });
  fs.renameSync(`${target}.tmp`, target);
  return name;
}

/**
 * Remove a gate whose run is gone (cancelled, timed out or killed by a
 * restart). Never release such a gate: with no reader, opening it for write
 * would block for good.
 */
export function removeStaleGate(script: string, jobKey: string | number): void {
  fs.rmSync(path.join(GATES_DIR, `${script}-${jobKey}`), { force: true });
}

export function removeFixtureScripts(names: string[]): void {
  for (const name of names) fs.rmSync(path.join(SCRIPTS_DIR, name), { recursive: true, force: true });
}

export function clearScriptRecords(): void {
  fs.rmSync(RECORDS_DIR, { recursive: true, force: true });
}

export function parseScriptRecord(id: string, text: string): ScriptRecord {
  const env: Record<string, string> = {};
  const meta: Record<string, string> = {};
  let last: string | undefined;
  for (const line of text.split("\n")) {
    const metaMatch = /^@([A-Z]+)=(.*)$/.exec(line);
    if (metaMatch) { meta[metaMatch[1]!] = metaMatch[2]!; last = undefined; continue; }
    const match = /^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/.exec(line);
    if (match) { env[match[1]!] = match[2]!; last = match[1]; continue; }
    // A value with a newline continues on the following lines.
    if (last !== undefined && line !== "") env[last] += `\n${line}`;
  }
  return { id, script: meta.SCRIPT ?? "", cwd: meta.CWD ?? "", argc: Number(meta.ARGC ?? 0), env };
}

/** Every complete record, oldest first. */
export function scriptRecords(script?: string): ScriptRecord[] {
  if (!fs.existsSync(RECORDS_DIR)) return [];
  return fs.readdirSync(RECORDS_DIR)
    .filter(file => file.endsWith(".env") && !file.startsWith("."))
    // The id carries whole seconds, so order by the file's own write time
    // (rename keeps it) and fall back to the id for files written together.
    .map(file => ({ file, writtenAt: fs.statSync(path.join(RECORDS_DIR, file)).mtimeMs }))
    .sort((left, right) => (left.writtenAt - right.writtenAt) || left.file.localeCompare(right.file))
    .map(({ file }) => parseScriptRecord(file.replace(/\.env$/, ""), fs.readFileSync(path.join(RECORDS_DIR, file), "utf8")))
    .filter(record => script === undefined || record.script === script);
}

/** Whether a gated fixture run for this job is blocked at its gate. */
export function gateWaiting(script: string, jobKey: string | number): boolean {
  return fs.existsSync(path.join(GATES_DIR, `${script}-${jobKey}`));
}

/** Open the gate of a blocked run. Only call once `gateWaiting` is true. */
export async function releaseGate(script: string, jobKey: string | number): Promise<void> {
  await fs.promises.writeFile(path.join(GATES_DIR, `${script}-${jobKey}`), "continue\n");
}

/** Gated runs currently blocked, as `script-jobKey` names. */
export function waitingGates(): string[] {
  return fs.existsSync(GATES_DIR) ? fs.readdirSync(GATES_DIR) : [];
}

/** Bodies used by more than one spec. */
export const scriptBodies = {
  /** Print `bytes` of output in 80-column lines. */
  output: (bytes: number) => `yes 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' | head -c ${bytes}\n`,
  /** Outlive any timeout a test configures; the product must end it. */
  sleepForever: "sleep 86400\n",
  /** Emit one NZBGet directive line. */
  directive: (key: string, value: string) => `printf '[NZB] %s=%s\\n' ${shellQuote(key)} ${shellQuote(value)}\n`,
  /** Rewrite every RSS item title in the feed file a FEED script is given. */
  rewriteFeedTitles: (prefix: string) =>
    `sed -i -e 's#<title>\\([^<]*\\)</title>#<title>${prefix}\\1</title>#g' "$NZBFP_FILENAME"\n`,
};
