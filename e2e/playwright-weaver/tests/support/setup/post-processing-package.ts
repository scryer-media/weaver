import fs from "node:fs";
import path from "node:path";

/**
 * Post-processing fixtures.
 *
 * There is no seeding backdoor any more: a script is a file in
 * `data_dir/scripts`, so the fixtures are exactly that. The flow enables
 * execution through the UI, lists them, and lets a real job run them.
 */
const SCRIPTS_DIR = "/weaver-data/scripts";
const WEAVER_OUTPUTS_DIR = "/data/complete";
const PLAYWRIGHT_OUTPUTS_DIR = "/weaver-downloads";

export const POST_PROCESSING_NOTIFY_SCRIPT = "e2e-notify.sh";
export const POST_PROCESSING_FAILING_SCRIPT = "e2e-failing.sh";
export const POST_PROCESSING_NZBGET_PACKAGE = "e2e-nzbget-package";
export const POST_PROCESSING_NZBGET_DISPLAY_NAME = "E2E NZBGet Package";
export const POST_PROCESSING_BROKEN_PACKAGE = "e2e-broken-package";
export const POST_PROCESSING_SECRET = "weaver-e2e-secret-must-never-appear";
/** Marker each script drops into the job's output directory. */
export const POST_PROCESSING_MARKER = "e2e-post-processing.txt";

function writeScript(name: string, body: string): void {
  const target = path.join(SCRIPTS_DIR, name);
  fs.rmSync(target, { recursive: true, force: true });
  fs.mkdirSync(SCRIPTS_DIR, { recursive: true });
  fs.writeFileSync(target, body, { mode: 0o755 });
}

/**
 * Seed every fixture the flow needs:
 *
 * - a bare SABnzbd script that succeeds and leaves evidence in the output dir,
 * - a bare script that exits nonzero, which SABnzbd records as a warning,
 * - an NZBGet manifest package with a secret option and exit 93,
 * - a package whose manifest is unparseable, which must surface as a problem.
 */
export function seedPostProcessingScripts(): void {
  writeScript(
    POST_PROCESSING_NOTIFY_SCRIPT,
    `#!/bin/sh
printf 'notify ran for %s\\n' "$SAB_FINAL_NAME"
printf 'notify\\n' >> "$SAB_COMPLETE_DIR/${POST_PROCESSING_MARKER}"
`,
  );
  writeScript(
    POST_PROCESSING_FAILING_SCRIPT,
    `#!/bin/sh
printf 'failing script refusing to process %s\\n' "$SAB_FINAL_NAME"
printf 'failing\\n' >> "$SAB_COMPLETE_DIR/${POST_PROCESSING_MARKER}"
exit 3
`,
  );

  const nzbgetPackage = path.join(SCRIPTS_DIR, POST_PROCESSING_NZBGET_PACKAGE);
  fs.rmSync(nzbgetPackage, { recursive: true, force: true });
  fs.mkdirSync(nzbgetPackage, { recursive: true });
  fs.writeFileSync(
    path.join(nzbgetPackage, "manifest.json"),
    `${JSON.stringify(
      {
        main: "run.sh",
        name: POST_PROCESSING_NZBGET_PACKAGE,
        kind: "POST-PROCESSING",
        displayName: POST_PROCESSING_NZBGET_DISPLAY_NAME,
        version: "1.0.0",
        author: "Weaver e2e",
        homepage: "https://example.invalid",
        license: "GNU",
        about: "Records its NZBGet environment for the release gate.",
        description: ["Writes NZBPO_* values into the job output directory."],
        requirements: [],
        queueEvents: "",
        taskTime: "",
        sections: [],
        commands: [],
        options: [
          {
            name: "Label",
            displayName: "Label",
            value: "default-label",
            description: ["Text written next to the job name."],
            select: [],
          },
          {
            name: "Token",
            displayName: "Token",
            value: "",
            description: ["Stored through the settings encryption envelope."],
            select: [],
            secret: true,
          },
        ],
      },
      null,
      2,
    )}\n`,
  );
  fs.writeFileSync(
    path.join(nzbgetPackage, "run.sh"),
    `#!/bin/sh
printf 'nzbget package label=%s token=%s\\n' "$NZBPO_Label" "$NZBPO_Token"
printf 'nzbget %s\\n' "$NZBPO_Label" >> "$NZBPP_DIRECTORY/${POST_PROCESSING_MARKER}"
exit 93
`,
    { mode: 0o755 },
  );

  const broken = path.join(SCRIPTS_DIR, POST_PROCESSING_BROKEN_PACKAGE);
  fs.rmSync(broken, { recursive: true, force: true });
  fs.mkdirSync(broken, { recursive: true });
  fs.writeFileSync(path.join(broken, "manifest.json"), "{ this is not json\n");
}

/** Remove every fixture, so a rerun of the flow starts from a clean directory. */
export function removePostProcessingScripts(): void {
  for (const name of [
    POST_PROCESSING_NOTIFY_SCRIPT,
    POST_PROCESSING_FAILING_SCRIPT,
    POST_PROCESSING_NZBGET_PACKAGE,
    POST_PROCESSING_BROKEN_PACKAGE,
  ]) {
    fs.rmSync(path.join(SCRIPTS_DIR, name), { recursive: true, force: true });
  }
}

/** Contents of a completed job's marker file on Playwright's shared-volume mount. */
export function postProcessingMarker(outputDir: string): string {
  const relative = path.relative(WEAVER_OUTPUTS_DIR, outputDir);
  if (
    relative === ""
    || relative === ".."
    || relative.startsWith(`..${path.sep}`)
    || path.isAbsolute(relative)
  ) {
    throw new Error(`unexpected Weaver completed-job output directory: ${outputDir}`);
  }
  const marker = path.join(PLAYWRIGHT_OUTPUTS_DIR, relative, POST_PROCESSING_MARKER);
  return fs.existsSync(marker) ? fs.readFileSync(marker, "utf8") : "";
}
