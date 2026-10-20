// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, CombinedError, Provider } from "urql";
import { fromValue, mergeMap, pipe } from "wonka";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import type { JobData } from "@/lib/job-types";
import { ProtocolTag } from "@/next/components/chrome";
import { statusDetail, useStatusLabel } from "@/next/data/status";
import { AddNzbDialog } from "@/next/features/AddNzbDialog";
import { TorrentDetail } from "@/next/features/torrents/TorrentDetail";
import { TorrentRemoveDialog } from "@/next/features/torrents/TorrentRemoveDialog";
import { DownloadRow } from "@/next/pages/downloads/DownloadRow";
import type { SeedingPolicyData, SeedingSource, TorrentData } from "@/lib/torrents";
import { nextEn } from "@/next/i18n/en";
import "@/next/fonts.css";
import "@/next/theme.css";

const query = new URLSearchParams(location.search);
const screen = query.get("screen") ?? "tag";
const dictionary = { ...en, ...nextEn } as Record<string, string>;
const t = (key: string, values?: Record<string, unknown>) =>
  Object.entries(values ?? {}).reduce((text, [name, value]) => text.replaceAll(`{{${name}}}`, String(value)), dictionary[key] ?? key);
const status = document.getElementById("status")!;

// Every mutation the page sends, and the refusals the daemon gives: a duplicate magnet, a file it cannot read, and a remove whose goal is unmet.
const sent: { name: string; variables: Record<string, unknown> }[] = [];
Object.assign(window, { torrentsFixture: { sent } });
const refusal = (message: string, code: string) =>
  new CombinedError({ graphQLErrors: [{ message, extensions: { code } }] });

const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  const variables = operation.variables as Record<string, unknown>;
  if (operation.kind === "mutation") sent.push({ name, variables });
  if (name === "RemoveTorrent") {
    return fromValue(
      variables.confirmUnmetGoal
        ? { operation, data: { removeTorrent: true } }
        : { operation, error: refusal("Ratio 0.40 of 1.00 goal.", "SEEDING_GOAL_UNMET") },
    );
  }
  const answer = name === "Categories" ? { categories: [{ id: 1, name: "movies", destDir: null, aliases: "", seeding: null, torrentRoute: false }] } : {};
  return fromValue({ operation, data: answer });
}))] });

// The Add dialog speaks to the daemon with fetch, a file as multipart. The daemon reads what each one is:
// a bencoded dictionary is a torrent, an NZB document is staged, anything else is refused.
const answer = (data: unknown) => new Response(JSON.stringify(data), { headers: { "Content-Type": "application/json" } });
const passThrough = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const url = String(input instanceof Request ? input.url : input);
  if (url.endsWith("/api/auth/csrf")) return answer({ csrfToken: "fixture" });
  if (!url.endsWith("/graphql")) return passThrough(input, init);
  const body = init?.body;
  const operations = JSON.parse(body instanceof FormData ? String(body.get("operations")) : String(body));
  const file = body instanceof FormData ? (body.get("0") as File | null) : null;
  const content = file ? await file.text() : "";
  const name = /mutation (\w+)/.exec(operations.query)?.[1] ?? "";
  sent.push({ name, variables: file ? { ...operations.variables, file: file.name } : operations.variables });
  if (name === "StageNzbUpload") {
    if (/^d\d/.test(content)) return answer({ data: { stageNzbUpload: { kind: "TORRENT", accepted: false } } });
    return answer({
      data: {
        stageNzbUpload: content.includes("<nzb")
          ? { kind: "NZB", accepted: true, stagedUploadId: `staged-${sent.length}`, displayName: file?.name }
          : { kind: "NZB", accepted: false, error: "not an NZB" },
      },
    });
  }
  if (name === "Submit") {
    const text = (operations.variables.input as { text?: string | null }).text ?? "";
    return text.includes("duplicate")
      ? answer({ errors: [{ message: "This torrent is already in the queue.", extensions: { code: "DUPLICATE" } }] })
      : answer({ data: { submit: { accepted: true, status: "ACCEPTED", message: null } } });
  }
  return answer({ data: {} });
};

const job = (rest: Partial<JobData>): JobData => ({
  id: 1, name: "fixture", displayTitle: "fixture", originalTitle: "fixture", parsedRelease: {} as JobData["parsedRelease"],
  status: "DOWNLOADING", progress: 0, totalBytes: 0, downloadedBytes: 0, optionalRecoveryBytes: 0, optionalRecoveryDownloadedBytes: 0,
  phaseProgress: [], failedBytes: 0, health: 1000, hasPassword: false, category: null, metadata: [], ...rest,
});

function Remove() {
  const [open, setOpen] = useState(true);
  return (
    <TorrentRemoveDialog
      id={open ? 5 : null}
      name="fixture torrent"
      onRemoved={(id) => { setOpen(false); status.textContent = `removed ${id}`; }}
      onFailed={(message) => { setOpen(false); status.textContent = `failed ${message}`; }}
      onDismiss={() => { setOpen(false); status.textContent = "dismissed"; }}
    />
  );
}

const TORRENT_STATES = ["FETCHING_METADATA", "CHECKING_FILES", "SEEDING", "STALLED"] as const;

/** The queue's rows for each torrent state, labelled by the one status renderer every row uses. */
function Queue() {
  const statusLabel = useStatusLabel();
  return (
    <ul>
      {TORRENT_STATES.map((state, index) => (
        <li key={state} aria-label={state}>
          <DownloadRow
            job={job({ id: index + 1, status: state, protocol: "TORRENT" })}
            selected={false}
            onSelect={() => {}}
            picked={false}
            onPick={() => {}}
            statusLabel={statusLabel}
            held={null}
            hold={null}
            onPause={() => {}}
            onCancel={() => {}}
          />
        </li>
      ))}
    </ul>
  );
}

const policy = (rest: Partial<SeedingPolicyData>): SeedingPolicyData => ({
  ratioGoal: null, seedTimeGoalMinutes: null, inactiveGoalMinutes: null, mode: "ANY", action: "STOP", maxUploadBps: null, ...rest,
});

const torrent = (id: number, source: SeedingSource, override: SeedingPolicyData | null): TorrentData => ({
  id, name: `torrent ${id}`, infohash: "00".repeat(20), state: "SEEDING", detail: null, category: "movies", private: false,
  forced: false, progress: 1, totalBytes: 2048, haveBytes: 2048, uploadedBytes: 0, downloadedBytes: 2048, ratio: 0.4,
  downloadBps: 0, uploadBps: 0, peers: 9, seeds: 3, leechers: 6, seedingLeft: null, route: null,
  seedingOverride: override,
  seedingPolicy: override ?? policy({ ratioGoal: source === "CATEGORY" ? 1.5 : 1 }),
  seedingSource: source,
  seedingRule: null,
  files: [{ index: 0, path: "fixture.mkv", length: 2048, selected: true, progress: 0.25 }],
  trackers: [],
});

/** One torrent's detail per place its seeding policy comes from. */
function Seeding() {
  const sources = [
    torrent(1, "TORRENT", policy({ ratioGoal: 2.5 })),
    torrent(2, "CATEGORY", null),
    torrent(3, "GLOBAL", null),
  ];
  return (
    <>
      {sources.map((entry) => (
        <div key={entry.id} aria-label={entry.seedingSource}>
          <TorrentDetail
            torrent={entry}
            onChanged={() => { status.textContent = `changed ${entry.id}`; }}
            onReport={(message) => { status.textContent = message; }}
          />
        </div>
      ))}
    </>
  );
}

function Fixture() {
  if (screen === "tag") {
    return (
      <ul>
        <li aria-label="torrent row"><ProtocolTag protocol="TORRENT" />torrent job</li>
        <li aria-label="nzb row"><ProtocolTag protocol="NZB" />nzb job</li>
        <li aria-label="older row"><ProtocolTag protocol={undefined} />older job</li>
      </ul>
    );
  }
  if (screen === "detail") {
    const details = [
      job({ downloadWaitReason: "Not seeding: route down" }),
      job({ downloadWaitReason: "PROPAGATION_DELAY" }),
      job({ downloadWaitReason: "SOME_NEW_CODE" }),
    ].map((entry) => statusDetail(t, entry, null));
    return <ul aria-label="details">{details.map((detail, index) => <li key={index}>{detail}</li>)}</ul>;
  }
  if (screen === "remove") return <Remove />;
  if (screen === "queue") return <Queue />;
  if (screen === "seeding") return <Seeding />;
  return <AddNzbDialog open onClose={() => { status.textContent = "closed"; }} />;
}

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t, uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><main className="mx-auto max-w-[1000px] p-8"><Fixture /></main></MemoryRouter></TranslateContext.Provider></Provider>,
);
