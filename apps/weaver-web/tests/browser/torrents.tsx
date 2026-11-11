// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { useState } from "react";
import { createRoot } from "react-dom/client";
import { MemoryRouter } from "react-router";
import { Client, CombinedError, Provider, useSubscription } from "urql";
import { empty, fromValue, make, mergeMap, pipe } from "wonka";
import { TORRENT_UPDATES_SUBSCRIPTION } from "@/graphql/queries";
import { TranslateContext } from "@/lib/context/translate-context";
import en from "@/lib/i18n/locales/en";
import type { JobData } from "@/lib/job-types";
import { ProtocolTag } from "@/next/components/chrome";
import { statusDetail, useStatusLabel } from "@/next/data/status";
import { AddNzbDialog } from "@/next/features/AddNzbDialog";
import { TorrentDetail } from "@/next/features/torrents/TorrentDetail";
import { TorrentRemoveDialog } from "@/next/features/torrents/TorrentRemoveDialog";
import { DownloadRow } from "@/next/pages/downloads/DownloadRow";
import type { SeedingPolicyData, SettingLevel, TorrentChangesData, TorrentData, TorrentFileData, TorrentTrackerData } from "@/lib/torrents";
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
const refusal = (message: string, code: string) =>
  new CombinedError({ graphQLErrors: [{ message, extensions: { code } }] });

// The torrent the files screen follows. Each mutation changes what the daemon will hold; the change is read,
// by a follower new or old, only once the test publishes it, so the page can show nothing but what it was sent.
let held: TorrentData | null = null;
let pending: TorrentData | null = null;
// A follower stays for the page's life: one the page has stopped following answers for an operation it no
// longer reads, which changes nothing it shows.
const followers = new Set<(torrent: TorrentData) => void>();
const publish = () => {
  held = pending;
  for (const follower of followers) follower(held!);
};
Object.assign(window, { torrentsFixture: { sent, publish } });
const holdChange = (name: string, variables: Record<string, unknown>) => {
  if (pending === null) return;
  if (name === "SetTorrentSequential") pending = { ...pending, sequential: variables.sequential as boolean };
  if (name === "SetTorrentFirstLastPieceFirst") pending = { ...pending, firstLastPieceFirst: variables.firstLastPieceFirst as boolean };
  if (name === "SetTorrentFilePriorities") {
    const levels = new Map((variables.priorities as { index: number; priority: TorrentFileData["priority"] }[]).map((entry) => [entry.index, entry.priority]));
    pending = { ...pending, files: pending.files.map((file) => ({ ...file, priority: levels.get(file.index) ?? file.priority })) };
  }
};

const client = new Client({ url: "http://fixture.invalid/graphql", exchanges: [() => (operations) => pipe(operations, mergeMap((operation) => {
  const definition = operation.query.definitions.find((entry) => entry.kind === "OperationDefinition");
  const name = definition?.kind === "OperationDefinition" ? definition.name?.value ?? "" : "";
  const variables = operation.variables as Record<string, unknown>;
  if (operation.kind === "teardown") return empty;
  if (operation.kind === "mutation") {
    sent.push({ name, variables });
    holdChange(name, variables);
  }
  if (name === "TorrentUpdates") {
    // A subscription's answers go on while each says another may follow.
    return make<{ operation: typeof operation; data: { torrentUpdates: TorrentChangesData<TorrentData> }; hasNext: boolean }>((observer) => {
      const follower = (torrent: TorrentData) =>
        observer.next({ operation, data: { torrentUpdates: { fullUpdate: true, torrents: [torrent], removedIds: [] } }, hasNext: true });
      followers.add(follower);
      // A daemon answers once the page is listening, never inside the request.
      queueMicrotask(() => follower(held!));
      return () => followers.delete(follower);
    });
  }
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

/** A release name longer than any row is wide. */
const LONG_NAME = Array.from({ length: 24 }, (_, index) => `Segment${index}`).join(".");

const TORRENT_STATES = ["FETCHING_METADATA", "CHECKING_FILES", "SEEDING", "STALLED"] as const;

/**
 * The queue's rows for each torrent state, labelled by the one status renderer every row uses, and
 * an older job with no protocol, and one whose name is longer than the row. The list is a container, as on the page, so its width lays out the rows.
 */
function Queue() {
  const statusLabel = useStatusLabel();
  const rows: [string, JobData][] = [
    ...TORRENT_STATES.map((state, index): [string, JobData] => [state, job({ id: index + 1, status: state, protocol: "TORRENT" })]),
    ["older", job({ id: 9, name: "older job", displayTitle: "older job", originalTitle: "older job" })],
    ["long", job({ id: 10, name: LONG_NAME, displayTitle: LONG_NAME, originalTitle: LONG_NAME })],
  ];
  return (
    <ul className="@container">
      {rows.map(([label, entry]) => (
        <li key={label} aria-label={label}>
          <DownloadRow
            job={entry}
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

const INHERIT = { state: "INHERIT", value: null } as const;

/** A torrent's own terms: its ratio goal, every other term inherited. */
const ratioOverride = (ratioGoal: number): SeedingPolicyData => ({
  ratioGoal: { state: "SET", value: ratioGoal }, seedTimeGoalMinutes: INHERIT, inactiveGoalMinutes: INHERIT, mode: INHERIT, action: INHERIT,
});

/** A torrent whose ratio goal comes from `level`, and every other term from the global settings. */
const torrent = (id: number, level: SettingLevel, override: SeedingPolicyData | null): TorrentData => ({
  id, name: `torrent ${id}`, infohash: "00".repeat(20), infohashV2: null, state: "SEEDING", detail: null, routeReason: null, category: "movies", private: false,
  forced: false, progress: 1, totalBytes: 2048, haveBytes: 2048, uploadedBytes: 0, downloadedBytes: 2048, ratio: 0.4,
  downloadBps: 0, uploadBps: 0, uploadLimit: null, downloadLimit: null, etaSeconds: null,
  peers: 9, seeds: 3, leechers: 6, maxPeers: 50, swarmSeeds: 12, swarmLeechers: 20, reannounceSeconds: null,
  activeSeconds: 0, downloadAvgBps: 0, uploadAvgBps: 0, wastedBytes: 0, lastSeenCompleteMs: null,
  pieceCount: null, pieceSize: null, piecesHave: null, createdBy: null, createdAtMs: null, comment: null,
  addedAtMs: 0, completedAtMs: null, savePath: null, unpack: false, unpackOverride: null, seedingLeft: null, route: null,
  seedingOverride: override,
  seedingPolicy: {
    ratioGoal: override?.ratioGoal.value ?? (level === "CATEGORY" ? 1.5 : 1),
    seedTimeGoalMinutes: null, inactiveGoalMinutes: null, mode: "ANY", action: "STOP", uploadCap: null,
    levels: {
      ratioGoal: level, seedTimeGoalMinutes: "GLOBAL", inactiveGoalMinutes: "GLOBAL", mode: "GLOBAL", action: "GLOBAL", uploadCap: "GLOBAL",
    },
  },
  trackerRule: null,
  files: [{ index: 0, path: "fixture.mkv", length: 2048, selected: true, priority: "NORMAL", progress: 0.25 }],
  sequential: false,
  firstLastPieceFirst: false,
  trackers: [],
});

/** One torrent's detail per place its seeding policy comes from. */
function Seeding() {
  const sources = [
    torrent(1, "TORRENT", ratioOverride(2.5)),
    torrent(2, "CATEGORY", null),
    torrent(3, "GLOBAL", null),
  ];
  return (
    <>
      {sources.map((entry) => (
        <div key={entry.id} aria-label={entry.seedingPolicy.levels.ratioGoal}>
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

const file = (index: number, path: string, priority: TorrentFileData["priority"]): TorrentFileData => ({
  index, path, length: 1024 * (index + 1), selected: priority !== "IGNORED", priority, progress: 0,
});

/** One torrent's detail as the job page shows it: read from its subscription, and read again on each change. */
function Files() {
  const [{ data }, follow] = useSubscription<{ torrentUpdates: TorrentChangesData<TorrentData> }>({
    query: TORRENT_UPDATES_SUBSCRIPTION,
    variables: { id: 4 },
  });
  const live = data?.torrentUpdates.torrents[0];
  return live ? (
    <TorrentDetail torrent={live} onChanged={() => follow()} onReport={(message) => { status.textContent = message; }} />
  ) : null;
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
  if (screen === "trackers" || screen === "privateTrackers") {
    const tracker = (url: string, source: TorrentTrackerData["source"]): TorrentTrackerData => ({
      tier: 0, url, source, status: "WORKING", updating: false, message: null, seeders: 1, leechers: 0, peers: 1,
    });
    const named = {
      ...torrent(5, "GLOBAL", null),
      private: screen === "privateTrackers",
      trackers: [tracker("http://a.test/announce", "TORRENT"), tracker("http://b.test/announce", "OPERATOR")],
    };
    return <TorrentDetail torrent={named} onChanged={() => {}} onReport={(message) => { status.textContent = message; }} />;
  }
  if (screen === "files") {
    held ??= {
      ...torrent(4, "GLOBAL", null),
      state: "DOWNLOADING",
      files: [file(0, "set/a.mkv", "NORMAL"), file(1, "set/b.nfo", "NORMAL"), file(2, "set/c.srt", "IGNORED")],
    };
    pending ??= held;
    return <Files />;
  }
  return <AddNzbDialog open onClose={() => { status.textContent = "closed"; }} />;
}

createRoot(document.getElementById("root")!).render(
  <Provider value={client}><TranslateContext.Provider value={{
    t, uiLanguage: "eng", selectedLanguage: { code: "eng", label: "English" }, setLanguagePreference: () => {},
  }}><MemoryRouter><main className="mx-auto max-w-[1000px] p-8"><Fixture /></main></MemoryRouter></TranslateContext.Provider></Provider>,
);
