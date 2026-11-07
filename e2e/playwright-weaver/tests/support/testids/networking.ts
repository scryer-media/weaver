// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A copy of the web app's apps/weaver-web/src/next/testids/networking.ts, between the
// markers. The package does not import across its boundary; the
// testids-pin check keeps the two equal.

// testids:begin
/** The networking pages. */
export const networkIds = {
  // Networking -> Speed test: what to run.
  speedConsumer: "speed-consumer",
  speedKind: "speed-kind",
  speedProvider: "speed-provider",
  speedServer: "speed-server",
  speedCustomUrl: "speed-custom-url",
  speedNzb: "speed-nzb",
  speedNzbUrl: "speed-nzb-url",
  speedConnections: "speed-connections",
  speedMaxDuration: "speed-max-duration",
  speedRun: "speed-run",
  speedRefusal: "speed-refusal",

  // A run as it goes, keyed by the phase word the daemon reports.
  speedProgress: "speed-progress",
  speedPhase: (phase: string) => `speed-phase-${phase}`,
  speedCancel: "speed-cancel",

  // A run as it ended, keyed by the run's status.
  speedResult: "speed-result",
  speedOutcome: (status: string) => `speed-outcome-${status}`,
  speedRunStatus: (status: string) => `speed-run-status-${status}`,
  speedRunFinished: /^speed-run-status-(COMPLETED|CANCELLED|FAILED)$/,

  // Comparing two runs.
  speedCompare: "speed-compare",
  speedCompareRun: (runId: number) => `speed-compare-run-${runId}`,

  // The small tables inside a run, keyed by the table and the column's own key.
  speedTable: (table: string) => `speed-table-${table}`,
  speedTableRow: (table: string) => `speed-table-${table}-row`,
  speedTableCell: (table: string, column: string) => `speed-table-${table}-${column}`,

  // A run's row in the runs table.
  /** The kind cell, by what the run measured with (bandwidth-provider, bandwidth-custom, usenet-sab, usenet-file, usenet-url). */
  speedRunKind: (kind: string) => `speed-run-kind-${kind}`,
  /** One thing a run had down, by its key (leg-<position>, rung-<leg>-<rung>, egress-<id>, proxy-<id>; zero-based). */
  speedRunDown: (part: string) => `speed-run-down-${part}`,

  // The route flow: the diagram, and each box's state keyed by what it stands for.
  flowDiagram: "flow-diagram",
  flowEgress: (egressId: number, state: string) => `flow-egress-${egressId}-${state}`,
  flowLeg: (position: number, state: string) => `flow-leg-${position}-${state}`,
  flowRung: (leg: number, rung: number, state: string) => `flow-rung-${leg}-${rung}-${state}`,
  flowDirect: (leg: number, state: string) => `flow-direct-${leg}-${state}`,
  /** Any egress or rung box drawn down. */
  flowBoxDown: /^flow-(egress|rung)-.+-DOWN$/,

  // The Up/Down switch on a box, keyed by what it takes down, and each of its sides.
  legSwitch: (position: number) => `flow-switch-leg-${position}`,
  rungSwitch: (leg: number, rung: number) => `flow-switch-rung-${leg}-${rung}`,
  proxySwitch: (proxyId: number) => `flow-switch-proxy-${proxyId}`,
  egressSwitch: (egressId: number) => `flow-switch-egress-${egressId}`,
  switchSide: (switchId: string, side: "up" | "down") => `${switchId}-${side}`,
} as const;
// testids:end
