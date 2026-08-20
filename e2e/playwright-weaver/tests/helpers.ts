import net from "node:net";
import { expect, test as base, type APIRequestContext, type Page } from "@playwright/test";

type ExpectedHttpError = {
  method: string;
  pathname: string;
  status: number;
  /// Minimum number of matching responses that must be observed.
  count: number;
  /// Upper bound of matching responses to tolerate (defaults to `count`).
  /// Use when the exact count is incidental — e.g. how many in-flight
  /// polling queries happen to 401 between enabling login and the client
  /// redirecting is a timing artifact, not a product contract.
  maxCount?: number;
};

type RecordedHttpError = Omit<ExpectedHttpError, "count"> & {
  url: string;
};

type WeaverFixtures = {
  cleanPage: Page;
};

const expectedHttpErrorsByPage = new WeakMap<Page, ExpectedHttpError[]>();

export function expectHttpErrors(
  page: Page,
  expected: Omit<ExpectedHttpError, "count"> & { count?: number },
): void {
  const expectations = expectedHttpErrorsByPage.get(page);
  if (!expectations) {
    throw new Error("expectHttpErrors requires the cleanPage fixture");
  }
  expectations.push({
    ...expected,
    method: expected.method.toUpperCase(),
    count: expected.count ?? 1,
    maxCount: expected.maxCount ?? expected.count ?? 1,
  });
}

export const test = base.extend<WeaverFixtures>({
  cleanPage: async ({ page }, use) => {
    const failures: string[] = [];
    const httpErrors: RecordedHttpError[] = [];
    const expectedHttpErrors: ExpectedHttpError[] = [];
    expectedHttpErrorsByPage.set(page, expectedHttpErrors);
    const responseInspections: Promise<void>[] = [];
    page.on("console", (message) => {
      if (message.type() !== "error") return;
      const browserHttpDiagnostic = /^Failed to load resource: the server responded with a status of \d+/.test(
        message.text(),
      );
      if (!browserHttpDiagnostic) failures.push(`console: ${message.text()}`);
    });
    page.on("pageerror", (error) => failures.push(`pageerror: ${error.message}`));
    page.on("requestfailed", (request) => {
      const errorText = request.failure()?.errorText ?? "unknown";
      if (errorText !== "net::ERR_ABORTED") {
        failures.push(`requestfailed: ${request.method()} ${request.url()} ${errorText}`);
      }
    });
    page.on("response", (response) => {
      responseInspections.push((async () => {
        const request = response.request();
        if (response.status() >= 400) {
          httpErrors.push({
            method: request.method().toUpperCase(),
            pathname: new URL(response.url()).pathname,
            status: response.status(),
            url: response.url(),
          });
        }
        if (
          response.status() >= 400
          ||
          request.method() !== "POST"
          || !response.url().includes("/graphql")
          || !response.headers()["content-type"]?.includes("application/json")
        ) {
          return;
        }
        const payload = await response.json().catch(() => undefined) as
          | { errors?: Array<{ message?: string }> }
          | undefined;
        const messages = (payload?.errors ?? [])
          .map(({ message }) => message ?? "unknown GraphQL error");
        const unexpected = messages.filter(
          (message) => !/source run has no failed or interrupted attempt/i.test(message),
        );
        if (unexpected.length > 0) {
          failures.push(`graphql: ${unexpected.join(" | ")}`);
        }
      })());
    });
    await use(page);
    await Promise.allSettled(responseInspections);
    const consumed = new Set<number>();
    for (const expected of expectedHttpErrors) {
      const matches = httpErrors
        .map((recorded, index) => ({ recorded, index }))
        .filter(({ recorded, index }) =>
          !consumed.has(index)
          && recorded.method === expected.method
          && recorded.pathname === expected.pathname
          && recorded.status === expected.status
        )
        .slice(0, expected.maxCount ?? expected.count);
      if (matches.length < expected.count) {
        failures.push(
          `expected-http-response: wanted ${expected.count}x ${expected.status} `
          + `${expected.method} ${expected.pathname}, observed ${matches.length}`,
        );
      }
      for (const { index } of matches) consumed.add(index);
    }
    for (const [index, recorded] of httpErrors.entries()) {
      if (!consumed.has(index)) {
        failures.push(
          `http-response: ${recorded.status} ${recorded.method} ${recorded.url}`,
        );
      }
    }
    expectedHttpErrorsByPage.delete(page);
    expect(failures, failures.join("\n")).toEqual([]);
  },
});

export { expect };

const initializedApiSessions = new WeakSet<APIRequestContext>();

const configuredBaseUrl = new URL(
  process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090/",
);
const configuredBasePath = configuredBaseUrl.pathname.replace(/\/+$/, "");

export function weaverRoute(path: string): string {
  const normalizedPath = path === "/" ? "" : `/${path.replace(/^\/+/, "")}`;
  return `${configuredBasePath}${normalizedPath}` || "/";
}

async function ensureApiSession(request: APIRequestContext): Promise<void> {
  if (initializedApiSessions.has(request)) return;
  const response = await request.get(weaverRoute("/"));
  const body = await response.text();
  expect(response.ok(), body).toBeTruthy();
  initializedApiSessions.add(request);
}

export async function graphql<T>(
  request: APIRequestContext,
  query: string,
  variables: Record<string, unknown> = {},
): Promise<T> {
  await ensureApiSession(request);
  const response = await request.post(weaverRoute("/graphql"), { data: { query, variables } });
  const body = await response.text();
  expect(response.ok(), body).toBeTruthy();
  const payload = JSON.parse(body);
  expect(payload.errors ?? [], JSON.stringify(payload.errors ?? [])).toEqual([]);
  return payload.data as T;
}

export async function metrics(request: APIRequestContext): Promise<string> {
  await ensureApiSession(request);
  const response = await request.get(weaverRoute("/metrics"));
  const body = await response.text();
  expect(response.ok(), body).toBeTruthy();
  return body;
}

export function metricValue(
  body: string,
  metric: string,
  labels: Record<string, string> = {},
): number | undefined {
  for (const line of body.split("\n")) {
    if (!line.startsWith(metric) || line.startsWith(`${metric}_`)) continue;
    const match = /^([^{\s]+)(?:\{([^}]*)\})?\s+(-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?|[+-]Inf|NaN)$/.exec(
      line.trim(),
    );
    if (!match || match[1] !== metric) continue;
    const sampleLabels = Object.fromEntries(
      [...(match[2] ?? "").matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g)].map((entry) => [
        entry[1],
        entry[2].replaceAll('\\"', '"').replaceAll("\\\\", "\\"),
      ]),
    );
    if (Object.entries(labels).every(([key, value]) => sampleLabels[key] === value)) {
      return Number(match[3]);
    }
  }
  return undefined;
}

export async function expectSetting(
  request: APIRequestContext,
  field: string,
  expected: unknown,
) {
  const data = await graphql<{ settings: Record<string, unknown> }>(
    request,
    `query WeaverE2ESetting { settings { ${field} } }`,
  );
  expect(data.settings[field]).toEqual(expected);
}

const serverInput = {
  host: "nntp",
  port: 119,
  tls: false,
  username: "e2e-user",
  password: "e2e-pass",
  connections: 4,
  active: true,
  priority: 0,
  backfill: false,
  retentionDays: 0,
};

export async function configuredServer(
  request: APIRequestContext,
  host: string,
): Promise<{ id: number; host: string; port: number }> {
  const data = await graphql<{ servers: Array<{ id: number; host: string; port: number }> }>(
    request,
    "query WeaverE2EServers { servers { id host port } }",
  );
  const server = data.servers.find((candidate) => candidate.host === host);
  expect(server, `configured Weaver server ${host}`).toBeTruthy();
  return server!;
}

export async function updateConfiguredServer(
  request: APIRequestContext,
  host: string,
  overrides: Record<string, unknown>,
): Promise<{ id: number; host: string; port: number; connections: number; maxDownloadSpeed: number }> {
  const server = await configuredServer(request, host);
  const input = {
    ...serverInput,
    host: server.host,
    port: server.port,
    ...overrides,
  };
  const data = await graphql<{
    updateServer: {
      id: number;
      host: string;
      port: number;
      connections: number;
      maxDownloadSpeed: number;
    };
  }>(
    request,
    `mutation WeaverE2EUpdateServer($id: Int!, $input: ServerInput!) {
      updateServer(id: $id, input: $input) {
        id host port connections maxDownloadSpeed
      }
    }`,
    { id: server.id, input },
  );
  return data.updateServer;
}

export async function setNntpChaos(
  config: string,
  host = "nntp",
  port = 119,
): Promise<void> {
  await withNntpConnection(host, port, async (session) => {
    expect(await session.command("AUTHINFO USER e2e-user")).toMatch(/^381 /);
    expect(await session.command("AUTHINFO PASS e2e-pass")).toMatch(/^281 /);
    expect(await session.command(`CHAOS ${config}`)).toMatch(/^290 /);
  });
}

export type NntpConnectionMetrics = {
  attempted: number;
  accepted: number;
  rejected: number;
  active: number;
  peak_active: number;
  configured_limit: number;
};

/// Read the server's connection counters.
///
/// Retried, because this probe competes for the very resource it measures: the
/// connection cap counts every client, this one included. Callers reach it
/// just after telling Weaver to stand down, and Weaver's sockets close
/// asynchronously — so a greeting of `502 Too many connections` means the
/// drain is still in flight, not that the product misbehaved. The last failure
/// is rethrown, so a server that never frees a slot still fails the test.
export async function nntpConnectionMetrics(
  host = "nntp",
  port = 119,
): Promise<NntpConnectionMetrics> {
  const deadline = Date.now() + 30_000;
  for (let attempt = 0; ; attempt += 1) {
    try {
      let metrics: NntpConnectionMetrics | undefined;
      await withNntpConnection(host, port, async (session) => {
        expect(await session.command("AUTHINFO USER e2e-user")).toMatch(/^381 /);
        expect(await session.command("AUTHINFO PASS e2e-pass")).toMatch(/^281 /);
        const response = await session.command("METRICS CONNECTIONS");
        expect(response).toMatch(/^290 /);
        metrics = JSON.parse(response.replace(/^290\s+/, "")) as NntpConnectionMetrics;
      });
      return metrics!;
    } catch (error) {
      if (Date.now() >= deadline) {
        throw error;
      }
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(1_000, 100 * (attempt + 1))),
      );
    }
  }
}

export type NntpBodyMetrics = {
  body_counts: Record<string, number>;
  body_bytes: number;
  body_transfers: number;
  body_first_unix_nano: number;
  body_last_unix_nano: number;
};

export async function resetNntpMetrics(
  host = "nntp",
  port = 119,
): Promise<void> {
  await withNntpConnection(host, port, async (session) => {
    expect(await session.command("AUTHINFO USER e2e-user")).toMatch(/^381 /);
    expect(await session.command("AUTHINFO PASS e2e-pass")).toMatch(/^281 /);
    expect(await session.command("METRICS RESET")).toMatch(/^290 /);
  });
}

export async function nntpBodyMetrics(
  prefix = "",
  host = "nntp",
  port = 119,
): Promise<NntpBodyMetrics> {
  let bodyMetrics: NntpBodyMetrics | undefined;
  await withNntpConnection(host, port, async (session) => {
    expect(await session.command("AUTHINFO USER e2e-user")).toMatch(/^381 /);
    expect(await session.command("AUTHINFO PASS e2e-pass")).toMatch(/^281 /);
    const response = await session.command(`METRICS BODY${prefix ? ` ${prefix}` : ""}`);
    expect(response).toMatch(/^290 /);
    bodyMetrics = JSON.parse(response.replace(/^290\s+/, "")) as NntpBodyMetrics;
  });
  return bodyMetrics!;
}

export async function postProbeArticle(
  messageId: string,
  bodyBytes: number,
  host = "nntp",
  port = 119,
): Promise<void> {
  const normalizedId = messageId.replace(/^<|>$/g, "");
  await postYencProbeArticle(
    normalizedId,
    bodyBytes,
    `${normalizedId.replace(/[^A-Za-z0-9._-]/g, "_")}.bin`,
    undefined,
    host,
    port,
  );
}

export async function postMultipartProbeArticle(
  messageId: string,
  bodyBytes: number,
  part: {
    filename: string;
    number: number;
    total: number;
    begin: number;
    end: number;
    totalBytes: number;
  },
  host = "nntp",
  port = 119,
): Promise<void> {
  await postYencProbeArticle(
    messageId.replace(/^<|>$/g, ""),
    bodyBytes,
    part.filename,
    part,
    host,
    port,
  );
}

async function postYencProbeArticle(
  normalizedId: string,
  bodyBytes: number,
  filename: string,
  part: {
    number: number;
    total: number;
    begin: number;
    end: number;
    totalBytes: number;
  } | undefined,
  host: string,
  port: number,
): Promise<void> {
  const decodedBytes = Math.max(1, bodyBytes);
  const lineLength = 128;
  const encodedLines = Array.from(
    { length: Math.ceil(decodedBytes / lineLength) },
    (_, index) => "*".repeat(Math.min(lineLength, decodedBytes - index * lineLength)),
  );
  const crc32 = zeroBytesCrc32(decodedBytes);
  const ybegin = part
    ? `=ybegin part=${part.number} total=${part.total} line=${lineLength} size=${part.totalBytes} name=${filename}`
    : `=ybegin line=${lineLength} size=${decodedBytes} name=${filename}`;
  const yend = part
    ? `=yend size=${decodedBytes} part=${part.number} pcrc32=${crc32}${
        part.number === part.total ? ` crc32=${zeroBytesCrc32(part.totalBytes)}` : ""
      }`
    : `=yend size=${decodedBytes} crc32=${crc32}`;
  const payload = [
    `Message-ID: <${normalizedId}>`,
    "Newsgroups: alt.binaries.test",
    `Subject: Weaver e2e probe ${normalizedId}`,
    "",
    ybegin,
    ...(part ? [`=ypart begin=${part.begin} end=${part.end}`] : []),
    ...encodedLines,
    yend,
  ].join("\r\n");
  await withNntpConnection(host, port, async (session) => {
    expect(await session.command("AUTHINFO USER e2e-user")).toMatch(/^381 /);
    expect(await session.command("AUTHINFO PASS e2e-pass")).toMatch(/^281 /);
    expect(await session.command("POST")).toMatch(/^340 /);
    expect(await session.command(`${payload}\r\n.`)).toMatch(/^240 /);
  });
}

const zeroBodyCrc32Cache = new Map<number, string>();
const crc32Table = Array.from({ length: 256 }, (_, byte) => {
  let value = byte;
  for (let bit = 0; bit < 8; bit += 1) {
    value = (value & 1) === 1 ? (value >>> 1) ^ 0xedb88320 : value >>> 1;
  }
  return value >>> 0;
});

function zeroBytesCrc32(length: number): string {
  const cached = zeroBodyCrc32Cache.get(length);
  if (cached) return cached;

  let crc = 0xffffffff;
  for (let offset = 0; offset < length; offset += 1) {
    crc = (crc >>> 8) ^ crc32Table[crc & 0xff]!;
  }
  const value = ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
  zeroBodyCrc32Cache.set(length, value);
  return value;
}

export async function submitProbeNzb(
  request: APIRequestContext,
  name: string,
  articles: Array<{ messageId: string; bytes: number }>,
  extraInput: Record<string, unknown> = {},
  fileLayout: "separate-files" | "single-multipart-file" = "separate-files",
): Promise<{
  accepted: boolean;
  status: string;
  jobId: number | null;
  errorCode: string | null;
  duplicateDecision: { action: string; forceBypassed: boolean } | null;
}> {
  const files = fileLayout === "single-multipart-file"
    ? `
        <file poster="weaver-e2e" date="1700000000" subject="${name}.bin">
          <groups><group>alt.binaries.test</group></groups>
          <segments>${articles
            .map(
              ({ messageId, bytes }, index) =>
                `<segment bytes="${bytes}" number="${index + 1}">${messageId.replace(/^<|>$/g, "")}</segment>`,
            )
            .join("")}</segments>
        </file>`
    : articles
      .map(
        ({ messageId, bytes }, index) => `
        <file poster="weaver-e2e" date="1700000000" subject="${name}.${index + 1}.bin">
          <groups><group>alt.binaries.test</group></groups>
          <segments><segment bytes="${bytes}" number="1">${messageId.replace(/^<|>$/g, "")}</segment></segments>
        </file>`,
      )
      .join("");
  const nzb = `<?xml version="1.0" encoding="UTF-8"?>
    <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">${files}</nzb>`;
  const data = await graphql<{
    submitNzb: {
      accepted: boolean;
      status: string;
      jobId: number | null;
      errorCode: string | null;
      duplicateDecision: { action: string; forceBypassed: boolean } | null;
    };
  }>(
    request,
    `mutation WeaverE2ESubmitProbe($input: SubmitNzbInput!) {
      submitNzb(input: $input) {
        accepted status jobId errorCode
        duplicateDecision { action forceBypassed }
      }
    }`,
    {
      input: {
        nzbBase64: Buffer.from(nzb).toString("base64"),
        filename: `${name}.nzb`,
        ...extraInput,
      },
    },
  );
  return data.submitNzb;
}

type NntpSession = {
  command(command: string): Promise<string>;
};

async function withNntpConnection(
  host: string,
  port: number,
  use: (session: NntpSession) => Promise<void>,
): Promise<void> {
  const socket = net.createConnection({ host, port });
  socket.setTimeout(15_000);
  let buffered = "";
  const lines: string[] = [];
  const waiters: Array<(line: string) => void> = [];
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    buffered += chunk;
    for (;;) {
      const newline = buffered.indexOf("\n");
      if (newline < 0) break;
      const line = buffered.slice(0, newline).replace(/\r$/, "");
      buffered = buffered.slice(newline + 1);
      const waiter = waiters.shift();
      if (waiter) waiter(line);
      else lines.push(line);
    }
  });
  const nextLine = () =>
    new Promise<string>((resolve, reject) => {
      const line = lines.shift();
      if (line !== undefined) {
        resolve(line);
        return;
      }
      const onError = (error: Error) => reject(error);
      socket.once("error", onError);
      waiters.push((value) => {
        socket.off("error", onError);
        resolve(value);
      });
    });
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  expect(await nextLine()).toMatch(/^20[01] /);
  const session: NntpSession = {
    command: async (command) => {
      socket.write(`${command}\r\n`);
      return nextLine();
    },
  };
  try {
    await use(session);
  } finally {
    socket.write("QUIT\r\n");
    socket.end();
  }
}
