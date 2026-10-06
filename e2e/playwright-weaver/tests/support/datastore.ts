import crypto from "node:crypto";
import net from "node:net";
import tls from "node:tls";
import { DatabaseSync } from "node:sqlite";
import { expect } from "../helpers";

/**
 * Read-only access to Weaver's datastore for assertions the API does not
 * expose (the script event queue, settings rows written at boot). The phase's
 * datastore decides the backend: SQLite at /weaver-config/weaver.db (the
 * weaver-config volume), or the Postgres service named by
 * E2E_WEAVER_DATABASE_URL.
 *
 * Queries are plain SQL text that both backends accept; values reach the SQL
 * through `literal`, never by string concatenation of raw input. Every value
 * comes back as a string (or null) so a spec reads both backends the same way.
 */
export type Row = Record<string, string | null>;

export const SQLITE_PATH = "/weaver-config/weaver.db";

export function datastoreKind(): "sqlite" | "postgres" {
  return (process.env.E2E_WEAVER_RELEASE_FLOW_DATASTORE ?? "sqlite") === "postgres" ? "postgres" : "sqlite";
}

/** A SQL string literal both backends accept. */
export function literal(value: string | number | null): string {
  if (value === null) return "NULL";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error(`non-finite SQL number ${value}`);
    return String(value);
  }
  if (value.includes("\0")) throw new Error("NUL in SQL literal");
  return `'${value.replaceAll("'", "''")}'`;
}

export async function query(sql: string): Promise<Row[]> {
  return datastoreKind() === "postgres" ? await postgresQuery(postgresUrl(), sql) : sqliteQuery(sql);
}

/** Poll `sql` until `predicate` holds for its rows; returns those rows. */
export async function waitRows(sql: string, predicate: (rows: Row[]) => boolean, describe: string): Promise<Row[]> {
  let rows: Row[] = [];
  await expect.poll(async () => predicate(rows = await query(sql)),
    { message: describe, timeout: 0 }).toBe(true);
  return rows;
}

export async function setting(key: string): Promise<string | null | undefined> {
  const rows = await query(`SELECT value FROM settings WHERE key = ${literal(key)}`);
  return rows.length === 0 ? undefined : rows[0]!.value;
}

function sqliteQuery(sql: string): Row[] {
  const database = new DatabaseSync(SQLITE_PATH, { readOnly: true });
  try {
    return (database.prepare(sql).all() as Array<Record<string, unknown>>).map(row =>
      Object.fromEntries(Object.entries(row).map(([key, value]) => [key, value === null || value === undefined ? null : String(value)])));
  } finally {
    database.close();
  }
}

function postgresUrl(): URL {
  const raw = process.env.E2E_WEAVER_DATABASE_URL;
  expect(raw, "a postgres phase must pass E2E_WEAVER_DATABASE_URL to Playwright").toBeTruthy();
  return new URL(raw!);
}

// ------------------------------------------------------------ postgres wire

/** Buffers socket bytes and hands out whole backend messages. */
class MessageReader {
  private buffer = Buffer.alloc(0);
  private waiters: Array<() => void> = [];
  private failure: Error | undefined;

  constructor(socket: net.Socket | tls.TLSSocket) {
    socket.on("data", chunk => { this.buffer = Buffer.concat([this.buffer, chunk]); this.wake(); });
    socket.on("error", error => { this.failure = error; this.wake(); });
    socket.on("close", () => { this.failure ??= new Error("postgres connection closed"); this.wake(); });
  }

  private wake() { for (const waiter of this.waiters.splice(0)) waiter(); }

  async bytes(count: number): Promise<Buffer> {
    while (this.buffer.length < count) {
      if (this.failure) throw this.failure;
      await new Promise<void>(resolve => this.waiters.push(resolve));
    }
    const out = this.buffer.subarray(0, count);
    this.buffer = this.buffer.subarray(count);
    return out;
  }

  async message(): Promise<{ type: string; body: Buffer }> {
    const header = await this.bytes(5);
    const body = await this.bytes(header.readInt32BE(1) - 4);
    return { type: String.fromCharCode(header[0]!), body };
  }
}

function frame(type: string, ...parts: Buffer[]): Buffer {
  const body = Buffer.concat(parts);
  const header = Buffer.alloc(5);
  header.write(type, 0, "latin1");
  header.writeInt32BE(body.length + 4, 1);
  return Buffer.concat([header, body]);
}
const int32 = (value: number) => { const out = Buffer.alloc(4); out.writeInt32BE(value); return out; };
const cstring = (value: string) => Buffer.from(`${value}\0`, "utf8");

function backendError(body: Buffer): Error {
  const fields: Record<string, string> = {};
  let offset = 0;
  while (offset < body.length && body[offset] !== 0) {
    const code = String.fromCharCode(body[offset]!);
    const end = body.indexOf(0, offset + 1);
    fields[code] = body.subarray(offset + 1, end).toString("utf8");
    offset = end + 1;
  }
  return new Error(`postgres ${fields.S ?? "ERROR"} ${fields.C ?? ""}: ${fields.M ?? "unknown error"}`);
}

async function connect(url: URL): Promise<{ socket: tls.TLSSocket | net.Socket; reader: MessageReader }> {
  const host = url.hostname;
  const port = Number(url.port || 5432);
  const sslmode = url.searchParams.get("sslmode") ?? "prefer";
  const plain = net.connect({ host, port });
  await new Promise<void>((resolve, reject) => { plain.once("connect", resolve); plain.once("error", reject); });
  if (sslmode === "disable") return { socket: plain, reader: new MessageReader(plain) };
  // SSLRequest: the server answers one byte, S or N.
  plain.write(Buffer.concat([int32(8), int32(80877103)]));
  const answer = await new Promise<Buffer>((resolve, reject) => { plain.once("data", resolve); plain.once("error", reject); });
  if (answer[0] !== 0x53) {
    if (sslmode === "require" || sslmode.startsWith("verify")) throw new Error("postgres refused TLS");
    return { socket: plain, reader: new MessageReader(plain) };
  }
  // The e2e service uses a self-signed certificate; `require` encrypts without verifying, as libpq does.
  const secure = tls.connect({ socket: plain, servername: net.isIP(host) ? undefined : host, rejectUnauthorized: false });
  await new Promise<void>((resolve, reject) => { secure.once("secureConnect", resolve); secure.once("error", reject); });
  return { socket: secure, reader: new MessageReader(secure) };
}

async function authenticate(socket: net.Socket | tls.TLSSocket, reader: MessageReader, user: string, password: string): Promise<void> {
  let clientNonce = "";
  let clientFirstBare = "";
  let serverSignature = Buffer.alloc(0);
  for (;;) {
    const message = await reader.message();
    if (message.type === "E") throw backendError(message.body);
    if (message.type === "Z") return;
    if (message.type !== "R") continue; // ParameterStatus, BackendKeyData, notices
    const code = message.body.readInt32BE(0);
    if (code === 0) continue; // AuthenticationOk; wait for ReadyForQuery
    if (code === 3) { socket.write(frame("p", cstring(password))); continue; }
    if (code === 5) {
      const salt = message.body.subarray(4, 8);
      const inner = crypto.createHash("md5").update(password + user).digest("hex");
      socket.write(frame("p", cstring(`md5${crypto.createHash("md5").update(inner).update(salt).digest("hex")}`)));
      continue;
    }
    if (code === 10) {
      const mechanisms = message.body.subarray(4).toString("utf8").split("\0").filter(Boolean);
      if (!mechanisms.includes("SCRAM-SHA-256")) throw new Error(`postgres offers no supported SASL mechanism: ${mechanisms}`);
      clientNonce = crypto.randomBytes(18).toString("base64");
      clientFirstBare = `n=,r=${clientNonce}`;
      const first = Buffer.from(`n,,${clientFirstBare}`, "utf8");
      socket.write(frame("p", cstring("SCRAM-SHA-256"), int32(first.length), first));
      continue;
    }
    if (code === 11) {
      const serverFirst = message.body.subarray(4).toString("utf8");
      const attributes = Object.fromEntries(serverFirst.split(",").map(part => [part[0], part.slice(2)]));
      if (!attributes.r?.startsWith(clientNonce)) throw new Error("postgres SCRAM nonce mismatch");
      const salted = crypto.pbkdf2Sync(password.normalize("NFKC"), Buffer.from(attributes.s!, "base64"), Number(attributes.i), 32, "sha256");
      const clientKey = crypto.createHmac("sha256", salted).update("Client Key").digest();
      const storedKey = crypto.createHash("sha256").update(clientKey).digest();
      const finalWithoutProof = `c=biws,r=${attributes.r}`;
      const authMessage = `${clientFirstBare},${serverFirst},${finalWithoutProof}`;
      const clientSignature = crypto.createHmac("sha256", storedKey).update(authMessage).digest();
      const proof = Buffer.from(clientKey.map((byte, index) => byte ^ clientSignature[index]!));
      const serverKey = crypto.createHmac("sha256", salted).update("Server Key").digest();
      serverSignature = crypto.createHmac("sha256", serverKey).update(authMessage).digest();
      socket.write(frame("p", Buffer.from(`${finalWithoutProof},p=${proof.toString("base64")}`, "utf8")));
      continue;
    }
    if (code === 12) {
      const verifier = message.body.subarray(4).toString("utf8");
      if (verifier !== `v=${serverSignature.toString("base64")}`) throw new Error("postgres SCRAM server signature mismatch");
      continue;
    }
    throw new Error(`unsupported postgres authentication request ${code}`);
  }
}

export async function postgresQuery(url: URL, sql: string): Promise<Row[]> {
  const { socket, reader } = await connect(url);
  try {
    const user = decodeURIComponent(url.username);
    const database = decodeURIComponent(url.pathname.replace(/^\//, "")) || user;
    socket.write(Buffer.concat([
      (() => { const startup = Buffer.concat([int32(196608), cstring("user"), cstring(user), cstring("database"), cstring(database), Buffer.from([0])]); return Buffer.concat([int32(startup.length + 4), startup]); })(),
    ]));
    await authenticate(socket, reader, user, decodeURIComponent(url.password));
    socket.write(frame("Q", cstring(sql)));
    let columns: string[] = [];
    const rows: Row[] = [];
    let failure: Error | undefined;
    for (;;) {
      const message = await reader.message();
      if (message.type === "T") {
        columns = [];
        let offset = 2;
        for (let index = 0; index < message.body.readInt16BE(0); index += 1) {
          const end = message.body.indexOf(0, offset);
          columns.push(message.body.subarray(offset, end).toString("utf8"));
          offset = end + 1 + 18; // table oid, column, type oid, size, modifier, format
        }
      } else if (message.type === "D") {
        const row: Row = {};
        let offset = 2;
        for (let index = 0; index < message.body.readInt16BE(0); index += 1) {
          const length = message.body.readInt32BE(offset);
          offset += 4;
          if (length < 0) { row[columns[index]!] = null; continue; }
          row[columns[index]!] = message.body.subarray(offset, offset + length).toString("utf8");
          offset += length;
        }
        rows.push(row);
      } else if (message.type === "E") {
        failure = backendError(message.body);
      } else if (message.type === "Z") {
        break;
      }
    }
    if (failure) throw failure;
    return rows;
  } finally {
    try { socket.write(frame("X")); } catch { /* closing anyway */ }
    socket.end();
  }
}
