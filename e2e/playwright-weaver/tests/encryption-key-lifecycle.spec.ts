import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
} from "node:crypto";
import {
  chmodSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { expect, graphql, test, updateConfiguredServer } from "./helpers";

type LifecycleState = {
  serverId: number;
  apiKey: {
    id: number;
    name: string;
    scope: string;
  };
  rawApiKey: string;
};

type SealedLifecycleState = {
  version: 1;
  nonce: string;
  authTag: string;
  ciphertext: string;
};

const stage = process.env.E2E_WEAVER_ENCRYPTION_LIFECYCLE_STAGE ?? "";

test("encrypted credentials and API keys survive every container lifecycle phase", async ({
  request,
}) => {
  if (stage === "initial") {
    const server = await updateConfiguredServer(request, "nntp", {
      username: "e2e-user",
      password: "e2e-pass",
      maxDownloadSpeed: 0,
      downloadQuota: {
        enabled: false,
        limitBytes: 0,
        period: "ONE_TIME",
        resetTimeMinutesLocal: 0,
        weeklyResetWeekday: "MON",
        monthlyResetDay: 1,
      },
    });
    const createdKey = await graphql<{
      createApiKey: {
        key: { id: number; name: string; scope: string };
        rawKey: string;
      };
    }>(
      request,
      `mutation WeaverE2EEncryptedApiKey($name: String!, $scope: ApiKeyScope!) {
        createApiKey(name: $name, scope: $scope) {
          key { id name scope }
          rawKey
        }
      }`,
      { name: "e2e-encryption-lifecycle", scope: "READ" },
    );
    expect(createdKey.createApiKey.rawKey.length).toBeGreaterThan(20);

    const state: LifecycleState = {
      serverId: server.id,
      apiKey: createdKey.createApiKey.key,
      rawApiKey: createdKey.createApiKey.rawKey,
    };
    writeSealedState(state);
    await verifyPersistedState(request, state);
    return;
  }

  if (
    stage === "restart" ||
    stage === "recreate" ||
    stage === "down-up" ||
    stage === "final-recovery"
  ) {
    await verifyPersistedState(request, readSealedState());
    return;
  }

  throw new Error(
    `E2E_WEAVER_ENCRYPTION_LIFECYCLE_STAGE must be initial, restart, recreate, down-up, or final-recovery; got ${JSON.stringify(stage)}`,
  );
});

async function verifyPersistedState(
  request: Parameters<typeof graphql>[0],
  state: LifecycleState,
): Promise<void> {
  const connection = await graphql<{
    testConfiguredServerConnection: {
      success: boolean;
      message: string;
      latencyMs: number | null;
      supportsPipelining: boolean;
    };
  }>(
    request,
    `mutation WeaverE2EPersistedConnection($id: Int!) {
      testConfiguredServerConnection(id: $id) {
        success message latencyMs supportsPipelining
      }
    }`,
    { id: state.serverId },
  );
  expect(
    connection.testConfiguredServerConnection.success,
    connection.testConfiguredServerConnection.message,
  ).toBeTruthy();

  const details = await graphql<{
    server: { id: number; username: string | null };
  }>(
    request,
    "query WeaverE2EEncryptedServerRead($id: Int!) { server(id: $id) { id username } }",
    { id: state.serverId },
  );
  expect(details.server).toEqual({ id: state.serverId, username: "e2e-user" });
  expect(JSON.stringify(details)).not.toContain("e2e-pass");

  const response = await fetch(weaverGraphqlUrl(), {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": state.rawApiKey,
    },
    body: JSON.stringify({ query: "query WeaverE2EApiKeyProof { version }" }),
  });
  const proofBody = await response.text();
  expect(response.ok, proofBody).toBeTruthy();
  const proof = JSON.parse(proofBody) as {
    data?: { version?: string };
    errors?: unknown[];
  };
  expect(proof.errors ?? []).toEqual([]);
  expect(proof.data?.version).toBeTruthy();

  const listed = await graphql<{
    apiKeys: Array<{ id: number; name: string; scope: string }>;
  }>(
    request,
    "query WeaverE2EEncryptedApiKeys { apiKeys { id name scope } }",
  );
  expect(listed.apiKeys).toContainEqual(state.apiKey);
  expect(JSON.stringify(listed)).not.toContain(state.rawApiKey);
}

function writeSealedState(state: LifecycleState): void {
  const secret = lifecycleSecret();
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", deriveStateKey(secret), nonce);
  const ciphertext = Buffer.concat([
    cipher.update(JSON.stringify(state), "utf8"),
    cipher.final(),
  ]);
  const sealed: SealedLifecycleState = {
    version: 1,
    nonce: nonce.toString("base64"),
    authTag: cipher.getAuthTag().toString("base64"),
    ciphertext: ciphertext.toString("base64"),
  };
  const path = lifecycleStatePath();
  writeFileSync(path, `${JSON.stringify(sealed)}\n`, { encoding: "utf8", mode: 0o600 });
  chmodSync(path, 0o600);
}

function readSealedState(): LifecycleState {
  const sealed = JSON.parse(
    readFileSync(lifecycleStatePath(), "utf8"),
  ) as SealedLifecycleState;
  expect(sealed.version).toBe(1);
  const decipher = createDecipheriv(
    "aes-256-gcm",
    deriveStateKey(lifecycleSecret()),
    Buffer.from(sealed.nonce, "base64"),
  );
  decipher.setAuthTag(Buffer.from(sealed.authTag, "base64"));
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(sealed.ciphertext, "base64")),
    decipher.final(),
  ]);
  return JSON.parse(plaintext.toString("utf8")) as LifecycleState;
}

function lifecycleStatePath(): string {
  const path = process.env.E2E_WEAVER_ENCRYPTION_LIFECYCLE_STATE_FILE;
  if (!path) {
    throw new Error("E2E_WEAVER_ENCRYPTION_LIFECYCLE_STATE_FILE is required");
  }
  return path;
}

function lifecycleSecret(): string {
  const secret = process.env.E2E_WEAVER_ENCRYPTION_LIFECYCLE_STATE_SECRET;
  if (!secret) {
    throw new Error("E2E_WEAVER_ENCRYPTION_LIFECYCLE_STATE_SECRET is required");
  }
  return secret;
}

function deriveStateKey(secret: string): Buffer {
  return createHash("sha256").update(secret, "utf8").digest();
}

function weaverGraphqlUrl(): string {
  const url = new URL(
    process.env.PLAYWRIGHT_BASE_URL || "http://weaver:9090/",
  );
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/graphql`;
  url.search = "";
  url.hash = "";
  return url.toString();
}
