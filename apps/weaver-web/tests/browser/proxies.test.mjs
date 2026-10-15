import assert from "node:assert/strict";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { createServer } from "vite";
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE_PATH ?? "playwright");
let server, browser, baseUrl;
before(async () => {
  server = await createServer({ cacheDir: "node_modules/.vite/browser-proxies", server: { host: "127.0.0.1", port: 0 } });
  await server.listen();
  baseUrl = `http://127.0.0.1:${server.httpServer.address().port}`;
  browser = await chromium.launch({ headless: true });
});
after(async () => { await browser?.close(); await server?.close(); });
async function open(query = "", shown = "Amsterdam") {
  const page = await browser.newPage({ viewport: { width: 1400, height: 1500 } });
  page.setDefaultTimeout(0);
  page.on("pageerror", (error) => console.error(error));
  await page.route("**/*", (route) => new URL(route.request().url()).origin === baseUrl ? route.continue() : route.abort());
  await page.goto(`${baseUrl}/tests/browser/proxies.html${query}`);
  await page.getByText(shown, { exact: true }).waitFor();
  return page;
}
/** With `PROXIES_SCREENSHOT_DIR` set to a directory, the page as it stands is saved there as `name`.png. */
async function shot(page, name) {
  if (process.env.PROXIES_SCREENSHOT_DIR) {
    await page.screenshot({ path: join(process.env.PROXIES_SCREENSHOT_DIR, `${name}.png`) });
  }
}
const daemon = (page) => page.evaluate(() => window.proxiesFixture);
const action = (scope, name) => scope.getByRole("button", { name, exact: true });
const field = (scope, name) => scope.getByLabel(name, { exact: true });
/** The fields in `scope` a password manager is still free to fill or to offer to save. */
const openToPasswordManagers = (scope) =>
  scope
    .locator("input:not([type='file']):not([type='checkbox']):not([type='radio']):not([type='range']), textarea")
    .evaluateAll((fields) =>
      fields
        .filter((field) => field.getAttribute("data-1p-ignore") !== "true" || field.getAttribute("autocomplete") !== "off")
        .map((field) => field.getAttribute("aria-label") ?? field.outerHTML));
const option = (scope, name) => scope.getByRole("radio", { name, exact: true });
const dropZone = (scope) => action(scope, "Drop a config file here, or click to choose");
const file = (name, text) => ({ name, mimeType: "text/plain", buffer: Buffer.from(text) });
/** Drags a file of this name and text over `zone`, and lets go of it there. */
async function drop(page, zone, name, text) {
  const dragged = await page.evaluateHandle(([fileName, content]) => {
    const transfer = new DataTransfer();
    transfer.items.add(new File([content], fileName, { type: "text/plain" }));
    return transfer;
  }, [name, text]);
  await zone.dispatchEvent("dragover", { dataTransfer: dragged });
  await zone.dispatchEvent("drop", { dataTransfer: dragged });
}
/** Waits for the faults listed to be exactly these: the last of them is the one no earlier reading had. */
async function faults(scope, expected) {
  const list = scope.getByRole("alert").getByRole("listitem");
  await list.filter({ hasText: expected.at(-1) }).waitFor();
  assert.deepEqual(await list.allTextContents(), expected);
}
async function adding(page) {
  await action(page.locator("main"), "Add proxy").click();
  const editor = page.getByRole("dialog", { name: "Add proxy", exact: true });
  await editor.waitFor();
  return editor;
}
async function editing(page, name) {
  await page.getByText(name, { exact: true }).click();
  const editor = page.getByRole("dialog", { name, exact: true });
  await editor.waitFor();
  return editor;
}
async function pick(page, scope, label, choice) {
  await action(scope, label).click();
  await page.getByRole("menuitemradio", { name: choice, exact: true }).click();
}

const KEY = (letter) => `${letter.repeat(43)}=`;
const CONFIG = `[Interface]
PrivateKey = ${KEY("a")}
Address = 10.6.0.2/32, fd00::2/128
DNS = 10.6.0.1
MTU = 1380
ListenPort = 51820

[Peer]
PublicKey = ${KEY("b")}
PresharedKey = ${KEY("c")}
AllowedIPs = 0.0.0.0/0
Endpoint = vpn.fixture.invalid:51999
PersistentKeepalive = off
`;
const DETAILS = ["Endpoint", "Port", "Private key", "Peer public key", "Preshared key", "Interface addresses", "MTU", "Keepalive"];
/** What the configuration above fills the form with, as the daemon is sent it. */
const FILLED = {
  kind: "WIRE_GUARD", enabled: true, host: "vpn.fixture.invalid", port: 51999, resolver: "PROXY", dnsServers: ["10.6.0.1"],
  tunnelAddresses: ["10.6.0.2/32", "fd00::2/128"], peerPublicKey: KEY("b"), mtu: 1380, keepaliveSeconds: 0,
  timeoutSeconds: 30, privateKey: KEY("a"), presharedKey: KEY("c"),
};
async function filled(editor) {
  // What the file carried that a tunnel has no use for is said, not dropped in silence.
  await editor.getByText(
    "Filled the form from the configuration. Ignored ListenPort, AllowedIPs, which a tunnel proxy does not use.",
    { exact: true },
  ).waitFor();
  assert.equal(await option(editor, "Enter details manually").getAttribute("aria-checked"), "true");
  assert.equal(await field(editor, "Endpoint").inputValue(), "vpn.fixture.invalid");
  assert.equal(await field(editor, "Port").inputValue(), "51999");
  assert.equal(await field(editor, "Private key").inputValue(), KEY("a"));
  assert.equal(await field(editor, "Private key").getAttribute("type"), "password");
  assert.equal(await field(editor, "Peer public key").inputValue(), KEY("b"));
  assert.equal(await field(editor, "Preshared key").inputValue(), KEY("c"));
  assert.equal(await field(editor, "Interface addresses").inputValue(), "10.6.0.2/32\nfd00::2/128");
  // The tunnel's DNS servers answer inside it, so the proxy resolves, and only with servers.
  assert.equal(await option(editor, "Proxy").getAttribute("aria-checked"), "true");
  assert.equal(await editor.getByRole("radiogroup", { name: "Using", exact: true }).count(), 0);
  assert.equal(await field(editor, "DNS servers").inputValue(), "10.6.0.1");
  assert.equal(await field(editor, "MTU").inputValue(), "1380");
  assert.equal(await field(editor, "Keepalive").inputValue(), "0");
}

test("a new WireGuard proxy has three ways in and opens on the file, and entering the details by hand opens an empty form", async () => {
  const page = await open();
  try {
    const editor = await adding(page);
    const start = editor.getByRole("radiogroup", { name: "Set up from", exact: true });
    assert.deepEqual(await start.getByRole("radio").allTextContents(), [
      "Upload a file", "Paste raw config", "Enter details manually",
    ]);
    // It opens waiting for a file, so there are no details to fill in yet and nothing to save.
    assert.deepEqual(await start.locator("[aria-checked='true']").allTextContents(), ["Upload a file"]);
    await dropZone(editor).waitFor();
    for (const name of DETAILS) assert.equal(await field(editor, name).count(), 0, name);
    assert.equal(await editor.getByRole("textbox", { name: "WireGuard configuration", exact: true }).count(), 0);
    assert.equal(await action(editor, "Save").isDisabled(), true);
    await field(editor, "Name").waitFor();
    await shot(page, "proxy-wireguard-start");

    await option(editor, "Enter details manually").click();
    for (const name of DETAILS) await field(editor, name).waitFor();
    for (const name of ["Endpoint", "Private key", "Peer public key", "Preshared key", "Interface addresses", "MTU", "Keepalive"]) {
      assert.equal(await field(editor, name).inputValue(), "", name);
    }
    // Weaver resolves with the host's own DNS until told otherwise.
    assert.equal(await option(editor, "Weaver").getAttribute("aria-checked"), "true");
    assert.equal(await option(editor, "Host").getAttribute("aria-checked"), "true");
    assert.equal(await field(editor, "DNS servers").count(), 0);
    assert.equal(await field(editor, "Port").inputValue(), "51820");
    assert.equal(await action(editor, "Save").isDisabled(), false);
    await shot(page, "proxy-wireguard-manual");
    // Another way in shuts the details again without losing them.
    await field(editor, "Endpoint").fill("typed.fixture.invalid");
    await option(editor, "Paste raw config").click();
    assert.equal(await dropZone(editor).count(), 0);
    assert.equal(await field(editor, "Endpoint").count(), 0);
    assert.equal(await action(editor, "Save").isDisabled(), true);
    await option(editor, "Enter details manually").click();
    assert.equal(await field(editor, "Endpoint").inputValue(), "typed.fixture.invalid");
  } finally { await page.close(); }
});

test("a pasted configuration is checked as it is typed, and Parse fills the form once nothing is wrong with it", async () => {
  const page = await open();
  try {
    const editor = await adding(page);
    await option(editor, "Paste raw config").click();
    const config = editor.getByRole("textbox", { name: "WireGuard configuration", exact: true });
    const parse = action(editor, "Parse");
    assert.equal(await parse.isDisabled(), true);
    assert.equal(await editor.getByRole("alert").count(), 0);

    await config.fill("the quick brown fox");
    await faults(editor, ["That does not look like a WireGuard configuration."]);
    assert.equal(await parse.isDisabled(), true);

    // Every fault is listed at once, by the key at fault.
    await config.fill(`[Interface]\nPrivateKey = short\nMTU = big\n\n[Peer]\nEndpoint = vpn.fixture.invalid\n`);
    await faults(editor, [
      "PrivateKey is not a WireGuard key.",
      "Address is missing from [Interface].",
      "MTU is not a number.",
      "PublicKey is missing from [Peer].",
      "Endpoint is not host:port.",
    ]);
    assert.equal(await parse.isDisabled(), true);
    await shot(page, "proxy-wireguard-paste-errors");

    await config.fill(CONFIG);
    // Parse comes on once nothing is wrong with it; a trial click waits for that.
    await parse.click({ trial: true });
    assert.equal(await editor.getByRole("alert").count(), 0);
    assert.equal(await parse.isDisabled(), false);
    // Nothing is filled until Parse is pressed.
    assert.equal(await field(editor, "Endpoint").count(), 0);
    await shot(page, "proxy-wireguard-paste-valid");
    await parse.click();
    await filled(editor);
    await shot(page, "proxy-wireguard-filled");

    // The details are the form's own from here: one is changed, and that is what is saved.
    await field(editor, "MTU").fill("1400");
    await field(editor, "Name").fill("Pasted");
    await action(editor, "Save").click();
    await page.getByText("Pasted", { exact: true }).waitFor();
    const held = await daemon(page);
    assert.deepEqual(held.requests.at(-1), {
      name: "SaveProxyProfile", variables: { id: null, input: { name: "Pasted", ...FILLED, mtu: 1400 } },
    });
  } finally { await page.close(); }
});

test("a configuration file dropped on the target or chosen through it is read, its faults are listed, and a sound one fills the form", async () => {
  const page = await open();
  try {
    const editor = await adding(page);
    const zone = dropZone(editor);
    await zone.waitFor();
    const input = editor.locator("input[type='file']");

    await input.setInputFiles(file("notes.conf", "the quick brown fox"));
    await editor.getByText("notes.conf", { exact: true }).waitFor();
    await faults(editor, ["That does not look like a WireGuard configuration."]);
    assert.equal(await field(editor, "Endpoint").count(), 0);

    await drop(page, zone, "broken.conf", CONFIG.replace(KEY("b"), "short").replace("Address", "#Address"));
    await editor.getByText("broken.conf", { exact: true }).waitFor();
    await faults(editor, [
      "Address is missing from [Interface].",
      "PublicKey is not a WireGuard key.",
    ]);
    assert.equal(await action(editor, "Save").isDisabled(), true);
    await shot(page, "proxy-wireguard-upload-errors");

    await input.setInputFiles(file("too-large.conf", `${CONFIG}#${"x".repeat(65536)}`));
    await editor.getByText("too-large.conf", { exact: true }).waitFor();
    await faults(editor, ["That configuration is larger than 64 KiB."]);

    await drop(page, zone, "wg0.conf", CONFIG);
    await filled(editor);
    assert.equal(await editor.getByRole("alert").count(), 0);
    await field(editor, "Name").fill("Uploaded");
    await action(editor, "Save").click();
    await page.getByText("Uploaded", { exact: true }).waitFor();
    assert.deepEqual((await daemon(page)).requests.at(-1), {
      name: "SaveProxyProfile", variables: { id: null, input: { name: "Uploaded", ...FILLED } },
    });
  } finally { await page.close(); }
});

test("a stored WireGuard proxy opens on its details, and a configuration can still be loaded over them", async () => {
  const page = await open();
  try {
    const editor = await editing(page, "Amsterdam");
    assert.equal(await option(editor, "Enter details manually").getAttribute("aria-checked"), "true");
    assert.equal(await field(editor, "Endpoint").inputValue(), "amsterdam.fixture.invalid");
    assert.equal(await field(editor, "Interface addresses").inputValue(), "10.0.0.1/32");
    assert.equal(await action(editor, "Save").isDisabled(), false);
    await option(editor, "Upload a file").click();
    await dropZone(editor).waitFor();
    await editor.locator("input[type='file']").setInputFiles(file("wg0.conf", CONFIG));
    await filled(editor);
  } finally { await page.close(); }
});

test("pools and proxies are each added from the middle under their own table, and never from the top bar", async () => {
  const page = await open("?pools");
  try {
    const list = page.locator("main");
    for (const name of ["Add pool", "Add proxy"]) {
      const under = action(list, name);
      await under.waitFor();
      assert.equal(await action(page, name).count(), 1);
      // The button's middle is the list's middle, to the pixel either side.
      const [button, table] = [await under.boundingBox(), await list.boundingBox()];
      const off = button.x + button.width / 2 - (table.x + table.width / 2);
      assert.ok(Math.abs(off) <= 1, `${name} is centred`);
      await under.click();
      const editor = page.getByRole("dialog", { name, exact: true });
      await editor.waitFor();
      // A new pool, like a new proxy, starts as WireGuard.
      if (name === "Add pool") {
        assert.match(await action(editor, "Proxy type").innerText(), /WireGuard/);
      }
      await action(editor, "Cancel").click();
      await editor.waitFor({ state: "detached" });
    }
    assert.equal(await page.locator("#controls").getByRole("button").count(), 0);
    await shot(page, "pools-and-proxies");
  } finally {
    await page.close();
  }
});

test("a pool can be saved before it has a member, and saves with one", async () => {
  const page = await open("?pools");
  try {
    await action(page.locator("main"), "Add pool").click();
    const editor = page.getByRole("dialog", { name: "Add pool", exact: true });
    await field(editor, "Name").fill("Solo");
    // A pool can be made before any proxy is put in it.
    assert.equal(await action(editor, "Save pool").isEnabled(), true);
    await editor.getByRole("checkbox", { name: "Amsterdam", exact: true }).click();
    await editor.getByRole("checkbox", { name: "Amsterdam", checked: true, exact: true }).waitFor();
    assert.equal(await action(editor, "Save pool").isEnabled(), true);
    await action(editor, "Save pool").click();
    await editor.waitFor({ state: "detached" });
    const { poolSaves } = await daemon(page);
    assert.deepEqual(poolSaves.map(({ input }) => input), [{ name: "Solo", kind: "WIRE_GUARD", memberIds: [1], enabled: true }]);
  } finally {
    await page.close();
  }
});

/** The line of a table that holds `text`, which is a button itself and holds its own Edit button. */
const line = (page, text) => page.locator("main").getByRole("button").filter({ hasText: text });
/** Opens `title` from the line's own body, then from its Edit button, and closes it each time. */
async function opensBothWays(page, text, edit, title) {
  const editor = page.getByRole("dialog", { name: title, exact: true });
  for (const target of [page.getByText(text, { exact: true }), action(line(page, text), edit)]) {
    await target.click();
    await editor.waitFor();
    await action(editor, "Cancel").click();
    await editor.waitFor({ state: "detached" });
  }
}

test("a pool and a proxy open from anywhere on their line and from its Edit button", async () => {
  const page = await open("?pools");
  try {
    await opensBothWays(page, "Exits", "Edit pool", "Exits");
    await opensBothWays(page, "Seedbox", "Edit proxy", "Seedbox");
    await shot(page, "pools-and-proxies-edit");
  } finally {
    await page.close();
  }
});

test("a proxy is removed from its own line once confirmed, and a refusal says why", async () => {
  const page = await open();
  try {
    const proxies = page.locator("main");
    assert.equal(await proxies.getByText("assigned to providers and feeds on their own panels").count(), 0);
    const confirm = page.getByRole("dialog", { name: "Remove proxy", exact: true });

    // A pool's member is refused, and the dialog stays open to say so.
    await action(line(page, "Relay"), "Remove").click();
    await action(confirm, "Remove proxy").click();
    await confirm.getByRole("alert").getByText("This proxy is in a pool. Remove it from the pool first.", { exact: true }).waitFor();
    await action(confirm, "Cancel").click();
    await confirm.waitFor({ state: "detached" });
    assert.equal(await line(page, "Relay").count(), 1);

    // Anything else goes once confirmed.
    await action(line(page, "Seedbox"), "Remove").click();
    assert.equal(await confirm.getByRole("alert").count(), 0);
    await shot(page, "proxy-remove");
    await action(confirm, "Remove proxy").click();
    await confirm.waitFor({ state: "detached" });
    await line(page, "Seedbox").waitFor({ state: "detached" });
    const removes = (await daemon(page)).requests.filter((request) => request.name === "DeleteProxyProfile");
    assert.deepEqual(removes.map((request) => request.variables.id), [3, 2]);
  } finally {
    await page.close();
  }
});

test("a route opens from anywhere on its line and from its Edit button", async () => {
  const page = await open("?routes", "news.fixture.invalid");
  try {
    await opensBothWays(page, "news.fixture.invalid", "Edit route", "Route for news.fixture.invalid");
    await shot(page, "routes-edit");
  } finally {
    await page.close();
  }
});

test("a rung starts on the first target it offers, and a chain's hops offer only the proxies that can still follow", async () => {
  const page = await open("?routes", "news.fixture.invalid");
  try {
    await action(page, "Edit route").click();
    const editor = page.getByRole("dialog", { name: "Route for news.fixture.invalid", exact: true });
    // A ladder starts with its first rung.
    await option(editor, "Proxy ladder").click();
    // A new ladder falls back to direct until that is turned off.
    assert.equal(await editor.getByRole("checkbox", { name: "Allow direct fallback on this egress", exact: true }).isChecked(), true);
    // A proxy's or a pool's select sits under the rung's type, where a chain's hops do, and starts on the first it offers.
    const type = editor.getByRole("radiogroup", { name: "Leg 1 rung 1 type", exact: true });
    for (const [kind, first] of [["Proxy", "Amsterdam"], ["Pool", "Exits"]]) {
      await option(editor, kind).click();
      const target = action(editor, "Leg 1 rung 1");
      assert.equal(await target.textContent(), first);
      const [select, group] = [await target.boundingBox(), await type.boundingBox()];
      assert.ok(select.y >= group.y + group.height, `${kind} select is under the type`);
      assert.ok(select.x < group.x, `${kind} select starts at the rung's left edge`);
    }
    await shot(page, "route-rung-pool");
    await option(editor, "Chain").click();
    const hop = (position) => action(editor, `Chain hop ${position}`);
    const hops = () => Promise.all([1, 2, 3].map((position) => hop(position).textContent()));
    /** What the hop's menu lists, read with the menu open and closed again by picking `choice`. */
    const offered = async (position, choice) => {
      await hop(position).click();
      const items = page.getByRole("menuitemradio");
      const names = await items.allTextContents();
      await items.filter({ hasText: choice }).click();
      return names;
    };
    // A new chain takes the first two proxies that can ride one another, and no third.
    assert.deepEqual(await hops(), ["Amsterdam · WireGuard", "Seedbox · SSH", "None"]);
    assert.deepEqual(await Promise.all([1, 2, 3].map((position) => hop(position).isDisabled())), [false, false, false]);
    // The three share one row, however long the names in them.
    const tops = await Promise.all([1, 2, 3].map(async (position) => (await hop(position).boundingBox()).y));
    assert.equal(new Set(tops).size, 1, `hops sit at ${tops.join(", ")}`);
    // Under each hop, whether the chain needs it: the first two, and not the third.
    const notes = editor.locator("span").filter({ hasText: /^(\* Required|Optional)$/ });
    assert.deepEqual(await notes.allTextContents(), ["* Required", "* Required", "Optional"]);
    await shot(page, "route-chain-new");

    // The proxy beneath is gone from the list, and WireGuard still rides WireGuard.
    assert.deepEqual(await offered(2, "Seedbox"), ["Seedbox · SSH", "Relay · SOCKS5", "Paris · WireGuard"]);
    // Nothing that needs UDP rides SSH.
    assert.deepEqual(await offered(3, "Relay"), ["None", "Relay · SOCKS5"]);
    await shot(page, "route-chain");
    // The third hop comes off again.
    await offered(3, "None");
    assert.deepEqual(await hops(), ["Amsterdam · WireGuard", "Seedbox · SSH", "None"]);

    // A second hop that cannot ride the new first gives way to the first that can.
    await offered(2, "Paris");
    assert.deepEqual(await offered(1, "Seedbox"), ["Amsterdam · WireGuard", "Seedbox · SSH", "Relay · SOCKS5"]);
    assert.deepEqual(await hops(), ["Seedbox · SSH", "Relay · SOCKS5", "None"]);

    // An added rung starts on the first proxy the ladder has not used.
    await action(editor, "Add rung").click();
    assert.equal(await action(editor, "Leg 1 rung 2").textContent(), "Amsterdam");
  } finally {
    await page.close();
  }
});

test("SSH signs in with an Ed25519 key and has no password", async () => {
  const page = await open();
  try {
    // A password typed while the profile was another type is not sent once it is SSH.
    const editor = await adding(page);
    await pick(page, editor, "Type", "SOCKS5");
    await field(editor, "Password").fill("fixture-password");
    await pick(page, editor, "Type", "SSH");
    assert.equal(await field(editor, "Password").count(), 0);
    assert.equal(await editor.getByRole("radiogroup", { name: "Set up from", exact: true }).count(), 0);
    assert.equal(await field(editor, "Port").inputValue(), "22");
    await editor.getByText("Ed25519 keys only.", { exact: true }).waitFor();
    // None of it is the Weaver login, so no field is left to a password manager.
    assert.deepEqual(await openToPasswordManagers(editor), []);
    await shot(page, "proxy-ssh");
    await field(editor, "Name").fill("Bastion");
    await field(editor, "Host").fill("bastion.fixture.invalid");
    await field(editor, "Username").fill("operator");
    await field(editor, "Private key").fill("fixture-ed25519-key");
    await action(editor, "Save").click();
    await page.getByText("Bastion", { exact: true }).waitFor();
    // Whatever holds the table is told, so its own list of proxies can follow.
    assert.equal((await daemon(page)).changes, 1);
    const input = (await daemon(page)).requests.at(-1).variables.input;
    assert.equal(input.kind, "SSH");
    assert.equal(input.username, "operator");
    assert.equal(input.privateKey, "fixture-ed25519-key");
    assert.equal("password" in input, false);

    // A stored one says the same, beside what it says of the key it holds.
    const stored = await editing(page, "Seedbox");
    assert.equal(await field(stored, "Password").count(), 0);
    await stored.getByText("Ed25519 keys only. Stored. Leave blank to keep it.", { exact: true }).waitFor();
    await action(stored, "Cancel").click();
    // The types that do take a password still have one.
    const relay = await editing(page, "Relay");
    await field(relay, "Password").waitFor();
  } finally { await page.close(); }
});

test("a proxy's names are resolved by Weaver or the proxy, with the host's DNS or servers asked in order", async () => {
  const page = await open();
  try {
    const seedbox = await editing(page, "Seedbox");
    assert.equal(await option(seedbox, "Proxy").getAttribute("aria-checked"), "true");
    assert.equal(await option(seedbox, "Host").getAttribute("aria-checked"), "true");
    assert.equal(await field(seedbox, "DNS servers").count(), 0);
    await option(seedbox, "DNS servers").click();
    // Servers chosen and none named is not the host's DNS by another name.
    await action(seedbox, "Save").click();
    await seedbox.getByText("Add at least one DNS server.", { exact: true }).waitFor();
    await field(seedbox, "DNS servers").fill("192.0.2.53\n192.0.2.54");
    await shot(page, "proxy-resolution-servers");
    await action(seedbox, "Save").click();
    await seedbox.waitFor({ state: "detached" });
    let input = (await daemon(page)).requests.at(-1).variables.input;
    assert.equal(input.resolver, "PROXY");
    assert.deepEqual(input.dnsServers, ["192.0.2.53", "192.0.2.54"]);

    // WireGuard hands its peer no name, so a WireGuard proxy resolves only with servers.
    const amsterdam = await editing(page, "Amsterdam");
    assert.equal(await option(amsterdam, "Weaver").getAttribute("aria-checked"), "true");
    await option(amsterdam, "Proxy").click();
    assert.equal(await amsterdam.getByRole("radiogroup", { name: "Using", exact: true }).count(), 0);
    await field(amsterdam, "DNS servers").waitFor();
    await option(amsterdam, "Weaver").click();
    assert.equal(await option(amsterdam, "Host").getAttribute("aria-checked"), "true");
    assert.equal(await field(amsterdam, "DNS servers").count(), 0);
    await action(amsterdam, "Cancel").click();

    // The host's DNS sends no servers, whatever was typed before it was chosen.
    const relay = await editing(page, "Relay");
    await option(relay, "Weaver").click();
    await option(relay, "DNS servers").click();
    await field(relay, "DNS servers").fill("192.0.2.53");
    await option(relay, "Host").click();
    await action(relay, "Save").click();
    await relay.waitFor({ state: "detached" });
    input = (await daemon(page)).requests.at(-1).variables.input;
    assert.equal(input.resolver, "WEAVER");
    assert.deepEqual(input.dnsServers, []);
  } finally { await page.close(); }
});

/** Opens Test via from the proxy's own line. */
async function testingVia(page, name) {
  await action(line(page, name), "Test via").click();
  const dialog = page.getByRole("dialog", { name: "Test via", exact: true });
  await action(dialog, "Egress").waitFor();
  return dialog;
}
/** Each path a run tried, as its row reads: the hops, how it went, and what was said of it. */
async function paths(dialog, count) {
  const rows = dialog.getByRole("list", { name: "Paths", exact: true }).getByRole("listitem");
  await rows.nth(count - 1).waitFor();
  assert.equal(await rows.count(), count);
  return Promise.all(Array.from({ length: count }, async (_, index) => {
    const row = rows.nth(index);
    return {
      hops: await row.locator("span").first().textContent(),
      failing: await row.locator("span.text-wv-error-text").allTextContents(),
      sentence: await row.locator("p").textContent(),
    };
  }));
}
const runs = async (page) => (await daemon(page)).requests.filter((request) => request.name === "TestVia");

test("a proxy is tested through a route drafted for it, which cannot use the proxy, and each path is a row", async () => {
  const page = await open();
  try {
    const dialog = await testingVia(page, "Relay");
    await dialog.getByText("Relay · SOCKS5", { exact: true }).waitFor();
    // The draft starts direct on the first egress that is turned on.
    assert.equal(await action(dialog, "Egress").textContent(), "System");
    assert.equal(await option(dialog, "Direct").getAttribute("aria-checked"), "true");
    // A test tries every path, so the draft has no weights or share to hold.
    assert.equal(await dialog.getByRole("slider").count(), 0);
    assert.equal(await dialog.getByText("Simulate leg down", { exact: true }).count(), 0);
    assert.equal(await dialog.getByText("When a leg goes down", { exact: true }).count(), 0);

    await option(dialog, "Proxy ladder").click();
    // The proxy under test cannot carry itself, so it is not offered.
    await action(dialog, "Leg 1 rung 1").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["Amsterdam", "Seedbox"]);
    await page.getByRole("menuitemradio", { name: "Amsterdam", exact: true }).click();
    await action(dialog, "Add rung").click();
    assert.equal(await action(dialog, "Leg 1 rung 2").textContent(), "Seedbox");

    await action(dialog, "Test").click();
    const reachable = "Reachable. Give a host and port to check forwarding.";
    const silent = "No answer within 10 seconds. Check the address, the port and any firewall.";
    assert.deepEqual(await paths(dialog, 3), [
      { hops: "System > Amsterdam > Relay", failing: [], sentence: reachable },
      { hops: "System > Seedbox > Relay", failing: ["Seedbox"], sentence: silent },
      { hops: "System > Relay", failing: [], sentence: reachable },
    ]);
    const rows = dialog.getByRole("list", { name: "Paths", exact: true }).getByRole("listitem");
    await rows.nth(1).getByText("Failed at Seedbox", { exact: true }).waitFor();
    await rows.nth(0).getByText("Passed", { exact: true }).waitFor();
    await rows.nth(0).getByText("13 ms · source 192.0.2.10:40000", { exact: true }).waitFor();
    assert.deepEqual((await runs(page)).at(-1).variables, {
      route: {
        failover: "REDISTRIBUTE",
        legs: [{ egressId: 0, weight: 100, path: { ladder: { directFallback: true, rungs: [{ proxy: 1 }, { proxy: 2 }] } } }],
      },
      subject: { proxy: 3 },
      target: null,
    });
    await shot(page, "test-via-proxy");

    // The destination starts folded away under Optional.
    const optional = action(dialog, "Optional");
    assert.equal(await optional.getAttribute("aria-expanded"), "false");
    assert.equal(await field(dialog, "Host").count(), 0);

    // With somewhere to go, the proxy is asked to forward there.
    await optional.click();
    await field(dialog, "Host").fill("news.fixture.invalid");
    await field(dialog, "Port").fill("563");
    await action(dialog, "Test").click();
    await rows.first().getByText("Connected and forwarded through the proxy.", { exact: true }).waitFor();
    assert.deepEqual((await runs(page)).at(-1).variables.target, { host: "news.fixture.invalid", port: 563 });
    // Each path says how the host was resolved, the address it reached marked among several.
    const resolved = rows.first().locator("p").nth(1);
    assert.equal(await resolved.textContent(), "news.fixture.invalid → 192.0.2.80, 192.0.2.81 · DNS 192.0.2.53 via Relay");
    assert.equal(await resolved.locator("strong").textContent(), "192.0.2.81");
    // A path that failed before its lookup says nothing of one.
    assert.equal(await rows.nth(1).locator("p").count(), 1);

    // Folded away again, what was typed is not sent.
    await optional.click();
    assert.equal(await field(dialog, "Host").count(), 0);
    const sent = (await runs(page)).length;
    await action(dialog, "Test").click();
    await rows.first().getByText(reachable, { exact: true }).waitFor();
    assert.equal(await rows.first().locator("p").count(), 1);
    const last = await runs(page);
    assert.equal(last.length, sent + 1);
    assert.equal(last.at(-1).variables.target, null);

    // The proxy's line keeps the last run: the first path that failed.
    await action(dialog, "Close").click();
    await dialog.waitFor({ state: "detached" });
    await line(page, "Relay").getByText(silent, { exact: true }).waitFor();
    assert.equal(await line(page, "Amsterdam").getByText("—", { exact: true }).count(), 1);
  } finally { await page.close(); }
});

test("a test the daemon refuses says why in our words, and leaves no rows", async () => {
  const page = await open("?limit=2");
  try {
    const dialog = await testingVia(page, "Relay");
    // A draft has one leg: nothing to add.
    assert.equal(await action(dialog, "Add leg").count(), 0);
    await option(dialog, "Proxy ladder").click();
    await action(dialog, "Add rung").click();
    await action(dialog, "Test").click();
    await dialog.getByRole("alert").getByText("A test can try at most 2 paths. Use fewer rungs or pool members.", { exact: true }).waitFor();
    assert.equal(await dialog.getByRole("list", { name: "Paths", exact: true }).count(), 0);
    await shot(page, "test-via-refused");
    await action(dialog, "Close").click();
    await line(page, "Relay").getByText("A test can try at most 2 paths. Use fewer rungs or pool members.", { exact: true }).waitFor();
  } finally { await page.close(); }
});

test("a pool is tested member by member through a route drafted for it, and carries a host through the member it picks", async () => {
  const page = await open("?pools");
  try {
    await page.getByText("Exits", { exact: true }).click();
    const editor = page.getByRole("dialog", { name: "Exits", exact: true });
    await action(editor, "Test pool").click();
    const dialog = page.getByRole("dialog", { name: "Test via", exact: true });
    await dialog.getByText("Exits · SOCKS5", { exact: true }).waitFor();
    // The draft may not use the pool's own members.
    await option(dialog, "Proxy ladder").click();
    await action(dialog, "Leg 1 rung 1").click();
    assert.deepEqual(await page.getByRole("menuitemradio").allTextContents(), ["Amsterdam", "Seedbox"]);
    await page.getByRole("menuitemradio", { name: "Amsterdam", exact: true }).click();
    await option(dialog, "Direct").click();
    await action(dialog, "Test").click();
    assert.deepEqual(await paths(dialog, 1), [
      { hops: "System > Relay", failing: [], sentence: "Reachable. Give a host and port to check forwarding." },
    ]);
    assert.deepEqual((await runs(page)).at(-1).variables, {
      route: { failover: "REDISTRIBUTE", legs: [{ egressId: 0, weight: 100, path: { direct: true } }] },
      subject: { pool: 1 },
      target: null,
    });
    await shot(page, "test-via-pool");

    // Given somewhere to go, the pool carries it, and its hop names the member it picked.
    await action(dialog, "Optional").click();
    await field(dialog, "Host").fill("news.fixture.invalid");
    await action(dialog, "Test").click();
    const rows = dialog.getByRole("list", { name: "Paths", exact: true }).getByRole("listitem");
    await rows.first().getByText("Connected and forwarded through the proxy.", { exact: true }).waitFor();
    assert.equal(await rows.count(), 1);
    const hops = rows.first().locator("span").first();
    assert.equal(await hops.textContent(), "System > Exits (Relay)");
    assert.equal(await hops.locator("strong").textContent(), "Relay");
    assert.equal(await hops.locator("span.text-wv-error-text").count(), 0);
    assert.deepEqual((await runs(page)).at(-1).variables.target, { host: "news.fixture.invalid", port: 443 });
    await shot(page, "test-via-pool-picked");
  } finally { await page.close(); }
});

test("a pool's line opens Test via for the pool", async () => {
  const page = await open("?pools");
  try {
    const dialog = await testingVia(page, "Exits");
    await dialog.getByText("Exits · SOCKS5", { exact: true }).waitFor();
    // The pool's editor stays closed.
    assert.equal(await page.getByRole("dialog", { name: "Exits", exact: true }).count(), 0);
    await action(dialog, "Test").click();
    await paths(dialog, 1);
    assert.deepEqual((await runs(page)).at(-1).variables.subject, { pool: 1 });
    await action(dialog, "Close").click();
    await dialog.waitFor({ state: "detached" });
  } finally { await page.close(); }
});
