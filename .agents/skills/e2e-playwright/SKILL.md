---
name: e2e-playwright
description: How to write and change Playwright e2e specs, fixtures, page helpers and locators in this repo. Load it before touching any spec, fixture, support helper or locator under the Playwright test tree. Covers durable-id locating (no copy, no CSS or DOM traversal), race-free waiting, deterministic time, fixture isolation, product-claim assertions, file structure and the pre-hand-back checks.
---

# Writing Playwright e2e specs

Read this before you write or change a spec, a fixture, a page helper or a locator. It applies to every file under the Playwright test tree.

A spec here is an acceptance oracle. It must fail only when the product is wrong. A spec that breaks when a label changes, a `div` moves or a runner is slow is a broken spec, and fixing it is part of your task.

Some public Playwright guides rank `getByRole` and `getByText` first. This repo does not. Copy changes often and is not the product's contract, so durable ids come first here.

In the examples, `ids` stands for the repo's shared id module and helper names such as `createServer` are placeholders. The repo section at the end names the real ones.

## 1. Locating elements

**Use durable ids first.** A durable id is a `data-testid` read with `getByTestId`, or a product-owned `#id` read with `locator("#id")`. Build ids from the shared id module, never from a string typed in the spec.

```ts
const tls = page.getByTestId(ids.field("tls"));
```

**Never locate by copy.** Do not find an element by the words it shows. That rules out `getByRole` with a `name`, `getByText`, `getByLabel`, `getByTitle`, `getByPlaceholder` and `getByAltText`. A rename from "TLS" to "SSL/TLS" must not break a spec.

```ts
// Don't
await page.getByRole("switch", { name: "TLS" }).click();
// Do
await page.getByTestId(ids.field("tls")).click();
```

**Never walk the DOM.** No CSS paths, no class names, no tag names, no `nth-child`, no `..`, no xpath, no `locator("main")` or `locator("body")`.

```ts
// Don't
page.locator("main form > div:nth-child(3) button");
// Do
page.getByTestId(ids.action("save"));
```

**Use a role only when the role is the claim.** `getByRole("dialog")` inside an id-scoped locator is fine when the test claims "a dialog opened". The role must not stand in for a missing id.

```ts
await expect(page.getByTestId(ids.section("servers")).getByRole("dialog")).toBeVisible();
```

**Key rows by product id.** A row is found by the record's id, never by its position or its text. `.first()`, `.last()`, `.nth()` and `all()[0]` are banned.

```ts
// Don't
page.getByRole("row").nth(2);
// Do
page.getByTestId(ids.row("feeds", feed.id));
```

**Text belongs only in assertions that certify product behavior.** Read it from an id-scoped locator, and mark the line with the repo's claim comment (see the repo section). A heading or a button label is not a product claim.

```ts
await expect(page.getByTestId(ids.field("status"))).toHaveText("Connected"); // copy-is-claim: connection state is the behavior under test
```

**If no id exists, add one to the product.** Do not reach for copy or CSS as a stopgap. When you cannot change the product in this task, stop and report the missing id.

## 2. Waiting

**Use web-first assertions.** `await expect(locator).toBeVisible()`, `toHaveText`, `toHaveValue`, `toBeChecked`, `toHaveCount` and `toHaveAttribute` retry until they pass. Always `await` them.

```ts
await expect(page.getByTestId(ids.row("jobs", job.id))).toHaveAttribute("data-state", "complete");
```

**Never wait for time.** No `page.waitForTimeout`, no `sleep`, no `setTimeout` promise, no loop that polls on a fixed delay.

```ts
// Don't
await page.waitForTimeout(2000);
```

**Never use a one-shot read as a wait.** `isVisible()`, `isChecked()`, `isEnabled()`, `textContent()` and `innerText()` return at once. They do not retry. Do not branch on them and do not pass their result to `expect`.

```ts
// Don't
expect(await row.isVisible()).toBe(true);
// Do
await expect(row).toBeVisible();
```

**Tie every wait to the item under test.** Wait on that item's id, on a watermark taken before the action, or on the named network operation. "Some row appeared" or "the count went up" can be satisfied by earlier or unrelated work.

```ts
// Don't
await expect(page.getByTestId(ids.list("jobs")).getByRole("row")).not.toHaveCount(0);
// Do
await expect(page.getByTestId(ids.row("jobs", job.id))).toBeVisible();
```

**Wait for the operation you caused.** Arm `page.waitForResponse` before the click. Match the path and the operation name parsed from the request body, not a substring of it.

```ts
function operation(page: Page, name: string) {
  return page.waitForResponse((response) => {
    if (!new URL(response.url()).pathname.endsWith("/graphql")) return false;
    const body = response.request().postDataJSON() as { operationName?: string; query?: string } | null;
    if (body?.operationName) return body.operationName === name;
    return new RegExp(`\\b(query|mutation|subscription)\\s+${name}\\b`).test(body?.query ?? "");
  });
}
const saved = operation(page, "UpdateServer");
await page.getByTestId(ids.action("save")).click();
await saved;
```

**Put no timeouts or deadlines in a test.** Do not pass `{ timeout }` to an assertion, and do not call `test.setTimeout` or `test.slow()` to make a spec pass. The runner config bounds every test. Never assert how long something took.

```ts
// Don't
await expect(row).toBeVisible({ timeout: 60_000 });
```

## 3. Determinism

A spec must give the same result on a slow, loaded, shared runner as on a fast idle workstation. Passing locally proves nothing.

**Wait for the condition the assertion needs.** A state change, an event, a row, a response. Never elapsed time.

**Do not assume an order the product does not promise.** Background workers, pollers, schedulers and timers race the test. Example: the product polls a new RSS feed at once. A spec that asserts "not polled yet" right after adding the feed races that poll and fails at random. Assert what the product guarantees, such as "the feed's last poll is at or after the moment it was added".

**Never assume a background job has or has not run.** If the outcome depends on it, trigger it from the test or wait for its own evidence by id.

**Control time through the harness clock.** Move the clock, then wait for the effect. Do not use real timers, and do not use `page.clock` against a server that keeps its own time.

```ts
advanceClock(60 * 60_000);
await expect(page.getByTestId(ids.row("schedules", schedule.id))).toHaveAttribute("data-state", "fired");
```

**Never fix a flake with a retry, a longer timeout or a sleep.** Find the race and remove it. If the product shows nothing you can wait on, report that gap and add the observable to the product.

## 4. Fixtures and isolation

**Use fixtures, not globals.** Put setup in `test.extend` fixtures. Avoid `beforeEach` that writes module-level variables.

```ts
const test = base.extend<{ server: Server }>({
  server: async ({ page }, use) => {
    const server = await createServer(page, uniqueName("server"));
    await use(server);
    await deleteServer(page, server.id);
  },
});
```

**Each test makes its own records.** Give them unique names, such as one built from `test.info().testId`. Clean up what you made. Never rely on records another test left behind.

**No order between tests.** Every test must pass alone, first, last or in parallel. One browser context per test, which is Playwright's default `page` fixture.

**No shared mutable module state.** No `let` at file scope that tests write.

**Serial mode needs a reason.** Use `test.describe.configure({ mode: "serial" })` only when the product state truly cannot be split, and say why in a comment on that line.

## 5. Assertions

**Assert the product claim, not the furniture.** Check the state the user cares about: a saved value, a job's status, a row that exists. Do not assert headings, captions, helper text or button labels.

```ts
// Don't
await expect(page.getByRole("heading", { name: "Servers" })).toBeVisible();
// Do
await expect(page.getByTestId(ids.field("tls"))).toBeChecked();
```

**One behavior per test.** A test named "saves TLS" checks that TLS was saved. Split a second behavior into a second test.

**Browser first, API second.** Drive the action and the main success signal through the UI. An API, GraphQL or database read is secondary evidence. Say so in a comment where you use it.

```ts
// Secondary evidence: the setting persisted server-side, not only in the form.
await expectSetting(request, "tls", true);
```

## 6. Structure

- Put page helpers per area in the support folder, one module per area.
- Take every id from the shared id module. No literal selectors in specs.
- Never copy a helper from one spec into another. Move it to support and import it.
- Keep the shared helpers file thin: fixtures and cross-area plumbing only.

```ts
// Don't, in a spec
const save = page.locator("#settings-servers-save");
// Do
const save = page.getByTestId(ids.action("save"));
```

## 7. Evidence and failure output

**Name the id and the expected state in the message.** A failure should say what was expected of which item.

```ts
await expect(row, `job ${job.id} should be complete`).toHaveAttribute("data-state", "complete");
```

**Keep the audits green.** Never add an exception to an audit to make your spec pass. Fix the spec.

**Run the static checks before hand-back.** Typecheck and the audits listed in the repo section. Do not run e2e flows unless the operator asks: the operator runs them.

## 8. Do and don't

| Do | Don't |
| --- | --- |
| `getByTestId(ids.field("tls"))` | `getByRole("switch", { name: "TLS" })` |
| `getByTestId(ids.row("feeds", feed.id))` | `getByRole("row").nth(2)` |
| `await expect(row).toBeVisible()` | `expect(await row.isVisible()).toBe(true)` |
| Wait on the named operation you caused | `page.waitForTimeout(2000)` |
| Move the harness clock, then wait for the effect | Real timers, `page.clock` on a server-timed feature |
| Fixture that creates and deletes its own record | `beforeEach` that fills a module `let` |
| Assert the saved value | Assert the page heading |
| Text assertion from an id-scoped locator, with the claim comment | `getByText("Connected")` anywhere |
| Find the race | Retry, longer timeout, sleep |

## 9. Checklist before hand-back

1. No new copy locator without the claim comment and a real product claim.
2. No CSS path, class, tag, xpath, `..` or positional locator.
3. Every id comes from the shared id module.
4. No `waitForTimeout`, `sleep`, fixed-delay poll, or `{ timeout }`.
5. No `isVisible()`-style read used as a wait or a branch.
6. Every wait names the item under test or the operation it caused.
7. No assumption about whether a background worker has run.
8. Each test makes and cleans up its own uniquely named records.
9. Each test passes alone and in any order.
10. Assertions check product behavior, with the id and state in the message.
11. API or database checks are marked as secondary evidence.
12. Typecheck and every audit in the repo section pass.

## Weaver specifics

The Playwright project is `e2e/playwright-weaver`. Specs are `tests/*.spec.ts`, shared plumbing is `tests/helpers.ts`, and area helpers live in `tests/support/`.

- **Ids.** Product ids live in `apps/weaver-web/src/next/testids/<area>.ts` and are mirrored for specs at `e2e/playwright-weaver/tests/support/testids/<area>.ts`. A pin test keeps the two equal. Shared controls take a `testId` prop, and the settings framework derives ids: `field-<field.id>`, `section-<block.id>`, `row-<block.id>-<row.id>`, `group-`, `add-`, `action-`, `empty-<block.id>`, plus `shell-footer`, `shell-status`, `shell-nav` and `nav-<key>`. Add a missing id in both modules, never as a literal in a spec.
- **Claim comment.** A copy locator or text assertion that certifies product behavior ends with `// copy-is-claim: <reason>`. The selector audit (`scripts/selector-quality.mjs`) rejects positional, CSS, traversal and copy locators without it.
- **Fixtures.** Import `test` and `expect` from `tests/helpers.ts`. Use the `cleanPage` fixture: it fails the test on console errors, page errors, failed requests, unexpected HTTP errors and GraphQL errors. Declare an error the test expects with `expectHttpErrors(page, ...)` before the action that causes it.
- **Secondary evidence.** `graphql(request, query)` and `expectSetting(request, field, expected)` in `tests/helpers.ts`. Mark each use as secondary evidence.
- **Clock.** `tests/support/e2e-clock.ts`: `setClock`, `advanceClock`, `readClock`. Weaver reads "now" from the clock file in e2e mode, so move it and then wait for the effect.
- **Named operations.** `operationResponse(page, op)` in `tests/ui-ingress-automation.spec.ts` waits on a GraphQL operation by a substring of the body. Prefer the exact operation-name match in section 2. When a second spec needs it, move it to `tests/support/` instead of copying it.
- **Older helpers.** `openNavigation` in `tests/helpers.ts` still finds the rail by copy and branches on `isVisible()`. Do not copy that pattern into new code.
- **Determinism.** The "Test determinism" section of the repo's `AGENTS.md` is binding. Section 3 above is the same rules in browser terms.
- **Before hand-back.** From `e2e/playwright-weaver`, run `npm run typecheck` and `npm run audit` (self-containment, browser boundary and selector quality). Do not run the flows; the operator runs them.

## Sources

Our own text. These were read while writing it and none was copied:

- Playwright docs, best practices, locators and auto-waiting: https://playwright.dev/docs/best-practices, https://playwright.dev/docs/locators (Apache-2.0, microsoft/playwright).
- Anthropic `webapp-testing` skill: https://github.com/anthropics/skills/tree/main/skills/webapp-testing (Apache-2.0).
- Currents `playwright-best-practices` skill: https://github.com/currents-dev/playwright-best-practices-skill (MIT).
- wshobson `e2e-testing-patterns` skill: https://github.com/wshobson/agents/tree/main/plugins/developer-essentials/skills/e2e-testing-patterns (MIT).
