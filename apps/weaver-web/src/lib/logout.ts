/**
 * Sign this browser out of the Weaver that served the interface.
 *
 * The request and the page it lands on both resolve against the document's
 * base URL, so an install mounted below a path signs itself out instead of
 * posting to whatever answers at the host root. The page only moves once the
 * server confirms the sign-out; otherwise the login cookie may still be valid,
 * and the caller keeps the signed-in page and reports the failure.
 */
export async function signOut(
  baseURI: string,
  request: typeof fetch = fetch,
  navigate: (href: string) => void = (href) => window.location.assign(href),
): Promise<void> {
  const response = await request(new URL("api/logout", baseURI).href, { method: "POST" });
  if (!response.ok) {
    throw new Error(`Sign out failed (HTTP ${response.status})`);
  }
  navigate(new URL(".", baseURI).href);
}
