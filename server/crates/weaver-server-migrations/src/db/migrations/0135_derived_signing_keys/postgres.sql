-- Login, CSRF and script-run tokens are now signed with keys derived from
-- the instance's encryption master key, so the stored signing secret is no
-- longer read. Every login signs in once more after this upgrade.
DELETE FROM settings WHERE key = 'auth.jwt_signing_secret';

-- Every browser session ends too: its CSRF token was made with the old key
-- and can never be made again, so the session could only fail. Verifications
-- reference their session, so they go first.
DELETE FROM browser_session_verifications;
DELETE FROM browser_sessions;
