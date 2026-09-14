# Queue progress browser regression

Run `npm run test:browser` with an existing Playwright installation and Chromium.
If Playwright is installed outside this workspace, set `PLAYWRIGHT_MODULE_PATH`
to its absolute module entry point. No dependency installation is performed by
the test.

The suite starts and closes its own local Vite server. Queue data and event
bursts are synthetic; application API requests are blocked. It renders the real
queue in React Strict Mode and verifies concurrent moving/download updates and
pixel-stable progress tracks across column/sidebar breakpoints, changing rates,
and one-, two-, and three-digit percentages. A nine-job fixture checks that an
initially blank download phase and seven moving jobs keep advancing through
repeated bursts in alternating arrival orders without page refreshes.

For a before/after comparison, `QUEUE_PROGRESS_BASELINE` can point to a saved
earlier `JobList.tsx`. The test server substitutes only that component; it does
not modify the working tree.
