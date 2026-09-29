# Weaver 0.14.2 release notes

Everything below is new since 0.14.1.

## Highlights

- **Editing a provider opens that provider's settings.** Switching between
  providers could show the previous provider's values and save them over the
  selected provider. Both interfaces now wait for details with the correct ID.
- **Extraction makes progress when decoder memory is tight.** Memory requests
  are served more fairly, 7z header reads reserve less memory, and direct
  unpack workers can release their decoders when another task needs the space.
- **News server address selection reacts sooner to measured throughput.**
  Weaver can judge delivery once a minute when it has enough evidence, instead
  of waiting for the ten-minute address race.
- **Portable updates no longer fail on the archive's root directory entry.**
  Linux and macOS portable packages now use a layout older installed updaters
  accept. The updater in this release also handles the root entry itself.

## What changed

### Provider settings and navigation

- **Provider forms keep their own values.** A previous query result cannot
  populate a different provider's editor. In the legacy interface, adding a
  provider after editing one also starts with a blank form.
- **Late responses stay with the editor that requested them.** A connection
  test cannot attach its result or certificate to a reopened editor. A save or
  removal that finishes later cannot close a newer provider editor.
- **Connection test results belong to the submitted settings.** Changing the
  form hides results from the previous values, including certificates offered
  for trust. Changing the host or port in the legacy interface also clears an
  adopted certificate.
- **Job navigation resets details and confirmations.** Opening another job
  drops the previous job's query results, live updates, and pending delete
  confirmation. A previous job's cancel or delete request finishing later
  cannot redirect the user away from the new job.
- **Reopened folder pickers ignore earlier requests.** A browse or folder
  creation started in the previous picker cannot replace the new picker's
  location when it finishes.

### Extraction and memory

- **Large memory requests no longer wait behind an endless stream of smaller
  ones.** Waiting requests are served in arrival order, with exceptions for
  decoder growth and retained state that would otherwise prevent progress.
- **Waiting for process memory no longer holds a job's task lock.** Other
  tasks belonging to that job can start or finish while a decoder waits.
- **7z listing reserves memory for the header work it will perform.** It no
  longer reserves the job's entire extraction allowance just to inspect an
  archive. A listing can use a smaller grant beside retained state and retry
  with a larger allowance when its header requires it, including Zstandard
  decoder windows.
- **Direct unpack yields memory only when it can help.** Requests target
  workers that actually hold decoder memory. Two workers waiting to grow
  their decoders can yield instead of waiting on each other indefinitely.
  Giving up a decoder under memory pressure is recorded as `memory_yielded`,
  rather than a decode failure; final extraction can retry from the archive.

### News server connections

- Delivery comparisons require both enough article samples and at least ten
  seconds of accumulated transfer time per address. A few tiny articles no
  longer provide enough evidence to change the preferred address.
- A faster address still needs two agreeing verdicts before it becomes the
  preferred address. Reconnects continue sampling a candidate until it meets
  both evidence requirements.
- Delivery verdicts no longer reset the address-race clock. The next eligible
  connection still races addresses after the ten-minute interval, allowing
  Weaver to discover changes to the provider's DNS answers.

### Updates and validation

- Portable tarballs name the binary explicitly instead of including a `./`
  root entry. Manifest generation rejects archive layouts that installed
  updaters would refuse, and the updated updater accepts the root directory
  entry in older layouts.
- The end-to-end harness recognizes a Docker CLI connected to a Podman
  engine and applies the Podman configuration. It also waits for published
  ports to become available before restarting a test stack.
- Browser tests in the release gate stop early enough to preserve failure
  reports and traces before the gate's deadline. Direct-unpack checks
  distinguish a worker yielding memory from an actual decoder failure.
- Browser regressions cover provider identity, stale responses, certificate
  handling, job navigation, and folder picker reuse in both interfaces.

## Upgrade notes

- There are no configuration, database, or GraphQL schema changes.
- If the provider-editing bug previously saved another provider's values,
  check the affected provider's connection settings. This release prevents
  the mix-up; it cannot reconstruct values that were already overwritten.
- The portable packaging fix allows older installed updaters to read the
  0.14.2 package without first receiving the updater code included in it.
