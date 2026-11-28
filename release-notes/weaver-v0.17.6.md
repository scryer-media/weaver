# Weaver 0.17.6 release notes

Everything below is new since 0.17.5.

## Fixes

- **The screen links stay in view with many servers.** The left rail
  draws the live connection flow, and that drawing grows with the
  servers and routes on it. With many servers, or on a short window, it
  took the room the links to History, Monitoring and the settings pages
  scroll in, leaving one link or none in view. The drawing is now held
  to 30% of the rail and cut off below that. Picking it still shows all
  of it at full size.
- **A torrent's active and seeding times are not counted twice.** A
  save of a torrent's times that began just before a regular update,
  and reached the torrent just after it, could make the next update
  count the same span again. The time shown, and the time a seeding
  goal reads, could then run ahead until the next restart. A reading
  that comes late now counts nothing.

## Tests and build

- **The test that failed on the first 0.17.5 tag run.** It failed on
  the fault above, which depended on timing. The fix comes with a test
  that forces that order.
