import http from "node:http";

const port = Number(process.env.RSS_FIXTURE_PORT ?? 8089);
const feed = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <title>Weaver E2E RSS fixture</title>
  <item>
    <title>Weaver E2E RSS Behavior Probe</title>
    <guid isPermaLink="false">weaver-e2e-rss-behavior-probe</guid>
    <link>http://rss-fixture:8089/rss-probe.nzb</link>
    <pubDate>Fri, 14 Mar 2032 08:00:00 GMT</pubDate>
    <enclosure url="http://rss-fixture:8089/rss-probe.nzb" length="1" type="application/x-nzb" />
  </item>
</channel></rss>`;
const nzb = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="weaver-e2e" date="1700000000" subject="rss-behavior.bin">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="1" number="1">rss-behavior@e2e.invalid</segment></segments>
  </file>
</nzb>`;

// Counted feeds for the scheduling flow: every GET of /counted/<key>.xml is
// tallied, and /count/<key> reports the tally, so a spec can tell how many
// times a scheduled fetch reached this feed. The feed itself is empty.
const counts = new Map();
const countedFeed = (key) => `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Weaver E2E counted feed ${key}</title></channel></rss>`;

http
  .createServer((request, response) => {
    const counted = /^\/counted\/([A-Za-z0-9._-]+)\.xml$/.exec(request.url ?? "");
    if (counted) {
      counts.set(counted[1], (counts.get(counted[1]) ?? 0) + 1);
      response.writeHead(200, { "content-type": "application/rss+xml" });
      response.end(countedFeed(counted[1]));
      return;
    }
    const tally = /^\/count\/([A-Za-z0-9._-]+)$/.exec(request.url ?? "");
    if (tally) {
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ count: counts.get(tally[1]) ?? 0 }));
      return;
    }
    if (request.url === "/feed.xml") {
      response.writeHead(200, { "content-type": "application/rss+xml" });
      response.end(feed);
      return;
    }
    if (request.url === "/rss-probe.nzb") {
      response.writeHead(200, { "content-type": "application/x-nzb" });
      response.end(nzb);
      return;
    }
    response.writeHead(404).end();
  })
  .listen(port, "0.0.0.0", () => {
    console.log(`rss fixture listening on ${port}`);
  });
