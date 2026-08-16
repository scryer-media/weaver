import type { APIRequestContext } from "@playwright/test";

export async function registerRssProbeRelease(
  request: APIRequestContext,
  release: {
    guid: string;
    title: string;
    nzbXml: string;
  },
): Promise<void> {
  const response = await request.post("http://newznab:8088/admin/releases", {
    data: {
      guid: release.guid,
      title: release.title,
      nzb_xml: Buffer.from(release.nzbXml).toString("base64"),
      size_bytes: 1,
      pub_date: "2032-03-14T08:00:00Z",
      attributes: {
        category: "5000",
      },
    },
  });
  if (!response.ok()) {
    throw new Error(
      `register fake RSS release failed (${response.status()}): ${await response.text()}`,
    );
  }
}
