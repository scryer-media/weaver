import { nntpBodyMetrics } from "../../helpers";

export async function nntpBodyTransferCount(): Promise<number> {
  return (await nntpBodyMetrics()).body_transfers;
}
