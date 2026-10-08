import { NetworkingWorkspace } from "@/next/features/networking/NetworkingWorkspace";
import { ProxiesPanel } from "./ProxiesPanel";
import { BandwidthPanel } from "./BandwidthPanel";

export function NetworkingPanel() {
  return <NetworkingWorkspace proxies={<ProxiesPanel />} bandwidth={<BandwidthPanel />} />;
}
