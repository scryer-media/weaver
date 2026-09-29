import { NetworkingWorkspace } from "@/components/networking/NetworkingWorkspace";
import { ProxiesSettingsPage } from "./ProxiesSettingsPage";
import { BandwidthCapSettingsPage } from "./BandwidthCapSettingsPage";

export function NetworkingSettingsPage() {
  return <NetworkingWorkspace proxies={<ProxiesSettingsPage/>} bandwidth={<BandwidthCapSettingsPage/>}/>;
}
