import { NetworkingWorkspace } from "@/next/features/networking/NetworkingWorkspace";
import { ProxiesPanel, ProxyEditorFor } from "./ProxiesPanel";
import { BandwidthPanel } from "./BandwidthPanel";

export function NetworkingPanel() {
  return (
    <NetworkingWorkspace
      proxies={<ProxiesPanel />}
      bandwidth={<BandwidthPanel />}
      proxyEditor={(id, onClose, onChanged) => (
        <ProxyEditorFor key={id} id={id} onClose={onClose} onChanged={onChanged} />
      )}
    />
  );
}
