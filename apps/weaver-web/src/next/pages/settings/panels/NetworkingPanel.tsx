import { NetworkingWorkspace } from "@/next/features/networking/NetworkingWorkspace";
import { ProxiesPanel, ProxyEditorFor } from "./ProxiesPanel";

export function NetworkingPanel() {
  return (
    <NetworkingWorkspace
      proxies={(onChanged) => <ProxiesPanel onChanged={onChanged} />}
      proxyEditor={(id, onClose, onChanged) => (
        <ProxyEditorFor key={id} id={id} onClose={onClose} onChanged={onChanged} />
      )}
    />
  );
}
