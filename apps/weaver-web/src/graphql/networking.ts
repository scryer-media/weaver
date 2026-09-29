import { gql } from "urql";

export const NETWORKING_QUERY = gql`
  query Networking {
    egressInterfaces { id name bindingKind interfaceName sourceAddress addresses enabled maxDownloadSpeed health reason }
    discoverNetworkInterfaces { name index up addresses }
    platformNetworking { platform egressBindingKinds sourceAddressHint container bridgeNetworkSuspected maxWireguardInstances notes }
    proxyProfiles { id name kind enabled host port }
    proxyPools { id name kind memberIds enabled }
    servers { id host connections active routing: route { legs { egressId weight path { kind directFallback rungs { kind proxyId poolId chainIds } } } failover } }
    rssFeeds { id name enabled routing: route { legs { egressId weight path { kind directFallback rungs { kind proxyId poolId chainIds } } } failover } }
  }
`;
const FLOW_FIELDS = `sampledAt consumers { key id name kind cap route { legs { egressId weight path { kind directFallback rungs { kind proxyId poolId chainIds } } } failover } } proxies { id name kind enabled host port } proxyPools { id name kind memberIds enabled } egresses { id name bindingKind interfaceName sourceAddress addresses enabled maxDownloadSpeed health reason } legs { consumer position egressId weight target open opening state reason pinnedAddress sourceAddress bytesPerSecond selectedRung selectedProxyId rungStates path { kind directFallback rungs { kind proxyId poolId chainIds } } }
  pools { poolId egressId pinnedMember members { id state open opening warmed blocked handshakeMs connectMs bytesPerSecond samples failures } }`;
export const NETWORK_FLOW_QUERY = gql`query NetworkFlow { networkFlow { ${FLOW_FIELDS} } }`;
export const NETWORK_FLOW_SUBSCRIPTION = gql`subscription NetworkFlowUpdates { networkFlow { ${FLOW_FIELDS} } }`;
export const CREATE_EGRESS = gql`mutation CreateEgress($input:EgressInterfaceInput!) { createEgressInterface(input:$input) { id } }`;
export const UPDATE_EGRESS = gql`mutation UpdateEgress($id:Int!,$input:EgressInterfaceInput!) { updateEgressInterface(id:$id,input:$input) { id } }`;
export const DELETE_EGRESS = gql`mutation DeleteEgress($id:Int!) { deleteEgressInterface(id:$id) }`;
export const TEST_EGRESS = gql`mutation TestEgress($id:Int!,$proxyId:Int,$host:String!,$port:Int!) { testEgressInterface(id:$id,proxyId:$proxyId,host:$host,port:$port) { success message sourceAddress connectMillis } }`;
export const TEST_POOL = gql`mutation TestPool($id:Int!,$egressId:Int!,$host:String,$port:Int) { testProxyPool(id:$id,egressId:$egressId,host:$host,port:$port) { proxyId success message sourceAddress connectMillis } }`;
export const CREATE_POOL = gql`mutation CreatePool($input:ProxyPoolInput!) { createProxyPool(input:$input) { id } }`;
export const UPDATE_POOL = gql`mutation UpdatePool($id:Int!,$input:ProxyPoolInput!) { updateProxyPool(id:$id,input:$input) { id } }`;
export const DELETE_POOL = gql`mutation DeletePool($id:Int!) { deleteProxyPool(id:$id) }`;
export const SAVE_ROUTE = gql`mutation SaveNetworkRoute($kind:NetworkConsumerKind!,$id:Int!,$input:RouteInput!) { saveNetworkRoute(kind:$kind,id:$id,input:$input) { consumer } }`;
