import { gql } from "urql";

/** An egress as every page reads it: its quota settings, and how much of the allowance is spent. */
const EGRESS_FIELDS = `id name bindingKind interfaceName sourceAddress addresses enabled maxDownloadSpeed health reason
  downloadQuota { enabled period limitBytes resetTimeMinutesLocal weeklyResetWeekday monthlyResetDay }
  downloadQuotaUsage { usedBytes reservedBytes remainingBytes blocked windowStartsAtEpochMs windowEndsAtEpochMs timezoneName }`;

/** Every egress's allowance and how much of it the current window has spent. */
export const EGRESS_QUOTAS_QUERY = gql`
  query EgressQuotas {
    egressInterfaces {
      id name
      downloadQuota { enabled period limitBytes resetTimeMinutesLocal weeklyResetWeekday monthlyResetDay }
      downloadQuotaUsage { usedBytes reservedBytes remainingBytes blocked windowStartsAtEpochMs windowEndsAtEpochMs timezoneName }
    }
  }
`;
export const NETWORKING_QUERY = gql`
  query Networking {
    egressInterfaces { ${EGRESS_FIELDS} }
    discoverNetworkInterfaces { name index up addresses }
    platformNetworking { platform egressBindingKinds sourceAddressHint container bridgeNetworkSuspected maxWireguardInstances notes }
    proxyProfiles { id name kind enabled host port }
    proxyPools { id name kind memberIds enabled }
    servers { id host connections active routing: route { legs { egressId weight path { kind directFallback rungs { kind proxyId poolId chainIds } } } failover } }
    rssFeeds { id name enabled routing: route { legs { egressId weight path { kind directFallback rungs { kind proxyId poolId chainIds } } } failover } }
  }
`;
const FLOW_FIELDS = `sampledAt consumers { key id name kind cap route { legs { egressId weight path { kind directFallback rungs { kind proxyId poolId chainIds } } } failover } } proxies { id name kind enabled host port } proxyPools { id name kind memberIds enabled } egresses { ${EGRESS_FIELDS} } legs { consumer position egressId weight target open opening state reason pinnedAddress sourceAddress bytesPerSecond selectedRung selectedProxyId rungStates failingHops { rung proxyId reason code params } path { kind directFallback rungs { kind proxyId poolId chainIds } } }
  pools { poolId egressId pinnedMember members { id state open opening warmed blocked connectMs bytesPerSecond liveBytesPerSecond samples failures } }`;
export const NETWORK_FLOW_QUERY = gql`query NetworkFlow { networkFlow { ${FLOW_FIELDS} } }`;
export const NETWORK_FLOW_SUBSCRIPTION = gql`subscription NetworkFlowUpdates { networkFlow { ${FLOW_FIELDS} } }`;
export const CREATE_EGRESS = gql`mutation CreateEgress($input:EgressInterfaceInput!) { createEgressInterface(input:$input) { id } }`;
export const UPDATE_EGRESS = gql`mutation UpdateEgress($id:Int!,$input:EgressInterfaceInput!) { updateEgressInterface(id:$id,input:$input) { id } }`;
export const DELETE_EGRESS = gql`mutation DeleteEgress($id:Int!) { deleteEgressInterface(id:$id) }`;
export const RESET_EGRESS_USAGE = gql`mutation ResetEgressUsage($id:Int!) { resetEgressDownloadQuotaUsage(id:$id) { id } }`;
export const CREATE_POOL = gql`mutation CreatePool($input:ProxyPoolInput!) { createProxyPool(input:$input) { id } }`;
export const UPDATE_POOL = gql`mutation UpdatePool($id:Int!,$input:ProxyPoolInput!) { updateProxyPool(id:$id,input:$input) { id } }`;
export const DELETE_POOL = gql`mutation DeletePool($id:Int!) { deleteProxyPool(id:$id) }`;
export const SAVE_ROUTE = gql`mutation SaveNetworkRoute($kind:NetworkConsumerKind!,$id:Int!,$input:RouteInput!) { saveNetworkRoute(kind:$kind,id:$id,input:$input) { consumer } }`;
/** Every path through a drafted route, with a proxy or each member of a pool on top. Nothing is saved. */
export const TEST_VIA = gql`mutation TestVia($route:RouteInput!,$subject:TestSubjectInput,$target:TestTargetInput) { testVia(route:$route,subject:$subject,target:$target) { leg egressId carrierIds proxyId success code message params failingProxyId sourceAddress connectMillis } }`;
const PATH_FIELDS = "kind directFallback rungs { kind proxyId poolId chainIds }";
/** A speed test run as every speed test screen reads it: the route it took and what it measured. */
const SPEED_TEST_RUN_FIELDS = `id consumer consumerName kind status phase startedAt finishedAt failure { code message params }
  request { bandwidth { providerId server customUrl } usenet { sabSize nzbUrl nzbFilename nzbRunId connections maxDurationSeconds } }
  tested { legs { position egressId weight down downReason target path { ${PATH_FIELDS} } } failover
    overrides { downLegs downRungs { leg rung } downProxies downEgresses } names { kind id name } }
  result { bytes elapsedMs averageBps peakBps
    legs { position bytes averageBps peakBps connections sourceAddresses }
    connections { leg connectMs path { egressId rung poolId memberId proxyIds } sourceAddress bytes bytesPerSecond error }
    usenet { articlesRequested delivered missing failed declaredBytes decodedBytes pipelining tls tlsCipherSuite }
    bandwidth { providerId server url connections }
    samples { atMs leg bytes decodedBytes } }`;
export const SPEED_TEST_QUERY = gql`query SpeedTest { bandwidthProviders { id name host needsServer } speedTestRuns { ${SPEED_TEST_RUN_FIELDS} } }`;
export const SPEED_TEST_RUN_QUERY = gql`query SpeedTestRun($id:Int!) { speedTestRun(id:$id) { ${SPEED_TEST_RUN_FIELDS} } }`;
export const BANDWIDTH_PROVIDER_SERVERS_QUERY = gql`query BandwidthProviderServers($providerId:String!,$kind:NetworkConsumerKind!,$id:Int!) { bandwidthProviderServers(providerId:$providerId,kind:$kind,id:$id) { id name location host } }`;
export const START_SPEED_TEST = gql`mutation StartSpeedTest($input:SpeedTestInput!) { startSpeedTest(input:$input) { ${SPEED_TEST_RUN_FIELDS} } }`;
export const CANCEL_SPEED_TEST = gql`mutation CancelSpeedTest($id:Int!) { cancelSpeedTest(id:$id) }`;
