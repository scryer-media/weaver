import { gql } from "urql";
export const PROXY_PROFILES_QUERY = gql`
  query ProxyProfiles { proxyProfiles {
    id name kind enabled host port dnsServers tunnelAddresses peerPublicKey tunnelPublicKey
    mtu keepaliveSeconds timeoutSeconds hostKeyFingerprint
    hasUsername hasPassword hasPrivateKey hasPassphrase hasPresharedKey
  } }
`;
export const SAVE_PROXY_MUTATION = gql`mutation SaveProxyProfile($id: Int, $input: ProxyProfileInput!) { saveProxyProfile(id: $id, input: $input) { id } }`;
export const DELETE_PROXY_MUTATION = gql`mutation DeleteProxyProfile($id: Int!) { deleteProxyProfile(id: $id) }`;
export const RESET_PROXY_TRUST_MUTATION = gql`mutation ResetProxyHostKey($id: Int!) { resetProxyHostKey(id: $id) { id hostKeyFingerprint } }`;
export const TEST_PROXY_MUTATION = gql`mutation TestProxyProfile($id: Int!) { testProxyProfile(id: $id) { success message } }`;
