// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

import { gql } from "urql";

const SERVER_FIELDS = "wire address tlsName url";
const PROBE_FIELDS = "supported millis error { code detail }";

/**
 * The DNS page: every saved server list, and each kind's profiles with what
 * its table and editor show. A proxy name profile has no transit and no
 * system fallback, and is counted by the proxies that use it, a route
 * profile by its routes.
 */
export const DNS_QUERY = gql`
  query Dns {
    dnsServerLists { id name servers { ${SERVER_FIELDS} } usedByProfiles }
    nameProfiles: dnsProfiles(kind: PROXY_NAME) { id name builtIn who trust serverListId usedByProxies }
    routeProfiles: dnsProfiles(kind: ROUTE) { id name builtIn who transit trust serverListId systemFallback usedByRoutes }
  }
`;
/** What a route's DNS select offers, and what its in-form check needs. */
export const DNS_PROFILE_CHOICES_QUERY = gql`query DnsProfileChoices { dnsProfiles(kind: ROUTE) { id name who trust systemFallback } }`;
/** What a proxy's "Find this proxy with" select and its "Found with" line show. */
export const DNS_PROFILE_NAMES_QUERY = gql`query DnsProfileNames { dnsProfiles(kind: PROXY_NAME) { id name } }`;
export const SAVE_DNS_SERVER_LIST = gql`mutation SaveDnsServerList($input:DnsServerListInput!) { saveDnsServerList(input:$input) { id } }`;
export const DELETE_DNS_SERVER_LIST = gql`mutation DeleteDnsServerList($id:Int!) { deleteDnsServerList(id:$id) }`;
export const SAVE_DNS_PROFILE = gql`mutation SaveDnsProfile($input:DnsProfileInput!) { saveDnsProfile(input:$input) { id } }`;
export const DELETE_DNS_PROFILE = gql`mutation DeleteDnsProfile($id:Int!) { deleteDnsProfile(id:$id) }`;
/** The names of the routes and proxies a refusal names by key and id: the flow's consumers, or the servers and feeds before it has any. */
export const ROUTE_NAMES_QUERY = gql`query RouteNames { networkFlow { consumers { key name } } servers { id host } rssFeeds { id name } proxyProfiles { id name } }`;
/** What each server supports, probed through a drafted route. Nothing is saved. */
export const TEST_DNS_SERVERS = gql`mutation TestDnsServers($route:RouteInput!,$servers:[DnsServerInput!]!) {
  testDnsServers(route:$route,servers:$servers) {
    index
    plain { ${PROBE_FIELDS} } tls { ${PROBE_FIELDS} } https { ${PROBE_FIELDS} } dnssec { ${PROBE_FIELDS} }
  } }`;
