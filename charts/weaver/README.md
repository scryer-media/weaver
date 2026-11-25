# Weaver Helm chart

Runs [Weaver](https://github.com/scryer-media/weaver), the usenet and torrent
downloader, as one pod.

## Install

Not yet published. Once it is:

```sh
helm install weaver oci://ghcr.io/scryer-media/charts/weaver \
  --namespace media --create-namespace
```

From a checkout:

```sh
helm install weaver ./charts/weaver --namespace media --create-namespace
```

Requires Kubernetes 1.27 or later. The pod meets the Pod Security
`restricted` profile: user 1000, no capabilities, read-only root filesystem.

## First start

Open the UI:

```sh
kubectl -n media port-forward svc/weaver 9090
# then http://localhost:9090/
```

The setup page asks for a one-time code. Weaver prints it in its log:

```sh
kubectl -n media logs deploy/weaver | grep "one-time setup code"
```

To skip setup, give a login in a Secret (see [Secrets](#secrets)).

## Storage

| Mount        | Holds                                         | Value                   |
| ------------ | --------------------------------------------- | ----------------------- |
| `/config`    | database, encryption key file, Weaver's state | `persistence.config`    |
| `/downloads` | `intermediate/` and `complete/` folders       | `persistence.downloads` |

`/config` is a 1Gi claim the chart makes, or your own with `existingClaim`.

`/downloads` holds both folders on one volume, so a finished job moves by
rename instead of a copy. Pick its type:

```yaml
persistence:
  downloads:
    type: pvc            # or existingClaim, hostPath, nfs, emptyDir
    size: 500Gi
```

The default is `emptyDir`: downloads are lost when the pod goes.

For `hostPath`, the folder must already exist on the node and be writable by
user 1000. Pod Security `restricted` and `baseline` both refuse hostPath
volumes, so the namespace must enforce `privileged`; otherwise no pod is
created. Prefer `existingClaim` with a local PersistentVolume.

A claim made with `type: pvc` belongs to the release and is deleted on
uninstall, downloads included. Use `existingClaim` to keep them.

The chart sets `WEAVER_INTERMEDIATE_DIR=/downloads/intermediate` and
`WEAVER_COMPLETE_DIR=/downloads/complete`. These seed a new database only;
folders saved in Weaver's settings win.

## Host allowlist

Weaver answers only requests whose `Host` it knows; others get
`421 Misdirected Request`. The chart sets `WEAVER_HTTP_ALLOWED_HOSTS` to:

- the Service's names (`weaver`, `weaver.media`, `weaver.media.svc`,
  `weaver.media.svc.cluster.local`);
- every Ingress host and HTTPRoute hostname;
- `extraAllowedHosts`.

IP addresses and `localhost` are always allowed, so kubelet probes,
`port-forward` and Prometheus scrapes work. Entries are exact names; Weaver
takes no wildcards.

A reverse proxy outside the cluster that sends its own name:

```yaml
extraAllowedHosts:
  - downloads.example.com
```

## Ingress and Gateway API

```yaml
ingress:
  enabled: true
  className: nginx
  hosts: [weaver.example.com]
  tls:
    - secretName: weaver-tls
      hosts: [weaver.example.com]
```

```yaml
httpRoute:
  enabled: true
  parentRefs:
    - name: gateway
      namespace: gateway-system
  hostnames: [weaver.example.com]
```

To serve under a path, set `basePath: /weaver`. The chart passes it to
`--base-url`, and the probes, routes and test follow it.

## Torrents

```yaml
torrents:
  enabled: true
  port: 6881
  service:
    type: LoadBalancer   # or NodePort
```

This sets `WEAVER_TORRENTS=1`, opens the port on the pod (TCP and UDP) and
adds the Service `weaver-torrent`.

Then, in Weaver's Settings, turn torrents on and set the **listen port** to
`torrents.port`. Weaver picks a random listen port on first start, and no
environment variable sets it.

With `NodePort`, set `nodePort` and set Weaver's **announce port** to it, so
trackers hand out the port peers can reach.

## Secrets

The chart never makes a Secret. It reads these:

```sh
# Encryption key: 32 random bytes, base64.
kubectl -n media create secret generic weaver-encryption \
  --from-literal=encryption-key="$(openssl rand -base64 32)"

# First login. Used only while no login is stored.
kubectl -n media create secret generic weaver-bootstrap \
  --from-literal=username=admin --from-file=password=./password.txt
```

```yaml
encryptionKey:
  existingSecret: weaver-encryption
bootstrapLogin:
  existingSecret: weaver-bootstrap
database:
  existingSecret: weaver-database   # key database-url, a postgres:// URL
```

Without `encryptionKey`, Weaver makes `/config/encryption.key` on first start
and keeps using it. Back up `/config` with it. If the key is lost while
credentials are stored, Weaver refuses to start rather than make a new one.

Without `database`, Weaver uses SQLite at `/config/weaver.db`.

`env`, `extraEnv` and `envFrom` pass other settings through.

## Metrics

`/metrics` needs an API key with read access by default. Make one in Weaver,
store it in a Secret, and point the ServiceMonitor at it:

```yaml
metrics:
  serviceMonitor:
    enabled: true
    apiKeySecret:
      name: weaver-metrics
      key: api-key
```

## Upgrade and uninstall

```sh
helm upgrade weaver oci://ghcr.io/scryer-media/charts/weaver -n media
```

The old pod stops before the new one starts. Weaver does not upgrade itself in
the pod; the chart's image tag decides the version.

```sh
helm uninstall weaver -n media
```

The config claim `weaver-config` is kept, with the database in it. Reinstall
onto it with `persistence.config.existingClaim: weaver-config`, or delete it:

```sh
kubectl -n media delete pvc weaver-config
```

Set `persistence.config.retain: false` to remove it with the release.

## Values

| Value                                      | Default                       | Meaning                                       |
| ------------------------------------------ | ----------------------------- | --------------------------------------------- |
| `image.repository`                         | `ghcr.io/scryer-media/weaver` | Image                                         |
| `image.tag`                                | chart `appVersion`            | Image tag                                     |
| `image.digest`                             | `""`                          | `sha256:...`; wins over the tag               |
| `image.pullPolicy`                         | `IfNotPresent`                |                                               |
| `imagePullSecrets`                         | `[]`                          |                                               |
| `securityContext.runAsUser`                | `1000`                        | User                                          |
| `securityContext.runAsGroup`               | `1000`                        | Group                                         |
| `securityContext.fsGroup`                  | `1000`                        | Group that owns the volumes                   |
| `terminationGracePeriodSeconds`            | `60`                          | Time to stop after SIGTERM                    |
| `timezone`                                 | `Etc/UTC`                     | `TZ`                                          |
| `basePath`                                 | `""`                          | Serve under a path, e.g. `/weaver`            |
| `extraAllowedHosts`                        | `[]`                          | More allowed `Host` names                     |
| `clusterDomain`                            | `cluster.local`               | For the Service's full name                   |
| `service.type`                             | `ClusterIP`                   |                                               |
| `service.port`                             | `9090`                        |                                               |
| `service.annotations`                      | `{}`                          |                                               |
| `ingress.enabled`                          | `false`                       |                                               |
| `ingress.className`                        | `""`                          |                                               |
| `ingress.annotations`                      | `{}`                          |                                               |
| `ingress.hosts`                            | `[]`                          | Host names                                    |
| `ingress.tls`                              | `[]`                          | Ingress TLS entries                           |
| `httpRoute.enabled`                        | `false`                       |                                               |
| `httpRoute.annotations`                    | `{}`                          |                                               |
| `httpRoute.parentRefs`                     | `[]`                          | Gateways                                      |
| `httpRoute.hostnames`                      | `[]`                          |                                               |
| `persistence.config.existingClaim`         | `""`                          | Use this claim for `/config`                  |
| `persistence.config.storageClass`          | `""`                          | `""` default class, `-` none                  |
| `persistence.config.accessModes`           | `[ReadWriteOnce]`             |                                               |
| `persistence.config.size`                  | `1Gi`                         |                                               |
| `persistence.config.retain`                | `true`                        | Keep the claim on uninstall                   |
| `persistence.downloads.type`               | `emptyDir`                    | `emptyDir`, `pvc`, `existingClaim`, `hostPath`, `nfs` |
| `persistence.downloads.existingClaim`      | `""`                          | For `existingClaim`                           |
| `persistence.downloads.storageClass`       | `""`                          | For `pvc`                                     |
| `persistence.downloads.accessModes`        | `[ReadWriteOnce]`             | For `pvc`                                     |
| `persistence.downloads.size`               | `100Gi`                       | For `pvc`                                     |
| `persistence.downloads.hostPath`           | `""`                          | For `hostPath`; must exist                    |
| `persistence.downloads.nfs.server`         | `""`                          | For `nfs`                                     |
| `persistence.downloads.nfs.path`           | `""`                          | For `nfs`                                     |
| `extraVolumes`                             | `[]`                          | Passed through                                |
| `extraVolumeMounts`                        | `[]`                          | Passed through                                |
| `torrents.enabled`                         | `false`                       | Make torrents available                       |
| `torrents.port`                            | `6881`                        | Must equal Weaver's listen port               |
| `torrents.service.type`                    | `LoadBalancer`                | `LoadBalancer` or `NodePort`                  |
| `torrents.service.nodePort`                | `null`                        | For `NodePort`                                |
| `torrents.service.annotations`             | `{}`                          |                                               |
| `encryptionKey.existingSecret`             | `""`                          | Secret with the encryption key                |
| `encryptionKey.key`                        | `encryption-key`              |                                               |
| `bootstrapLogin.existingSecret`            | `""`                          | Secret with the first login                   |
| `bootstrapLogin.usernameKey`               | `username`                    |                                               |
| `bootstrapLogin.passwordKey`               | `password`                    | Mounted as a file                             |
| `database.existingSecret`                  | `""`                          | Secret with a Postgres URL                    |
| `database.key`                             | `database-url`                |                                               |
| `metrics.serviceMonitor.enabled`           | `false`                       |                                               |
| `metrics.serviceMonitor.interval`          | `""`                          | Prometheus default when empty                 |
| `metrics.serviceMonitor.labels`            | `{}`                          |                                               |
| `metrics.serviceMonitor.apiKeySecret.name` | `""`                          | Secret with a read API key                    |
| `metrics.serviceMonitor.apiKeySecret.key`  | `api-key`                     |                                               |
| `env`                                      | `{}`                          | `NAME: value`                                 |
| `extraEnv`                                 | `[]`                          | Env entries, passed through                   |
| `envFrom`                                  | `[]`                          | Passed through                                |
| `serviceAccount.create`                    | `true`                        | Its token is never mounted                    |
| `serviceAccount.name`                      | `""`                          |                                               |
| `serviceAccount.annotations`               | `{}`                          |                                               |
| `podLabels`, `podAnnotations`              | `{}`                          |                                               |
| `commonLabels`, `commonAnnotations`        | `{}`                          | On every object                               |
| `nameOverride`, `fullnameOverride`         | `""`                          |                                               |
| `resources`                                | `{}`                          |                                               |
| `nodeSelector`, `affinity`                 | `{}`                          |                                               |
| `tolerations`                              | `[]`                          |                                               |
| `priorityClassName`                        | `""`                          |                                               |

## License

GPL-3.0-or-later.
