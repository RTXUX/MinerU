# Router Upstream Configuration

`mineru-router` (also `mineru-kit router`) accepts a dedicated YAML file for remote upstreams:

```yaml
upstreams:
  - id: gpu-a
    url: http://mineru-a:8000
    api_key: backend-a-secret
    max_concurrent_jobs: 2
  - id: gpu-b
    url: https://mineru-b.example.com
    headers:
      Authorization: Bearer backend-b-secret
    max_concurrent_jobs: 1
```

```bash
mineru-router --host 0.0.0.0 --port 8002 --local-gpus none --config router.yaml
kill -HUP <router-pid>
```

Write the replacement configuration atomically, then send `SIGHUP` to the serving
process. The router validates the complete file before applying changes. Invalid
files leave the previous configuration active. Newly added upstreams become
eligible after discovery succeeds. Removed file-configured upstreams drain: they
receive no new task reservations, while existing tasks, streams, result leases,
and files remain reachable. Re-adding an ID at the same URL reactivates it.
Changing its URL requires a new ID, so existing task and file routes cannot
silently refer to another server.

`id` is optional; its default is a stable hash of the normalized URL. File order
does not affect identity. URLs and IDs must be unique across file entries,
repeatable `--upstream-url` options, and dynamically registered workers. CLI
`--upstream-url` entries and [Admin API](tasks.md#dynamic-router-upstreams-and-cascading)
workers (`/v1/router/workers`) remain untouched across `SIGHUP` reloads. An empty
`upstreams: []` drains all file-configured upstreams. `SIGHUP` reloads YAML
upstreams only; bind settings and managed GPU workers require a restart. Signal
handling requires a supported main-thread event loop (normally Linux/macOS).

Configured headers override the corresponding client headers for capability
discovery, `/v1/tasks` operations, per-file uploads, streaming downloads, and
background polling/lease renewal. `api_key` is shorthand for
`Authorization: Bearer ...`; use either it or an explicit `Authorization` header.
Without configured credentials, the client `Authorization` header is forwarded.
Values are literal strings; this file does not perform shell or environment
expansion. Protect the configuration file as a credential.

`max_concurrent_jobs` describes upstream capacity for load-balancing weights and
`/v1/usage` aggregation; it does not change that server's own concurrency limit.

Backend credentials authenticate the router to upstreams. They do not authenticate
clients to the router: use an authenticating gateway when client authentication is
required. Caller resource scopes still derive from the incoming `Authorization`
header, independently of any configured upstream credential.
