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

## Redis restart recovery

Install the optional Redis client and enable an isolated namespace for one active
router:

```bash
pip install 'mineru[router]'
export MINERU_ROUTER_REDIS_URL=redis://127.0.0.1:6379/0
mineru-router --host 0.0.0.0 --port 8002 \
  --local-gpus none --config router.yaml \
  --redis-namespace documents --source-dir /var/lib/mineru-router/sources
```

`--redis-url` can also supply the URL (`MINERU_ROUTER_REDIS_NAMESPACE` and
`MINERU_ROUTER_SOURCE_DIR` correspond to `--redis-namespace` and `--source-dir`).
Without a Redis URL, the router retains its default in-memory behavior. Redis mode
requires independently managed upstreams (`--local-gpus none`): router-managed
workers and their own task indexes would disappear with the router process.

The router checkpoints `/v1/tasks` records, per-file states, `/v1/files` routes,
caller ownership scopes, result leases, cumulative `/v1/usage` counters, and
remote upstream records (including dynamically registered `/v1/router/workers`
nodes) before returning HTTP responses and as background submissions progress.
After restart it restores those mappings, rebuilds active-job counts, and resumes
background task polling and result-lease renewal. Removed CLI or YAML upstreams
remain available in `draining` state for existing task and file IDs; present YAML
entries replace persisted credentials, while stable IDs cannot be reused for a
different URL. Dynamically registered Admin API upstreams retain their `active` or
`draining` state across restarts. Upstream servers must retain their own tasks and
files for recovery to succeed.

A renewable 30-second Redis lease permits **one active router per namespace**.
A second router fails startup. Graceful shutdown preserves remote in-flight tasks
and releases the Redis lease immediately; after an unclean crash, wait for the
lease to expire before starting the replacement. Lease ownership is checked before
accepting requests or background upstream submissions, and atomically on snapshot
writes. Redis failures or lease loss return `503 router_state_unavailable` rather
than acknowledging unsaved changes. A router that loses lease renewal must be
restarted. This provides single-router restart recovery, not active-active
replicas.

`--source-dir` preserves staged upload bytes and caller-scoped SHA-256 upload
caches on disk. Reuse the same directory (or mount the same persistent volume) on
restart so staged uploads not yet handed off to an upstream can finish and cached
hashes remain reusable. Without `--source-dir`, waiting client uploads, already
submitted tasks, cancellation, and result downloads still recover through Redis,
but unsubmitted staged upload bytes disappear and fail that file with
`source_file_unavailable`. Separate namespaces must use separate source
directories. File bodies are never stored in Redis.

Snapshots contain configured upstream credentials and the client headers needed
for background polling and lease renewal. Use a private, authenticated Redis
connection (including `rediss://` when applicable), with Redis persistence
configured if recovery must also survive a Redis restart. Snapshots have no
automatic expiry; snapshot size grows with retained tasks and files until
retention collection reclaims them.

Upstream task creation and the Redis write are separate network operations. A
crash between upstream acceptance and the checkpoint can leave an unacknowledged
upstream task to expire under the worker's retention policy; this implementation
does not promise exactly-once upstream submission. Already acknowledged mappings
form the restart-recovery boundary.

Run the router configuration and persistence test suite (optionally pointing
`MINERU_TEST_REDIS_URL` at a disposable Redis database to include multi-process
`SIGKILL` recovery):

```bash
MINERU_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  pytest tests/unittest/test_router_config.py \
         tests/unittest/test_router_persistence.py \
         tests/unittest/test_router_admin.py
```
