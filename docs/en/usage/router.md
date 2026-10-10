# Router upstream configuration

`mineru-router` (also `mineru-kit router`) accepts a dedicated YAML file:

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
eligible after discovery succeeds. Removed upstreams drain: they receive no new
uploads or jobs, while existing resources remain reachable. Re-adding an ID at
the same URL reactivates it. Changing its URL requires a new ID, so old task IDs
cannot silently refer to another server.

`id` is optional; its default is a stable hash of the normalized URL. File order
does not affect identity. URLs and IDs must be unique across file entries and
repeatable `--upstream-url` options. CLI upstreams are retained on every reload.
An empty `upstreams: []` drains all file-configured upstreams. `SIGHUP` reloads
upstreams only; bind settings and managed GPU workers require a restart. Signal
handling requires a supported main-thread event loop (normally Linux/macOS).

Configured headers override the corresponding client headers for discovery,
regular requests, streaming downloads, and background polling. `api_key` is
shorthand for `Authorization: Bearer ...`; use either it or an explicit
Authorization header. Without configured credentials, the client Authorization
header is forwarded. Values are literal strings; this file does not perform
shell or environment expansion. Protect the configuration file as a credential.

`max_concurrent_jobs` describes upstream capacity for load-balancing weights; it
does not change that server's own concurrency limit. Upload affinity and existing
file ownership still take precedence over load balancing.

Backend credentials authenticate the router to upstreams. They do not authenticate
clients to the router: use an authenticating gateway when client authentication is
required. Caller resource scopes still derive from the incoming Authorization
header, independently of the configured upstream credential.

## Redis restart recovery

Install the optional Redis client and enable a namespace for one active router:

```bash
pip install 'mineru[router]'
export MINERU_ROUTER_REDIS_URL=redis://127.0.0.1:6379/0
mineru-router --host 0.0.0.0 --port 8002 \
  --local-gpus none --config router.yaml \
  --redis-namespace documents --source-dir /var/lib/mineru-router/sources
```

`--redis-url` can also supply the URL. Without a Redis URL, the router retains
its existing in-memory behavior. Redis mode requires independently managed
upstreams (`--local-gpus none`): router-managed workers and their own job indexes
would disappear with the router process.

The router checkpoints upload/file/job IDs, ownership scopes, cached resource
metadata, cross-worker aliases, pending cleanup, and upstream connection records
before returning responses. After restart it restores those mappings, rebuilds
active-job counts, and resumes background polling. Removed upstreams remain
available for old IDs, even if omitted from the new configuration. Present config
entries replace persisted credentials; stable IDs cannot be reused for another
URL. Existing upstream servers must retain their own jobs/files for recovery to
work.

A renewable 30-second Redis lease permits **one active router per namespace**.
A second router fails startup. Graceful shutdown releases the lease; after a crash,
wait for it to expire before starting the replacement. Lease ownership is checked
on requests and atomically on snapshot writes. Redis failures return `503` rather
than acknowledging unsaved changes. A router that loses lease renewal must be
restarted. This provides restart recovery, not multiple active replicas.

`--source-dir` preserves staged upload bytes on disk. Reuse the same directory
(or mount the same persistent volume) on restart to finish pending uploads and
copy existing inputs between upstreams. Without it, task polling, cancellation,
and output downloads can recover through Redis, but staged input bytes disappear;
operations needing those bytes return `409 source_file_unavailable` or
`409 upload_not_ready` and require a new upload. Separate namespaces must use
separate source directories. File bodies are never stored in Redis.

Snapshots contain upstream credentials and the client Authorization headers needed
for background polling. Use a private/authenticated Redis connection (including
`rediss://` when applicable), with persistence configured if recovery must also
survive a Redis restart. Snapshots have no automatic expiry. Each checkpoint
serializes the resource registry; snapshot cost grows with retained resources.

The upstream job creation and Redis write are separate operations. A crash between
upstream acceptance and the checkpoint can leave an unacknowledged upstream job;
this implementation does not promise exactly-once submission. Already acknowledged
mappings are the restart-recovery boundary.

Run the router tests with a disposable Redis database:

```bash
MINERU_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  pytest tests/unittest/test_router_config.py \
         tests/unittest/test_router_persistence.py \
         tests/unittest/test_v1_router.py
```

Redis tests use unique namespaces and delete only their own keys. They cover
normal restart, `SIGKILL` recovery, pending uploads, cross-worker cleanup,
lease renewal/takeover, and persistence failures, using simulated MinerU upstreams.

## Synchronous `/file_parse` compatibility

The router implements multipart `POST /file_parse` internally. It selects one
healthy, non-draining upstream with V1 file uploads and ZIP outputs, uploads the
inputs through `/v1/uploads`, creates a `/v1/parse/jobs` job, polls it, downloads
its result archives, and constructs the legacy response locally. The upstream
needs only the V1 API; its `/file_parse` endpoint is never called.

Routing uses the requested `tier`, or maps `backend` (`pipeline` to `basic`,
`hybrid-*`/`vlm-*` compatibility names to `standard`, `flash` to `flash`). With
neither field, the default preference is `standard`, then `basic`, then `flash`.
An explicit `tier` overrides the backend mapping. Concurrent calls participate
in the router's active-job load balancing.

```bash
curl http://127.0.0.1:8002/file_parse \
  -F 'files=@document.pdf' \
  -F 'tier=standard' \
  -F 'return_md=true' \
  -F 'return_middle_json=true'
```

The router validates legacy options, converts zero-based inclusive PDF ranges to
V1 page ranges, and preserves duplicate filename handling, JSON-string output
fields, image data URLs, and the ZIP layout. JSON and ZIP responses are assembled
from V1 result archives using the same conversion helpers as the local adapter.
`return_original_file` includes the original uploaded bytes in ZIP output.
Legacy `lang_list`, `formula_enable`, `table_enable`, and `image_analysis` remain
accepted but ignored. Per-request `server_url` and client-side output generation
remain unsupported; `/tasks` remains unavailable.

Incoming Authorization is used for all V1 operations unless configured upstream
credentials override it. Each V1 request keeps the normal bounded HTTP timeout;
the polling loop has no overall parsing deadline. Clients and gateways can apply
their own request deadlines. Disconnects observed while polling and request
cancellation trigger a best-effort cancellation of the upstream job.

The router cancels pending uploads and deletes its own input/output File records
on success or failure. Cleanup is bounded to ten seconds and preserves inputs if
job cancellation cannot be confirmed. The V1 API has no completed-job deletion
endpoint; completed job/upload metadata and physical blob retention follow the
upstream's own policy. A crash or lost submission response can leave private
upstream resources for that policy to reclaim.

These synchronous calls have no public router task ID and do not enter the Redis
resource registry. Their HTTP responses cannot be resumed after a router restart.
Use `/v1/parse/jobs` when restart-recoverable public task mappings are required.
