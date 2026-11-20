# Router 上游配置

`mineru-router`（等价于 `mineru-kit router`）支持通过独立的 YAML 文件声明远程 upstream：

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

请以原子方式替换配置文件后再向服务进程发送 `SIGHUP`。Router 会在变更生效前完整校验整个文件；非法配置不会生效，上一份有效配置保持不变。新增 upstream 在能力探测成功后参与调度；从配置文件移除的节点进入 draining：不再接受新的任务预占，但已有任务、流式传输、结果租约和文件下载继续可用。以相同 URL 重新加入原 `id` 会恢复接单；更换 URL 必须使用新的 `id`，避免已有任务和文件路由静默指向其他服务。

`id` 可选，默认取规范化 URL 的稳定哈希，与条目顺序无关。配置文件、`--upstream-url` 参数和[动态管理接口](tasks.md#upstream)（`/v1/router/workers`）之间的 URL 与 `id` 必须全局唯一。`SIGHUP` 只重载 YAML 管理的条目，通过 `--upstream-url` 或动态管理接口注册的节点保持不变。空的 `upstreams: []` 会将所有 YAML 配置的节点置为 draining。监听地址与本地托管 GPU worker 变更仍需重启进程；`SIGHUP` 处理依赖受支持的主线程事件循环（通常为 Linux/macOS）。

配置的请求头会在能力探测、`/v1/tasks` 请求、逐文件上传、流式下载以及后台轮询与结果续租中覆盖同名客户端请求头。`api_key` 等价于 `Authorization: Bearer ...`，不能与显式 `Authorization` 头同时声明；未配置后端凭证时继续转发客户端 `Authorization` 头。配置值按字面量读取，不做环境变量或 shell 展开，请按敏感凭证保护该文件。

`max_concurrent_jobs` 用于 Router 负载均衡权重计算和 `/v1/usage` 并发汇总，不会修改对应服务的实际并发上限。

后端凭证仅用于 Router 向上游认证，不负责对访问 Router 的客户端鉴权；需要客户端鉴权时请前置认证网关。调用方的资源隔离范围仍由传入的客户端 `Authorization` 头独立派生。

## Redis 重启恢复 {#redis-restart-recovery}

安装可选 Redis 客户端，并为单个活动 Router 指定独立命名空间：

```bash
pip install 'mineru[router]'
export MINERU_ROUTER_REDIS_URL=redis://127.0.0.1:6379/0
mineru-router --host 0.0.0.0 --port 8002 \
  --local-gpus none --config router.yaml \
  --redis-namespace documents --source-dir /var/lib/mineru-router/sources
```

也可通过 `--redis-url` 传入连接地址（`MINERU_ROUTER_REDIS_NAMESPACE` 与 `MINERU_ROUTER_SOURCE_DIR` 分别对应 `--redis-namespace` 与 `--source-dir`）。未配置 Redis URL 时，Router 保持默认的纯内存行为。Redis 模式要求使用独立维护的远程 upstream（`--local-gpus none`），因为 Router 托管的本地子进程及其内部任务索引会随 Router 退出而消失。

Router 会在返回 HTTP 响应前及后台提交推进时，原子保存 `/v1/tasks` 任务记录、逐文件状态、`/v1/files` 映射、调用方隔离 scope、结果租约、`/v1/usage` 累计统计以及远程 upstream 记录（包括通过 `/v1/router/workers` 动态注册的节点）。重启后会恢复这些映射、重建 `active_jobs` 负载计数，并恢复后台任务轮询与结果续租。已从 CLI 或 YAML 中移除的旧节点以 `draining` 状态保留以供旧任务和文件继续访问；当前 YAML 中的同名条目会更新持久化的认证凭证，而同一 `id` 不能改绑到其他 URL。通过管理接口动态注册的节点跨重启保持原有的 `active` 或 `draining` 状态。完整恢复要求上游服务自身仍保留对应的任务和产物。

基于 30 秒自动续期的 Redis 租约保证**每个命名空间同一时刻只允许一个活动 Router**。第二个实例启动会直接报错退出。优雅关闭会保留下游在途任务并立即释放 Redis 租约；进程异常崩溃后，需等待原租约过期再拉起替代实例。每次处理请求、发起后台下游建单以及写入快照时都会原子核对租约持有权；Redis 故障或租约丢失统一返回 `503 router_state_unavailable`，绝不向客户端确认未落盘的状态变更。丢失续租的实例必须重启。该能力面向单活重启恢复，不支持多副本双活写入。

`--source-dir` 用于在磁盘上持久化暂存上传字节及按调用方隔离的 SHA-256 源缓存。重启时挂载同一目录即可继续投递尚未交接给 upstream 的已上传文件，并继续命中哈希免传缓存。未配置 `--source-dir` 时，等待客户端 PUT 的任务、已提交到 upstream 的任务、任务取消与结果下载仍可通过 Redis 恢复，但尚未投递到 upstream 的本地暂存字节会在重启后丢失，对应文件失败并返回 `source_file_unavailable`。不同命名空间必须使用不同的 `--source-dir`；文件内容不会写入 Redis。

快照中包含上游认证凭证以及后台轮询和续租所需的客户端请求头。请使用受控且启用认证的私有 Redis 连接（必要时使用 `rediss://`），若需跨 Redis 重启恢复请同时开启 Redis 持久化。快照本身不设自动过期，体积随保留期内的任务和文件数量增长，并随常规保留扫描回收。

向上游创建任务与写入 Redis 快照属于两次独立操作；若进程在上游受理后、快照落盘前崩溃，可能留下未被确认的私有上游任务，交由上游保留策略自然回收，不承诺跨故障严格一次（exactly-once）提交。已向客户端确认的映射构成重启恢复边界。

可使用独立测试 Redis 库运行完整配置、动态管理与包含多进程 `SIGKILL` 在内的恢复测试：

```bash
MINERU_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  pytest tests/unittest/test_router_config.py \
         tests/unittest/test_router_persistence.py \
         tests/unittest/test_router_admin.py
```
