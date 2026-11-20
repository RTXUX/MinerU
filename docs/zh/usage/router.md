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
