# V1 HTTP API 完整示例

自部署 V1 API 与 Python SDK、WebUI 使用同一套接口。本页说明一次完整请求周期——创建上传、上传字节、提交解析任务、轮询到终态、下载产物——并指向一个经过测试的示例脚本。Python 客户端见 [Python SDK](sdk_api.md)。

先启动本地服务，或将示例指向任意 V1 部署（包括官方云服务）：

```bash
mineru-kit api-server --host 127.0.0.1 --port 8000 --tier standard
```

## 经过测试的示例脚本

包含超时、响应校验、有界轮询、退出码和凭据隔离的完整闭环，以仓库中单一经过测试的脚本维护：

> [`scripts/http_api_example.sh`](https://github.com/opendatalab/MinerU/blob/master/scripts/http_api_example.sh)

```bash
export MINERU_API_URL=http://127.0.0.1:8000
export MINERU_API_KEY=secret-key        # 匿名本地访问时省略
./scripts/http_api_example.sh document.pdf
```

脚本行为由 `tests/unittest/test_http_api_example_script.py` 在真实 Flash 服务和脚本化 mock 服务上验证，无需下载模型。退出码面向自动化保持稳定：

| 码 | 含义 |
| --- | --- |
| `0` | 任务 completed，全部请求产物已保存 |
| `1` | 脚本、传输、协议、下载或本地写入失败 |
| `2` | 任务 partial：已先保存成功文件的产物 |
| `3` | 任务 failed |
| `4` | 任务 canceled |
| `124` | 轮询预算耗尽；打印 `job_id`，可继续轮询 |

## 鉴权与字节上传

服务以 `--api-key` 启动时，`/v1/*` 请求携带 `Authorization: Bearer $MINERU_API_KEY`；匿名本地访问可省略该头。

字节上传原样使用 `POST /v1/uploads` 返回的方法、URL 和请求头：

- 自部署服务返回同源 URL（如 `/v1/uploads/{id}/content`）。同源指 scheme + host + 有效端口，由客户端判断而非假设；同源上传在服务要求鉴权时必须携带 API 鉴权。
- 官方 API 返回自带授权的预签名对象存储 URL。**不要**向不同源的 URL 附加 MinerU API Key——那会泄露凭据。服务返回的上传头（`upload_headers`）始终原样保留。

示例脚本实现了与 Python SDK（`api_client._same_origin_upload_headers`）相同的规则：相对 URL 先按 API 基地址解析，再比较 scheme + host + 有效端口，仅对同源上传附加 MinerU Key。相对 URL 解析后即同源；同一主机的不同端口按不同源处理。

下载走 `GET /v1/files/{file_id}/content`，可能返回 `302` 重定向。示例跟随重定向但不使用 `--location-trusted`，凭据不会被重新发送到不同源（包括同主机不同端口；要求 curl >= 7.83.0）。

## 脚本刻意覆盖的请求周期细节

- **上传生命周期。** 创建上传时传 `sha256sum` 可启用完整性校验和秒传；命中已存在文件时响应直接为 `completed` 并内嵌 `file` 对象，跳过字节上传和完成两步。
- **HTTP 200 不等于合法响应。** 脚本在每一步校验 JSON 结构：ID 必须是非空字符串、状态必须属于支持集合；否则立即失败，而不是带着 null ID 进入轮询。
- **有界轮询。** `MAX_POLLS` 限制轮询请求次数（每次还包括请求超时和轮询间隔）；预算耗尽时脚本退出 `124` 并打印 `job_id`——任务未被取消，可直接继续轮询。
- **终态。** `completed`、`partial`、`failed`、`canceled` 都是终态。`partial` 表示部分文件成功：脚本先保存成功文件的产物、打印含错误的逐文件结果，然后才退出 `2`；`failed` 退出 `3`，`canceled` 退出 `4`。
- **原子下载。** 产物先写 `*.part`，成功后才移到最终文件名；`completed` 任务若有产物下载失败或缺失，不会退出 `0`。
- **产物。** 所有产物都是 `purpose:"parse_output"` 的文件；内容一律通过 `GET /v1/files/{file_id}/content`、用 `files[].output_files` 下的 id 获取。以 `GET /v1/health`（`features.sources`、`features.output_formats`）为当前部署实际能力的依据；渲染器支持的格式不代表 API 暴露。

## 生产部署注意事项

- 谨慎绑定 `--host`。默认 `127.0.0.1` 只监听回环；使用 `0.0.0.0` 暴露到网络时，应配合 `--api-key` 或带鉴权的反向代理。
- 存储与重启边界：文件字节保存在 `--upload-dir` 下（未配置时为临时目录，正常关闭即删除），但上传/文件/任务的资源索引是进程内状态。即使保留了目录，服务重启后旧的 `upload_id`/`file_id`/任务 ID 也不保证可用。该 API 不是可恢复的持久化任务服务；持久化阅读、索引和缓存需求使用文档库——但文档库不是这套上传/任务接口的透明持久化替代品。
- `--concurrency` 限制并发解析任务数；超出后表现为 `queued` 时间变长，而不是失败。
- `GET /v1/usage` 报告用量。用 `/v1/health` 和 `/docs`（启用时）的 OpenAPI 文档区分服务端故障与客户端错误；重试 `failed` 任务前先查看服务端日志。

## 旧版 multipart 适配器

本地解析服务（`mineru-kit api-server` / `mineru-api`）提供同步 `POST /file_parse` 兼容入口：

```bash
curl http://127.0.0.1:8000/file_parse \
  -F 'files=@document.pdf' \
  -F 'parse_method=auto' \
  -F 'return_md=true'
```

多个文件重复使用 `files` 字段；鉴权沿用 V1 的 Bearer token。响应形如 `{"backend":"hybrid-auto-engine","version":"...","results":{"document":{"md_content":"..."}}}`，重名文件 stem 自动添加数字后缀。解析失败返回 409，参数错误返回 400。每次最多 100 个文件，单文件上限 200 MiB。

| 字段 | 行为 |
| --- | --- |
| `parse_method` | `auto`（默认）、`txt` 或 `ocr` |
| `start_page_id`、`end_page_id` | PDF 零起始、两端包含的页范围，默认 `0`、`99999`；非 PDF 原生格式仍整本解析 |
| `return_md` | 默认 `true`，返回 `md_content` |
| `return_middle_json`、`return_model_output`、`return_content_list` | 默认 `false`，沿用旧接口的 JSON **字符串**；无模型输出时为 `null` |
| `return_images` | 默认 `false`，返回文件名到 base64 data URL 的 `images` 映射 |
| `response_format_zip` | 默认 `false`，返回 `results.zip`，选中的输出位于 `<stem>/<parse_method>/` |
| `return_original_file` | ZIP 中附带原始上传字节，命名为 `<stem>_origin.<extension>` |
| `backend` | `pipeline` 映射 Basic；`hybrid-*`、`vlm-*`（`auto-engine` 或 `http-client`）映射 Standard；`flash` 映射 Flash；省略时使用服务启动 tier |
| `tier` | 可显式覆盖 backend 映射，仍受服务能力限制 |

兼容范围是传输外壳，**不恢复旧文档协议或旧推理引擎**。Middle JSON、model output 使用当前协议，content list 使用当前 Content List V1 renderer。响应 `backend` 回显请求标签，省略时为 `hybrid-auto-engine`，不代表实际引擎。旧 `lang_list`、`formula_enable`、`table_enable`、`image_analysis` 字段接受但忽略，执行遵循当前解析及服务配置。非空 `server_url` 和 `client_side_output_generation=true` 会被拒绝；VLM 连接需在服务端配置。

适配器复用 V1 并发限制和模型生命周期，每次调用后清理临时上传、产物和任务记录。Router 也可按 tier 和负载选择支持该适配器的 upstream，并应用后端认证配置。同步调用没有可恢复的 Router 公共任务 ID；`/tasks` 不恢复。新客户端应使用 V1，见[迁移指南](../reference/migration_4.md)。
