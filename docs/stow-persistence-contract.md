# Stow 1.0 持久化与恢复契约

## 1. 目的

本文固定 Stow 1.0 的磁盘布局、SQLite schema、journal frame、投影规则和崩溃恢复不变量。实现可以优化内部算法，但不得破坏本契约中的顺序、幂等和恢复结论。

Stow 不要求与 Locus 的磁盘格式互通。Stow 自己的 1.x 格式必须向后可读；任何不兼容变更需要新 schema/format version 和显式迁移。

## 2. 目录布局

```text
{metadataDirectory}/
  tenants.json
  {tenantId}/metadata.db

{quotaDirectory}/
  {tenantId}/quotas.db

{queueDirectory}/
  {tenantId}/
    queue.log
    queue.state.json
    projector.cursor.json
    projection.snapshot.json

{watcherDirectory}/
  options.json
  root.json
  watchers/{watcherId}.json
  history/{watcherId}.jsonl

{volumeMount}/
  {tenantId}/{shard...}/{fileKey}{extension}
  .deadletter/{tenantId}/{yyyyMMdd}/{shard...}/{fileKey}{extension}
```

所有 JSON 状态文件使用 UTF-8、LF、ISO-8601 UTC 时间和 camelCase 字段。持久化更新统一使用同目录临时文件、文件刷盘、原子替换；支持的平台上同时刷盘父目录。临时文件名为 `.{name}.{uuid}.tmp`，启动时安全删除未被引用的残留临时文件。

### 2.1 tenants.json V1

`{metadataDirectory}/tenants.json` 是租户生命周期的唯一持久化文档。V1 的根对象严格使用下列 camelCase 字段：

```json
{
  "schemaVersion": 1,
  "tenants": [
    {
      "tenantId": "tenant-a",
      "status": "ENABLED",
      "createdAt": "2026-09-18T00:00:00Z",
      "updatedAt": "2026-09-18T00:00:00Z",
      "maxFiles": 0
    }
  ]
}
```

- `schemaVersion` 是整数，V1 必须为 `1`；未知版本或未知字段都视为前向不兼容，拒绝读取和写入，保留原文件以供显式迁移。
- `tenants` 是数组；每个 `tenantId` 必须唯一，并按 `tenantId` 的升序持久化。
- `tenantId` 是第 3 节定义的受限标识符。`status` 是字符串枚举，只能是 `ENABLED` 或 `DISABLED`。
- `createdAt` 与 `updatedAt` 是 ISO-8601 UTC instant 字符串，且 `updatedAt` 不早于 `createdAt`。
- `maxFiles` 是非负整数；`0` 表示无限制。创建时复制当时的默认配额，后续修改默认配额不得改写已有条目。

每次更新均通过同目录的 `.{name}.{uuid}.tmp` 文件完成。`JsonTenantRepository` 构造即视为租户状态启动：它在读取文档前删除仅与 `tenants.json` 名称和 UUID 格式精确匹配的常规临时文件；不匹配的文件、目录和链接保持不变。临时写入在 force 后、原子替换前失败时，已提交的 `tenants.json` 必须仍可读取。

## 3. 标识与路径

- `fileKey`：32 个小写十六进制字符，由 128 位安全随机值生成。
- `eventId`、`leaseId`：UUID v4，在 JSON 中使用带连字符小写格式。
- tenant ID：1 到 128 个字符，只允许 ASCII 字母、数字、`.`、`_`、`-`，但拒绝 `.`、`..`、首尾空白和 Windows 保留设备名。
- watcher ID、volume ID：使用相同规则。
- logical directory：使用 `/` 分隔；空值规范为 `/`；拒绝 `..`、反斜杠、NUL 和控制字符。
- extension：来自原文件名最后一段，包含前导点，最长 32，只允许字母、数字、`.`、`_`、`-`；不合规则丢弃扩展名而不是拼接原值。

路径创建后调用 `normalize()` 并验证仍位于所属根目录内。默认不跟随符号链接。

## 4. SQLite 通用规则

每租户 metadata 与 quota 数据库独立。打开连接后执行：

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=-4000;
PRAGMA busy_timeout=5000;
PRAGMA foreign_keys=ON;
PRAGMA temp_store=MEMORY;
```

可配置值必须经过白名单或数值范围验证，不能直接拼接任意字符串。时间列存 UTC epoch milliseconds，boolean 使用 `INTEGER NOT NULL CHECK(value IN (0,1))`。schema version 由 `PRAGMA user_version` 管理，1.0 使用 version 1。

## 5. metadata.db schema version 1

```sql
CREATE TABLE files (
    file_key                    TEXT PRIMARY KEY NOT NULL,
    tenant_id                   TEXT NOT NULL,
    volume_id                   TEXT NOT NULL,
    physical_path               TEXT NOT NULL,
    logical_directory           TEXT NOT NULL,
    file_size                   INTEGER NOT NULL CHECK(file_size >= 0),
    created_at_ms               INTEGER NOT NULL,
    status                      INTEGER NOT NULL,
    retry_count                 INTEGER NOT NULL DEFAULT 0 CHECK(retry_count >= 0),
    last_failed_at_ms           INTEGER,
    last_error                  TEXT,
    lease_id                    TEXT,
    processing_started_at_ms    INTEGER,
    completed_at_ms             INTEGER,
    delete_succeeded_at_ms      INTEGER,
    dead_lettered_at_ms         INTEGER,
    available_at_ms             INTEGER,
    original_file_name          TEXT,
    file_extension              TEXT,
    metadata_json               TEXT,
    last_event_sequence         INTEGER NOT NULL,
    row_version                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE applied_events (
    event_id                    TEXT PRIMARY KEY NOT NULL,
    sequence_number             INTEGER NOT NULL UNIQUE,
    applied_at_ms               INTEGER NOT NULL
);

CREATE INDEX idx_files_status_available
    ON files(status, available_at_ms, created_at_ms, file_key);
CREATE INDEX idx_files_status_completed
    ON files(status, completed_at_ms, file_key);
CREATE INDEX idx_files_status_failed
    ON files(status, last_failed_at_ms, file_key);
CREATE UNIQUE INDEX idx_files_physical_path
    ON files(physical_path);
```

每个投影批次在一个事务中先检查 `applied_events`，再应用 reducer，最后插入事件 ID。重复 event ID 是幂等成功；同 sequence 对应不同 event ID 是事实冲突，停止该租户投影。

领取使用条件更新：候选查询与 `UPDATE ... WHERE row_version=? AND status IN (...)` 位于同一事务。成功更新 `lease_id`、`processing_started_at_ms`、status 和 row_version；受影响行数不是 1 表示竞争失败并重新选择。

## 6. quotas.db schema version 1

```sql
CREATE TABLE tenant_quota (
    singleton_id                INTEGER PRIMARY KEY CHECK(singleton_id = 1),
    current_count               INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
    max_count                   INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
    updated_at_ms               INTEGER NOT NULL,
    row_version                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE directory_quotas (
    logical_directory           TEXT PRIMARY KEY NOT NULL,
    current_count               INTEGER NOT NULL DEFAULT 0 CHECK(current_count >= 0),
    max_count                   INTEGER NOT NULL DEFAULT 0 CHECK(max_count >= 0),
    enabled                     INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at_ms               INTEGER NOT NULL,
    updated_at_ms               INTEGER NOT NULL,
    row_version                 INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE quota_reservations (
    reservation_id              TEXT PRIMARY KEY NOT NULL,
    file_key                    TEXT NOT NULL UNIQUE,
    logical_directory           TEXT NOT NULL,
    created_at_ms               INTEGER NOT NULL
);

CREATE TABLE applied_quota_events (
    event_id                    TEXT PRIMARY KEY NOT NULL,
    sequence_number             INTEGER NOT NULL UNIQUE,
    applied_at_ms               INTEGER NOT NULL
);
```

写入开始前生成 file key，并在 quota 事务中同时检查 tenant/directory 上限、增加计数、插入 reservation。`ACCEPTED` 投影删除 reservation，但不再次增加计数。物理写入前失败或物理文件成功删除后删除 reservation 并减少计数。启动时 reservation 按 file key 与 journal/物理文件核对：存在事实则消费 reservation，不存在事实则回滚。

`DELETE_SUCCEEDED` 与 `DEAD_LETTERED` 投影只在文件第一次离开 active set 时各减少一次计数。每个 quota 事件先检查并写入 `applied_quota_events`，使 reservation 消费和计数减少可以独立幂等。对账从 metadata active rows 重算，不以物理目录文件数直接覆盖，物理扫描只用于 metadata 丢失恢复。

metadata.db 与 quotas.db 不能组成一个 SQLite 事务。projector 对每个事件先幂等提交 metadata，再幂等提交 quota，二者都成功后才推进 cursor；任一侧失败时重放通过各自的 applied-events 表补齐另一侧。任何对账都不得在事件尚未完整应用时推进 cursor。

有效 snapshot 已覆盖某个 sequence 且对应 journal 前缀已成功压缩后，维护事务可以分别删除两个 applied-events 表中不大于该 sequence 的记录。清理失败不影响正确性，只影响空间占用；不得在 snapshot 或 compaction 完成前提前删除。

## 7. queue.log Binary V1

文件不含全局 header，每条记录独立可扫描。所有整数使用 big-endian。

```text
offset  size  field
0       4     magic = ASCII "STW1"
4       4     frameLength，包含本 frame 全部字节
8       8     sequenceNumber，>= 1
16      4     payloadLength
20      N     UTF-8 canonical JSON payload
20+N    4     CRC32(sequenceNumber bytes + payloadLength bytes + payload)
```

`frameLength = 24 + payloadLength`。payload 上限 1 MiB，frameLength 小于 24、越界、magic 错误或 CRC 错误均为损坏。canonical JSON 使用固定字段名、忽略值为 null 的可选字段、枚举大写名称和毫秒精度 UTC 时间。

扫描只允许修复坏尾：如果最后一个 frame 不完整或 CRC 错误，截断到最后一个完整 frame 并记录原始长度。完整 frame 中的损坏、sequence gap/倒退不能自动跳过，租户进入 DOWN。

## 8. JsonLines V1

每行是一个完整 `QueueEventRecord` JSON，结尾必须为 LF。字段 `schemaVersion=1`、`sequenceNumber` 和 `eventId` 必填。每行另含 `payloadCrc32`，CRC32 覆盖移除该字段后的 canonical JSON UTF-8 bytes。

仅最后一行缺少 LF、JSON 不完整或 CRC 错误时允许截断。已有非空日志保持原格式，配置格式只影响新建或空日志。

## 9. journal 状态文件

`queue.state.json` version 1：

```json
{
  "schemaVersion": 1,
  "format": "BINARY_V1",
  "baseOffset": 0,
  "tailOffset": 0,
  "lastSequenceNumber": 0,
  "corruptTailDetected": false,
  "lastCorruptTailOffset": null,
  "repairCount": 0,
  "updatedAt": "2026-09-18T00:00:00Z"
}
```

state 是加速信息，不高于 queue.log。缺失、旧或损坏时扫描日志重建。`baseOffset` 是压缩前累计删除的逻辑字节数，`tailOffset = baseOffset + queue.log 当前长度`。

`projector.cursor.json` version 1 保存 tenantId、nextOffset、lastSequenceNumber、lastEventId 和 updatedAt。cursor 只能在 SQLite 事务提交后推进；cursor 落后时 applied_events 保证重放幂等。

## 10. projection snapshot

`projection.snapshot.json` version 1 包含：tenant ID、throughOffset、throughSequence、createdAt、active files 数组、tenant quota、directory quotas 和 `contentCrc32`。active files 按 file key 排序，目录配额按 logical directory 排序，以获得确定性输出。

snapshot 只有在 projector 已追上当时 tail 后生成。写入成功后再次读取并校验 CRC，再允许 journal compaction。恢复时只接受：CRC 正确、throughOffset 位于 `[baseOffset, tailOffset]`、throughSequence 不大于日志最后 sequence 的 snapshot。

## 11. 状态迁移表

| 事件 | 允许的前置状态 | 结果状态 | 关键副作用 |
| --- | --- | --- | --- |
| ACCEPTED | 不存在 | PENDING | 创建元数据，消费 quota reservation |
| PROCESSING_STARTED | PENDING/FAILED | PROCESSING | 写入新 leaseId 和 startedAt |
| PROCESSING_FAILED | PROCESSING，同 lease | FAILED 或 PERMANENTLY_FAILED | retry+1，清 lease，计算 availableAt |
| PROCESSING_TIMED_OUT | PROCESSING，同 lease | PENDING | 清 lease，不增加 retry |
| PROCESSING_COMPLETED | PROCESSING 或已释放的同 lease | COMPLETED | 清 lease，写 completedAt |
| DELETE_REQUESTED | COMPLETED | DELETE_REQUESTED | 排队物理删除 |
| DELETE_SUCCEEDED | DELETE_REQUESTED | DELETE_SUCCEEDED 后移除 | 减少 quota，删除 active row |
| DEAD_LETTERED | PERMANENTLY_FAILED | DEAD_LETTERED 后移除 | 减少 quota，删除 active row |

同 event ID 重放不产生副作用。相同合法最终状态的同租约重复调用是幂等成功；不同 leaseId、缺失前置事件或非法逆向迁移是冲突。

## 12. 写入事务边界

1. 生成 file key 与 reservation ID。
2. quota.db 原子创建 reservation 并增加 tenant/directory count。
3. 写同目录临时文件，按配置 force，原子移动到最终路径。
4. 追加 ACCEPTED；DURABLE 模式等待 force，BALANCED 等待写入，ASYNC 只等待进入有界队列。
5. 更新内存 active cache，并把投影写入有界队列。
6. projector 幂等提交 metadata 与 quota 两侧的 ACCEPTED，删除 reservation；两侧都成功后推进 cursor。

失败补偿遵循“物理文件存在则计数不能被释放”。无法删除的残留文件由孤儿恢复接管。

## 13. 删除与 dead-letter 边界

完成不直接删除文件。reaper 只处理 `DELETE_REQUESTED`：物理文件不存在视为幂等删除成功；成功后追加 `DELETE_SUCCEEDED`。投影应用该事件后才删除 metadata 和减少 quota。

MOVE_TO_DEAD_LETTER 使用同文件系统原子移动；跨文件系统配置在启动时拒绝。移动成功后追加 `DEAD_LETTERED`。事件追加失败时 dead-letter 扫描根据 file key 恢复事件，不把文件移回 active 目录。

## 14. 启动恢复顺序

1. 获取运行目录独占锁；失败则拒绝启动。
2. 验证配置和根路径边界。
3. 加载租户并挂载、探测卷。
4. `quick_check` metadata/quota 数据库；必要时备份损坏文件。
5. 扫描/修复 journal 坏尾，重建 queue.state。
6. 验证 snapshot 与 cursor。
7. 从 snapshot + journal 重建损坏投影，或从 cursor 继续幂等 replay。
8. 核对 quota reservations。
9. 分批加载 active cache并对账 quota。
10. 运行配置要求的启动孤儿恢复。
11. 标记 RUNNING，随后启动周期任务和 watcher。

任何租户出现中间日志损坏、未知 schema 或 sequence 冲突时，默认 fail-fast。关闭 fail-fast 只允许其他租户运行，问题租户必须保持隔离 DOWN，不能接受写入或领取。

## 15. 崩溃恢复矩阵

| 崩溃点 | 重启后的确定处理 |
| --- | --- |
| reservation 后、物理写前 | 无文件/事件，回滚 reservation 与计数 |
| 临时文件写入中 | 删除残留临时文件，回滚 reservation |
| 最终文件完成、ACCEPTED 前 | 孤儿扫描生成 ACCEPTED，消费 reservation |
| ACCEPTED 完成、SQLite 前 | replay 创建投影并消费 reservation |
| SQLite commit 后、cursor 前 | replay 命中 applied_events，幂等推进 cursor |
| snapshot 临时文件阶段 | 忽略/删除临时文件，使用旧 snapshot |
| compaction 替换阶段 | 根据 state、snapshot 和完整 frame 扫描确定 base/tail，不重复丢前缀 |
| DELETE_REQUESTED 后、删除前 | reaper 重试删除 |
| 删除后、DELETE_SUCCEEDED 前 | 文件不存在视为成功并补事件 |
| dead-letter 移动后、事件前 | dead-letter 扫描补 DEAD_LETTERED |
| metadata.db 损坏 | 备份后由 snapshot + journal 重建 |
| quotas.db 损坏 | 备份后由 metadata active rows 重建 |

## 16. 压缩不变量

只有同时满足以下条件才允许压缩：projector cursor 等于 tail、有效 snapshot 覆盖 cursor、处理字节达到阈值、当前无 append/读取持锁。压缩在租户 journal 独占锁内把未处理后缀写入临时文件、刷盘、原子替换，再原子写 state。任何失败保留原 queue.log 或一个可以由 snapshot 恢复的完整后缀。

## 17. 关闭不变量

关闭先阻止新写入和领取，再停止 watcher/cleanup，排空 journal writer 与 metadata 队列，刷 state/cursor，checkpoint SQLite，最后释放目录锁。超过 shutdown timeout 时返回明确失败并把 runtime 标记 FAILED；不得把未排空称为成功关闭。
