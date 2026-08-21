# `/forward_history` 历史消息转发功能实现计划

## Context

当前 TelegramForwarder 只监听并转发新消息。用户希望增加 `/forward_history <rule_id> [count]` 命令，把源聊天的历史消息按时间顺序同步到目标聊天，并在同步期间暂停该规则的实时转发，等追上最新消息后再处理排队的新消息。任务需要持久化、可恢复、带冷却、防重复，并能在目标聊天已有消息时通过“删后补回”保证顺序。

## 已确认的需求

- 命令格式：`/forward_history <rule_id> [count]`，不传 `count` 时全量转发。
- 历史消息要走完整的 FilterChain（关键词、替换、媒体过滤、AI 等）。
- 同一规则同时只能有一个活跃任务，第二个请求直接拒绝。
- 任务启动后清空目标聊天记录。
- 同步期间暂停该规则的新消息转发，新消息先排队，追上后补齐。
- 每处理一定数量消息后随机冷却 5–15 分钟。
- 任务状态实时写入数据库，重启后自动恢复。
- 转发时逐条对比目标聊天列表，删除时间排在当前源消息之后的目标消息，再重新补齐。

## 推荐方案

### 1. 新增数据库模型

在 `models/models.py` 增加：

- `ForwardHistoryTask`：任务状态表，字段包含 `rule_id`、`status`、`total_messages`、`processed_messages`、`deleted_messages`、`current_source_message_id`、`stop_message_id`、`start_message_id`、`error_message`、`started_at`、`updated_at`、`finished_at`。
- `ForwardHistoryPendingMessage`：同步期间排队的新消息表，字段 `task_id`、`rule_id`、`source_message_id`。

新增枚举 `ForwardHistoryStatus`（`PENDING`、`CLEARING_TARGET`、`SCANNING`、`SYNCING`、`COOLDOWN`、`PROCESSING_PENDING`、`COMPLETED`、`FAILED`、`CANCELLED`）。

### 2. 新增调度器

新增 `scheduler/history_forward_scheduler.py`：

- 维护 `self.tasks: dict[rule_id, asyncio.Task]`。
- `start()`：启动时从数据库加载非终态任务并恢复执行。
- `create_task(rule_id, count, event)`：创建新任务，检查排他性，写入数据库，启动协程。
- `_run_history_task(task_id)`：任务主循环，驱动各阶段。
- `_execute_history_task(task)`：核心逻辑。
- `stop()`：取消所有运行中任务并优雅停止。

### 3. 暂停普通转发

新增 `managers/history_sync_manager.py`：

- 内存标记当前正在同步的规则 `dict[rule_id, ForwardHistoryTask]`。

在 `message_listener.py` 的 `handle_user_message()` 中，处理每条规则前检查：

- 若该规则正在同步且消息 ID > `stop_message_id`，写入 `ForwardHistoryPendingMessage` 并跳过实时转发。
- 若消息 ID <= `stop_message_id`，由历史任务覆盖，也跳过。

### 4. 清空目标聊天

任务启动后：

- `status = CLEARING_TARGET`。
- 使用 `user_client.iter_messages(target_id, limit=None)` 分批读取，每 100 条调用 `user_client.delete_messages(..., revoke=True)` 删除。
- 处理 `FloodWaitError`。

### 5. 核心同步算法

1. 用 `user_client.get_messages(source_id, limit=1)` 获取启动时刻最新消息 ID，写入 `stop_message_id`。
2. 若指定 `count`，用 `get_messages(source_id, limit=count)` 得到最早要处理的消息，写入 `start_message_id`。
3. 预拉取目标聊天全部消息列表 `target_msgs`（按时间升序），用于对比。
4. 用 `user_client.iter_messages(source_id, reverse=True, offset_id=current_id, limit=BATCH_SIZE)` 从早到晚遍历源消息。
5. 媒体组消息归并到同一个 unit。
6. 对每个 unit：
   - 在 `target_msgs` 中定位到第一个时间 >= 当前源消息时间的位置。
   - 删除所有时间 > 当前源消息时间的目标消息（`delete_messages`）。
   - 调用 `filters/process.py` 的 FilterChain 转发当前 unit（传入 `is_history_sync=True`）。
   - 将发送后的目标消息按时间顺序插入 `target_msgs`。
7. 每处理完一个 unit 随机 sleep 1–5 秒。
8. 每处理 `FORWARD_HISTORY_BATCH_SIZE` 条后，随机冷却 5–15 分钟。
9. 当源消息 ID >= `stop_message_id` 时，进入 `PROCESSING_PENDING`：
   - 读取 `ForwardHistoryPendingMessage`，按 ID 升序转发。
   - 完成后标记 `COMPLETED`。

### 6. 过滤器兼容改动

- `filters/context.py`：`MessageContext` 增加 `is_history_sync = False`。
- `filters/process.py`：`process_forward_rule()` 增加 `is_history_sync=False` 参数并传入 `MessageContext`。
- `filters/delete_original_filter.py`：若 `is_history_sync` 直接返回 `True`，避免删除源历史消息。
- `filters/edit_filter.py`：若 `is_history_sync` 直接返回 `True`，避免编辑源历史消息。

### 7. 命令与 UI

- 在 `handlers/command_handlers.py` 新增 `handle_forward_history_command()` 和 `handle_forward_history_status_command()`。
- 在 `handlers/bot_handler.py` 注册 `forward_history`、`fh`、`forward_history_status`、`fhs`。
- 在 `main.py` 的 `register_bot_commands()` 增加 BotCommand 菜单。
- 帮助文本增加说明。

### 8. 配置项

在 `.env.example` 增加：

```text
FORWARD_HISTORY_BATCH_SIZE=100
FORWARD_HISTORY_COOLDOWN_MIN=300
FORWARD_HISTORY_COOLDOWN_MAX=900
FORWARD_HISTORY_INTERVAL_MIN=1
FORWARD_HISTORY_INTERVAL_MAX=5
```

### 9. 启动 Hook

在 `main.py` 的 `start_clients()` 中，客户端启动后、进入 `run_until_disconnected()` 前启动 `HistoryForwardScheduler`；在 finally 中停止。

## 关键文件

- `scheduler/history_forward_scheduler.py`（新建）
- `managers/history_sync_manager.py`（新建）
- `models/models.py`
- `models/db_operations.py`（可选辅助）
- `message_listener.py`
- `filters/process.py`
- `filters/context.py`
- `filters/delete_original_filter.py`
- `filters/edit_filter.py`
- `handlers/command_handlers.py`
- `handlers/bot_handler.py`
- `main.py`
- `.env.example`

## 验证步骤

1. 启动后检查数据库出现 `forward_history_tasks` 和 `forward_history_pending_messages`。
2. 对消息较少的规则执行 `/forward_history <rule_id>`，验证目标聊天被清空、消息按时间顺序出现、媒体组保持合并。
3. 执行 `/forward_history <rule_id> 10`，确认只转最近 10 条。
4. 在目标聊天手动发几条新消息后执行 `/forward_history`，验证这些消息被删除并重新补齐。
5. 任务运行期间向源聊天发新消息，确认目标未立即转发；任务完成后新消息被补上。
6. 中途停止容器后重启，确认任务从断点继续，不重复转发已处理消息。
7. 对同一规则连续发两次 `/forward_history`，第二次被拒绝。
8. 使用 `/forward_history_status <rule_id>` 观察进度变化。
