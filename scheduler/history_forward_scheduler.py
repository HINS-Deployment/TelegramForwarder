import asyncio
import logging
import os
import random
import traceback
from datetime import datetime
from typing import List, Optional

from telethon.errors import FloodWaitError
from telethon.tl.custom.message import Message

from enums.enums import ForwardHistoryStatus
from filters.process import process_forward_rule
from managers import history_sync_manager
from models.models import (
    Chat,
    ForwardHistoryPendingMessage,
    ForwardHistoryTask,
    ForwardRule,
    get_session,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("FORWARD_HISTORY_BATCH_SIZE", "100"))
COOLDOWN_MIN = int(os.getenv("FORWARD_HISTORY_COOLDOWN_MIN", "300"))
COOLDOWN_MAX = int(os.getenv("FORWARD_HISTORY_COOLDOWN_MAX", "900"))
INTERVAL_MIN = float(os.getenv("FORWARD_HISTORY_INTERVAL_MIN", "1"))
INTERVAL_MAX = float(os.getenv("FORWARD_HISTORY_INTERVAL_MAX", "5"))
TARGET_CLEAR_BATCH = 100


class HistoricalEvent:
    """用历史消息模拟一个 NewMessage 事件，供 FilterChain 使用。"""

    def __init__(self, client, message: Message, chat_id: int, chat_entity, sender=None):
        self.client = client
        self.message = message
        self.chat_id = chat_id
        self._chat_entity = chat_entity
        self.sender = sender

    async def get_chat(self):
        return self._chat_entity

    async def get_sender(self):
        return self.sender


class HistoryForwardScheduler:
    def __init__(self, user_client, bot_client):
        self.user_client = user_client
        self.bot_client = bot_client
        self.tasks: dict[int, asyncio.Task] = {}

    async def start(self):
        """启动时恢复未完成任务。"""
        session = get_session()
        try:
            active_tasks = session.query(ForwardHistoryTask).filter(
                ForwardHistoryTask.status.notin_([
                    ForwardHistoryStatus.COMPLETED,
                    ForwardHistoryStatus.FAILED,
                    ForwardHistoryStatus.CANCELLED,
                ])
            ).all()
            for task in active_tasks:
                logger.info(f"恢复历史转发任务 {task.id}，规则 {task.rule_id}")
                history_sync_manager.set_active(task.rule_id, task)
                self.tasks[task.rule_id] = asyncio.create_task(
                    self._run_history_task(task.id)
                )
        finally:
            session.close()

    def stop(self):
        """停止所有运行中的任务。"""
        for rule_id, task in list(self.tasks.items()):
            if not task.done():
                task.cancel()
        self.tasks.clear()

    async def create_task(self, rule_id: int, count: Optional[int], event) -> tuple[bool, str]:
        """创建新的历史转发任务。"""
        rule_id = int(rule_id)
        session = get_session()
        try:
            rule = session.query(ForwardRule).get(rule_id)
            if not rule:
                return False, "规则不存在"

            existing = session.query(ForwardHistoryTask).filter(
                ForwardHistoryTask.rule_id == rule_id,
                ForwardHistoryTask.status.notin_([
                    ForwardHistoryStatus.COMPLETED,
                    ForwardHistoryStatus.FAILED,
                    ForwardHistoryStatus.CANCELLED,
                ])
            ).first()
            if existing or history_sync_manager.is_active(rule_id):
                return False, f"规则 {rule_id} 已有一个活跃的历史转发任务"

            source_chat = session.query(Chat).get(rule.source_chat_id)
            if not source_chat:
                return False, "源聊天不存在"

            start_message_id = None
            if count and count > 0:
                source_id = int(source_chat.telegram_chat_id)
                msgs = await self.user_client.get_messages(source_id, limit=count)
                if msgs:
                    start_message_id = min(m.id for m in msgs if m)

            task = ForwardHistoryTask(
                rule_id=rule_id,
                status=ForwardHistoryStatus.PENDING,
                start_message_id=start_message_id,
            )
            session.add(task)
            session.commit()
            session.refresh(task)

            history_sync_manager.set_active(rule_id, task)
            self.tasks[rule_id] = asyncio.create_task(
                self._run_history_task(task.id)
            )

            return True, f"历史转发任务 #{task.id} 已创建并开始执行"
        except Exception as e:
            session.rollback()
            logger.error(f"创建历史转发任务失败: {e}\n{traceback.format_exc()}")
            return False, f"创建任务失败: {e}"
        finally:
            session.close()

    async def get_task_status_text(self, rule_id: Optional[int] = None) -> str:
        session = get_session()
        try:
            query = session.query(ForwardHistoryTask)
            if rule_id is not None:
                query = query.filter(ForwardHistoryTask.rule_id == int(rule_id))
            tasks = query.order_by(ForwardHistoryTask.started_at.desc()).limit(5).all()
            if not tasks:
                return "没有找到历史转发任务"
            lines = []
            for task in tasks:
                rule = task.rule
                src = rule.source_chat.name if rule and rule.source_chat else "?"
                tgt = rule.target_chat.name if rule and rule.target_chat else "?"
                total = task.total_messages or 0
                processed = task.processed_messages or 0
                percent = f"{processed / total * 100:.1f}%" if total else "N/A"
                lines.append(
                    f"任务 #{task.id} 规则 {task.rule_id} ({src} -> {tgt})\n"
                    f"状态: {task.status.value}\n"
                    f"进度: {processed} / {total} ({percent})\n"
                    f"已删除目标消息: {task.deleted_messages}\n"
                    f"当前源消息 ID: {task.current_source_message_id or '-'}\n"
                    f"开始: {task.started_at}\n"
                    f"更新: {task.updated_at}"
                )
            return "\n\n".join(lines)
        finally:
            session.close()

    async def _run_history_task(self, task_id: int):
        try:
            await self._execute_history_task(task_id)
        except asyncio.CancelledError:
            logger.info(f"历史转发任务 {task_id} 被取消")
            raise
        except Exception as e:
            logger.error(f"历史转发任务 {task_id} 异常: {e}\n{traceback.format_exc()}")
            self._update_task(task_id, status=ForwardHistoryStatus.FAILED, error_message=str(e)[:500])
        finally:
            session = get_session()
            try:
                task = session.query(ForwardHistoryTask).get(task_id)
                if task:
                    history_sync_manager.set_inactive(task.rule_id)
                    self.tasks.pop(task.rule_id, None)
            finally:
                session.close()

    async def _execute_history_task(self, task_id: int):
        session = get_session()
        try:
            task = session.query(ForwardHistoryTask).get(task_id)
            if not task:
                logger.error(f"历史转发任务 {task_id} 不存在")
                return

            rule = session.query(ForwardRule).get(task.rule_id)
            if not rule:
                raise ValueError("规则不存在")

            source_chat = session.query(Chat).get(rule.source_chat_id)
            target_chat = session.query(Chat).get(rule.target_chat_id)
            if not source_chat or not target_chat:
                raise ValueError("源聊天或目标聊天不存在")

            source_id = int(source_chat.telegram_chat_id)
            target_id = int(target_chat.telegram_chat_id)

            # 获取源/目标实体，用于后续 event 包装
            source_entity = await self.user_client.get_entity(source_id)
            target_entity = await self.user_client.get_entity(target_id)

            send_client = self.bot_client if rule.use_bot else self.user_client

            task.status = ForwardHistoryStatus.CLEARING_TARGET
            task.started_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            session.commit()

            # 清空目标聊天
            await self._clear_target_chat(target_id)

            task.status = ForwardHistoryStatus.SCANNING
            task.updated_at = datetime.utcnow()
            session.commit()

            # 获取目标消息列表（按时间升序）
            target_msgs: List[Message] = []
            async for msg in self.user_client.iter_messages(target_id, reverse=True, limit=None):
                target_msgs.append(msg)

            # 确定停止消息（启动时刻源聊天最新消息）
            latest_source = await self.user_client.get_messages(source_id, limit=1)
            stop_message_id = latest_source[0].id if latest_source else None
            task.stop_message_id = stop_message_id

            # 全量/部分处理
            current_id = 0
            if task.start_message_id:
                current_id = task.start_message_id - 1

            count = None
            # 如果 task.start_message_id 已设置说明是 count 模式，已在 create_task 里写好
            # 估算总数
            if task.start_message_id:
                # 取 count 条时，总数 = stop - start + 1 的近似
                total_info = await self.user_client.get_messages(source_id, limit=0)
                task.total_messages = max(0, stop_message_id - task.start_message_id + 1)
            else:
                total_info = await self.user_client.get_messages(source_id, limit=0)
                task.total_messages = total_info.total if hasattr(total_info, "total") else 0

            task.status = ForwardHistoryStatus.SYNCING
            task.updated_at = datetime.utcnow()
            session.commit()

            processed_since_cooldown = 0
            while True:
                # 拉取一批源消息（从早到晚）
                batch: List[Message] = []
                async for msg in self.user_client.iter_messages(
                    source_id,
                    limit=BATCH_SIZE,
                    offset_id=current_id,
                    reverse=True,
                ):
                    if stop_message_id and msg.id > stop_message_id:
                        break
                    batch.append(msg)

                if not batch:
                    break

                # 将 batch 归并为 unit（单条或媒体组）
                units: List[List[Message]] = self._group_into_units(batch)

                for unit in units:
                    await self._process_unit(
                        session, task, unit, source_id, source_entity,
                        target_id, target_msgs, send_client, rule
                    )
                    current_id = max(current_id, max(m.id for m in unit))
                    task.current_source_message_id = current_id
                    task.processed_messages += len(unit)
                    task.updated_at = datetime.utcnow()
                    session.commit()

                    processed_since_cooldown += len(unit)
                    if processed_since_cooldown >= BATCH_SIZE:
                        await self._cooldown(session, task)
                        processed_since_cooldown = 0

                    # 随机间隔
                    await asyncio.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX))

                if batch[-1].id >= stop_message_id:
                    break

            # 处理同步期间排队的新消息
            await self._process_pending_messages(
                session, task, source_id, source_entity, target_id, send_client, rule
            )

            task.status = ForwardHistoryStatus.COMPLETED
            task.finished_at = datetime.utcnow()
            task.updated_at = datetime.utcnow()
            session.commit()
            logger.info(f"历史转发任务 {task_id} 完成")
        finally:
            session.close()

    async def _process_unit(self, session, task, unit, source_id, source_entity,
                            target_id, target_msgs, send_client, rule):
        first_msg = unit[0]
        src_date = first_msg.date

        # 删除目标聊天中时间排在当前源消息之后的消息
        delete_ids = []
        idx = 0
        while idx < len(target_msgs) and target_msgs[idx].date < src_date:
            idx += 1
        while idx < len(target_msgs) and target_msgs[idx].date > src_date:
            delete_ids.append(target_msgs[idx].id)
            target_msgs.pop(idx)
        if delete_ids:
            try:
                await self.user_client.delete_messages(target_id, delete_ids, revoke=True)
                task.deleted_messages += len(delete_ids)
                logger.info(f"为保证顺序删除目标消息 {len(delete_ids)} 条")
            except FloodWaitError as e:
                logger.warning(f"删除目标消息触发 FloodWait，等待 {e.seconds} 秒")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"删除目标消息出错: {e}")

        # 尝试原生转发，若失败则降级为下载再上传
        msg_ids = [m.id for m in unit]
        forwarded = False
        try:
            await send_client.forward_messages(
                target_id,
                messages=msg_ids,
                from_peer=source_id,
            )
            logger.debug(f"已原生转发 {len(unit)} 条历史消息到目标聊天")
            forwarded = True
        except FloodWaitError as e:
            logger.warning(f"转发消息触发 FloodWait，等待 {e.seconds} 秒")
            await asyncio.sleep(e.seconds)
            # 重试一次原生转发
            try:
                await send_client.forward_messages(
                    target_id,
                    messages=msg_ids,
                    from_peer=source_id,
                )
                logger.debug(f"重试后原生转发 {len(unit)} 条历史消息成功")
                forwarded = True
            except Exception as e2:
                logger.error(f"重试原生转发仍失败: {e2}")
        except Exception as e:
            logger.warning(f"原生转发失败，尝试下载再上传: {e}")

        if not forwarded:
            # 降级：逐条下载再发送
            for msg in unit:
                try:
                    await send_client.send_message(
                        target_id,
                        msg,
                        link_preview=False,
                    )
                    logger.debug(f"已下载转发 1 条消息到目标聊天")
                except FloodWaitError as e:
                    logger.warning(f"下载转发触发 FloodWait，等待 {e.seconds} 秒")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"下载转发消息出错: {e}\n{traceback.format_exc()}")

        # 重新拉取刚发送的目标消息，维持 target_msgs 有序
        try:
            sent = await send_client.get_messages(target_id, limit=5)
            for m in sent:
                # 按时间顺序插入到合适位置
                insert_idx = 0
                while insert_idx < len(target_msgs) and target_msgs[insert_idx].date < m.date:
                    insert_idx += 1
                if insert_idx >= len(target_msgs) or target_msgs[insert_idx].id != m.id:
                    target_msgs.insert(insert_idx, m)
        except Exception as e:
            logger.debug(f"更新目标消息列表失败: {e}")

    async def _process_pending_messages(self, session, task, source_id, source_entity,
                                        target_id, send_client, rule):
        task.status = ForwardHistoryStatus.PROCESSING_PENDING
        task.updated_at = datetime.utcnow()
        session.commit()

        while True:
            pending = session.query(ForwardHistoryPendingMessage).filter_by(
                task_id=task.id
            ).order_by(ForwardHistoryPendingMessage.source_message_id.asc()).limit(BATCH_SIZE).all()
            if not pending:
                break

            msg_ids = [p.source_message_id for p in pending]
            messages = await self.user_client.get_messages(source_id, ids=msg_ids)
            # 收集有效的消息 ID 列表
            valid_ids = [m.id for m in messages if m]
            if valid_ids:
                try:
                    await send_client.forward_messages(
                        target_id,
                        messages=valid_ids,
                        from_peer=source_id,
                    )
                    logger.info(f"已转发 {len(valid_ids)} 条排队消息")
                except FloodWaitError as e:
                    logger.warning(f"转发排队消息触发 FloodWait，等待 {e.seconds} 秒")
                    await asyncio.sleep(e.seconds)
                except Exception as e:
                    logger.error(f"转发排队消息出错: {e}\n{traceback.format_exc()}")

            # 删除已处理的 pending 记录
            for p in pending:
                session.query(ForwardHistoryPendingMessage).filter_by(
                    task_id=task.id,
                    source_message_id=p.source_message_id
                ).delete()
            task.processed_messages += len(pending)
            task.updated_at = datetime.utcnow()
            session.commit()
            await asyncio.sleep(random.uniform(INTERVAL_MIN, INTERVAL_MAX))

    async def _clear_target_chat(self, target_id: int):
        logger.info(f"开始清空目标聊天 {target_id}")
        while True:
            batch = []
            async for msg in self.user_client.iter_messages(target_id, limit=TARGET_CLEAR_BATCH):
                batch.append(msg.id)
            if not batch:
                break

            # 记录删除前消息数量
            before_count = len(batch)

            try:
                result = await self.user_client.delete_messages(target_id, batch, revoke=True)
                # 获取实际删除数量
                deleted_count = len(result) if hasattr(result, "__len__") else before_count
                logger.info(f"已删除目标聊天 {deleted_count} 条消息")

                # 如果实际删除数为 0，说明这批消息无法删除，跳过并继续下一批
                if deleted_count == 0:
                    logger.warning(f"目标聊天 {target_id} 的这 {before_count} 条消息无法删除，可能为系统消息，跳过")
                    continue

                # 如果删除后的消息数量没有减少（即这批消息全部无法删除），需要检查是否还有可删除消息
                # 通过检查下一批消息是否与当前批次完全一致来判断死循环
                next_batch = []
                async for msg in self.user_client.iter_messages(target_id, limit=TARGET_CLEAR_BATCH):
                    next_batch.append(msg.id)
                if next_batch and next_batch == batch:
                    # 如果下一批消息 ID 与当前批次相同，说明这批消息都无法删除，退出循环
                    logger.warning(f"目标聊天 {target_id} 剩余消息全部无法删除，清空终止")
                    break

            except FloodWaitError as e:
                logger.warning(f"清空目标聊天触发 FloodWait，等待 {e.seconds} 秒")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"清空目标聊天出错: {e}")
                # 发生异常时，检查下一批消息是否相同，避免死循环
                next_batch = []
                async for msg in self.user_client.iter_messages(target_id, limit=TARGET_CLEAR_BATCH):
                    next_batch.append(msg.id)
                if next_batch and next_batch == batch:
                    logger.warning(f"目标聊天 {target_id} 出现异常且消息未减少，退出清空")
                    break
                # 否则继续尝试
            await asyncio.sleep(random.uniform(0.5, 1.5))

    async def _cooldown(self, session, task):
        task.status = ForwardHistoryStatus.COOLDOWN
        task.updated_at = datetime.utcnow()
        session.commit()
        wait = random.randint(COOLDOWN_MIN, COOLDOWN_MAX)
        logger.info(f"历史转发任务 {task.id} 进入冷却，等待 {wait} 秒")
        await asyncio.sleep(wait)
        task.status = ForwardHistoryStatus.SYNCING
        task.updated_at = datetime.utcnow()
        session.commit()

    def _group_into_units(self, messages: List[Message]) -> List[List[Message]]:
        units: List[List[Message]] = []
        current_group: List[Message] = []
        current_group_id = None
        for msg in messages:
            if msg.grouped_id:
                if current_group and msg.grouped_id == current_group_id:
                    current_group.append(msg)
                else:
                    if current_group:
                        units.append(current_group)
                    current_group = [msg]
                    current_group_id = msg.grouped_id
            else:
                if current_group:
                    units.append(current_group)
                    current_group = []
                    current_group_id = None
                units.append([msg])
        if current_group:
            units.append(current_group)
        return units

    def _update_task(self, task_id: int, **kwargs):
        session = get_session()
        try:
            task = session.query(ForwardHistoryTask).get(task_id)
            if not task:
                return
            for k, v in kwargs.items():
                setattr(task, k, v)
            task.updated_at = datetime.utcnow()
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"更新任务 {task_id} 状态失败: {e}")
        finally:
            session.close()
