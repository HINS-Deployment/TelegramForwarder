"""历史消息同步的内存状态管理器。

用于标记哪些规则正在进行历史转发，以便 message_listener 在同步期间
暂停实时转发，并把新消息排队。
"""

from typing import Optional, Dict
from models.models import ForwardHistoryTask

_active_tasks: Dict[int, ForwardHistoryTask] = {}


def set_active(rule_id: int, task: ForwardHistoryTask) -> None:
    _active_tasks[int(rule_id)] = task


def set_inactive(rule_id: int) -> None:
    _active_tasks.pop(int(rule_id), None)


def get_active_task(rule_id: int) -> Optional[ForwardHistoryTask]:
    return _active_tasks.get(int(rule_id))


def is_active(rule_id: int) -> bool:
    return int(rule_id) in _active_tasks
