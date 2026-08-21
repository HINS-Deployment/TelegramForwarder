import logging
import secrets
import traceback

from telethon import Button

from managers.state_manager import state_manager
from models.models import WebDAVAccount, get_session
from utils.auto_delete import reply_and_delete, async_delete_user_message

logger = logging.getLogger(__name__)


async def callback_webdav_settings(event, account_id, session, message, data):
    """显示 WebDAV 账号设置面板"""
    try:
        account = session.query(WebDAVAccount).get(int(account_id))
        if not account:
            await event.answer('账号不存在')
            return

        status = '✅ 启用' if account.enabled else '❌ 禁用'
        text = (
            f"📋 WebDAV 账号设置\n\n"
            f"聊天ID: {account.chat_id}\n"
            f"用户名: {account.username}\n"
            f"状态: {status}\n"
            f"独立 Token: {'是' if account.bot_token else '否'}\n"
            f"API 代理: {account.api_base_url or '未设置'}\n"
        )

        buttons = [
            [Button.inline('🔄 切换启用/禁用', f'webdav_toggle:{account.id}')],
            [Button.inline('🌐 设置 API 代理域名', f'webdav_set_api:{account.id}')],
            [Button.inline('🔑 重置密码', f'webdav_reset_token:{account.id}')],
            [Button.inline('🗑️ 删除账号', f'webdav_delete:{account.id}')],
            [Button.inline('👈 返回列表', 'webdav_list')],
            [Button.inline('❌ 关闭', 'close_settings')],
        ]

        await message.edit(text, buttons=buttons)
        await event.answer()
    except Exception as e:
        if 'message was not modified' not in str(e).lower():
            logger.error(f"显示 WebDAV 设置失败: {e}")
            await event.answer('显示设置失败')


async def callback_webdav_list(event, account_id, session, message, data):
    """显示 WebDAV 账号列表"""
    try:
        accounts = session.query(WebDAVAccount).all()
        if not accounts:
            await event.answer('没有 WebDAV 账号')
            return

        lines = ["📋 WebDAV 账号列表:\n"]
        buttons = []
        for acc in accounts:
            status = "✅" if acc.enabled else "❌"
            chat_name = acc.chat_id
            lines.append(f"{status} {chat_name}\n")
            buttons.append([Button.inline(f"{status} {chat_name}", f'webdav_settings:{acc.id}')])

        buttons.append([Button.inline('❌ 关闭', 'close_settings')])

        await message.edit('\n'.join(lines), buttons=buttons)
        await event.answer()
    except Exception as e:
        logger.error(f"列出 WebDAV 账号失败: {e}")
        await event.answer('获取列表失败')


async def callback_webdav_toggle(event, account_id, session, message, data):
    """切换启用/禁用"""
    try:
        account = session.query(WebDAVAccount).get(int(account_id))
        if not account:
            await event.answer('账号不存在')
            return

        account.enabled = not account.enabled
        session.commit()

        await callback_webdav_settings(event, account_id, session, message, data)
        await event.answer(f'已{"启用" if account.enabled else "禁用"}')
    except Exception as e:
        session.rollback()
        logger.error(f"切换 WebDAV 状态失败: {e}")
        await event.answer('操作失败')


async def callback_webdav_reset_token(event, account_id, session, message, data):
    """重置密码"""
    try:
        account = session.query(WebDAVAccount).get(int(account_id))
        if not account:
            await event.answer('账号不存在')
            return

        new_token = secrets.token_urlsafe(32)
        account.token = new_token
        session.commit()

        await message.edit(
            f"✅ 密码已重置\n\n"
            f"聊天ID: {account.chat_id}\n"
            f"新密码: {new_token}\n\n"
            f"⚠️ 请保存好新密码",
            buttons=[[Button.inline('👈 返回', f'webdav_settings:{account.id}')],
                     [Button.inline('❌ 关闭', 'close_settings')]]
        )
        await event.answer()
    except Exception as e:
        session.rollback()
        logger.error(f"重置 WebDAV 密码失败: {e}")
        await event.answer('重置失败')


async def callback_webdav_delete(event, account_id, session, message, data):
    """删除账号"""
    try:
        account = session.query(WebDAVAccount).get(int(account_id))
        if not account:
            await event.answer('账号不存在')
            return

        chat_id = account.chat_id
        session.delete(account)
        session.commit()

        await message.edit(f"✅ 已删除聊天 {chat_id} 的 WebDAV 账号",
                          buttons=[[Button.inline('📋 返回列表', 'webdav_list')],
                                   [Button.inline('❌ 关闭', 'close_settings')]])
        await event.answer()
    except Exception as e:
        session.rollback()
        logger.error(f"删除 WebDAV 账号失败: {e}")
        await event.answer('删除失败')


async def callback_webdav_set_api(event, account_id, session, message, data):
    """进入设置 API 代理域名状态"""
    try:
        from handlers.bot_handler import handle_command
        user_id = (await event.get_sender()).id
        chat_id = event.chat_id

        state_manager.set_state(user_id, chat_id, f'webdav_set_api:{account_id}', message)

        await message.edit(
            f"请输入 API 代理域名（例如 https://my-bot-api.example.com）\n"
            f"留空或发送 /cancel 取消",
            buttons=[[Button.inline('❌ 取消', f'webdav_cancel_set_api:{account_id}')]]
        )
        await event.answer()
    except Exception as e:
        logger.error(f"设置 WebDAV API 代理状态失败: {e}")
        await event.answer('设置失败')


async def callback_webdav_cancel_set_api(event, account_id, session, message, data):
    """取消设置 API 代理域名"""
    try:
        user_id = (await event.get_sender()).id
        chat_id = event.chat_id
        state_manager.clear_state(user_id, chat_id)

        await callback_webdav_settings(event, account_id, session, message, data)
        await event.answer('已取消')
    except Exception as e:
        logger.error(f"取消设置 API 代理失败: {e}")
        await event.answer('操作失败')


# 回调处理器映射
WEBDAV_CALLBACK_HANDLERS = {
    'webdav_settings': callback_webdav_settings,
    'webdav_list': callback_webdav_list,
    'webdav_toggle': callback_webdav_toggle,
    'webdav_reset_token': callback_webdav_reset_token,
    'webdav_delete': callback_webdav_delete,
    'webdav_set_api': callback_webdav_set_api,
    'webdav_cancel_set_api': callback_webdav_cancel_set_api,
}