import io
import logging
import os
import mimetypes
from datetime import datetime
from typing import List, Optional

from telethon.tl.custom.message import Message
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto, DocumentAttributeFilename

from wsgidav import dav_provider, dav_error

logger = logging.getLogger(__name__)
access_logger = logging.getLogger('webdav.access')

# 由 server.py 在启动时设置，供 _run_async 使用
_main_loop = None


def set_main_loop(loop):
    global _main_loop
    _main_loop = loop


def _run_async(client, coro_factory, timeout=120):
    """在 client 所在的事件循环上运行协程，返回结果。"""
    import asyncio
    loop = None
    # 优先使用存储的主循环
    if _main_loop is not None:
        loop = _main_loop
    elif hasattr(client, 'loop') and client.loop:
        loop = client.loop
    if loop is None:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    coro = coro_factory()
    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)
    else:
        return loop.run_until_complete(coro)


def _get_filename(msg: Message, fallback_idx: int) -> str:
    """从消息中提取文件名，提取不到则生成一个。"""
    media = msg.media
    if isinstance(media, MessageMediaDocument):
        for attr in media.document.attributes:
            if isinstance(attr, DocumentAttributeFilename) and attr.file_name:
                return attr.file_name
        # 根据 mime_type 生成
        ext = mimetypes.guess_extension(media.document.mime_type or '') or '.bin'
        return f'document_{msg.id}{ext}'
    if isinstance(media, MessageMediaPhoto):
        return f'photo_{msg.id}.jpg'
    # 兜底
    return f'file_{msg.id}.bin'


def _iter_media_messages(client, chat_id: int) -> List[Message]:
    """迭代聊天中所有带媒体的消息，返回列表。"""
    try:
        def _fetch():
            async def _inner():
                msgs = []
                async for msg in client.iter_messages(chat_id, limit=1000):
                    if msg.media and msg.action is None:
                        msgs.append(msg)
                return msgs
            return _inner()
        return _run_async(client, _fetch, timeout=60)
    except Exception as e:
        logger.error(f"获取聊天消息失败: {e}")
        return []


class _WriteBuffer:
    """写入缓冲区，close 后仍可读取数据。"""
    def __init__(self):
        self._data = b''
        self._closed = False

    def write(self, data):
        if self._closed:
            raise ValueError("write to closed buffer")
        self._data += data
        return len(data)

    def close(self):
        self._closed = True

    def getvalue(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class TelegramDAVFile(dav_provider.DAVNonCollection):
    """单个文件资源。支持 Telethon Message 对象和 Bot API 字典两种数据源。"""

    def __init__(self, path, environ, msg, client, chat_id: int, filename: str,
                 bot_token=None, api_base_url=None):
        super().__init__(path, environ)
        self.msg = msg
        self.client = client
        self.chat_id = chat_id
        self._filename = filename
        self._content = None
        self._bot_token = bot_token
        self._api_base_url = api_base_url
        self._is_bot_api = isinstance(msg, dict)

    def _bot_req(self, method, params=None, files=None, json=None):
        """发送 Bot API HTTP 请求（不走系统代理）。"""
        import requests
        url = f"{self._api_base_url.rstrip('/')}/bot{self._bot_token}/{method}"
        kwargs = {'timeout': 120, 'proxies': {'http': None, 'https': None}}
        if params:
            kwargs['params'] = params
        if files:
            kwargs['files'] = files
        if json:
            kwargs['json'] = json
        resp = requests.post(url, **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_content_length(self):
        if self._is_bot_api:
            return self.msg.get('file_size', 0)
        if not self.msg or not self.msg.media:
            return 0
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.size
        return 0

    def get_content_type(self):
        if self._is_bot_api:
            return self.msg.get('mime_type', 'application/octet-stream')
        if not self.msg or not self.msg.media:
            return 'application/octet-stream'
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.mime_type or 'application/octet-stream'
        if isinstance(media, MessageMediaPhoto):
            return 'image/jpeg'
        return 'application/octet-stream'

    def get_creation_date(self):
        if self._is_bot_api:
            return self.msg.get('date', 0)
        return self.msg.date.timestamp() if self.msg and self.msg.date else 0

    def get_last_modified(self):
        return self.get_creation_date()

    def get_display_name(self):
        return self._filename

    def get_etag(self):
        if self._is_bot_api:
            return str(self.msg.get("message_id", 0))
        return str(self.msg.id) if self.msg else self._filename

    def support_etag(self):
        return True

    def get_content(self):
        """下载文件内容"""
        if not self.msg:
            return io.BytesIO()
        if self._content is not None:
            return io.BytesIO(self._content)

        if self._is_bot_api and self._api_base_url and self._bot_token:
            # Bot API 下载：getFile -> 获取文件路径 -> 下载
            try:
                file_id = self.msg.get('file_id')
                if not file_id:
                    return io.BytesIO()
                data = self._bot_req('getFile', params={'file_id': file_id})
                if not data.get('ok'):
                    return io.BytesIO()
                file_path = data['result']['file_path']
                dl_url = f"{self._api_base_url.rstrip('/')}/file/bot{self._bot_token}/{file_path}"
                import requests
                resp = requests.get(dl_url, timeout=120, proxies={'http': None, 'https': None})
                resp.raise_for_status()
                self._content = resp.content
                access_logger.info(f"下载文件 {self._filename} ({len(self._content)} 字节) (via Bot API)")
            except Exception as e:
                logger.error(f"下载文件 {self._filename} 失败: {e}")
                raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))
        else:
            # Telethon 下载
            try:
                def _fetch():
                    async def _inner():
                        return await self.client.download_media(self.msg, bytes)
                    return _inner()
                self._content = _run_async(self.client, _fetch)
                access_logger.info(f"下载文件 {self._filename} ({len(self._content)} 字节)")
            except Exception as e:
                logger.error(f"下载文件 {self._filename} 失败: {e}", exc_info=True)
                raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))
        return io.BytesIO(self._content) if self._content else io.BytesIO()

    def begin_write(self, content_type=None):
        """上传文件"""
        self._upload_buffer = _WriteBuffer()
        return self._upload_buffer

    def end_write(self, with_errors):
        """完成上传，发送到 Telegram"""
        if with_errors or not hasattr(self, '_upload_buffer'):
            return
        try:
            data = self._upload_buffer.getvalue()
            if self._api_base_url and self._bot_token:
                # Bot API 上传（不走系统代理）
                url = f"{self._api_base_url.rstrip('/')}/bot{self._bot_token}/sendDocument"
                import requests
                files = {'document': (self._filename, data)}
                resp = requests.post(url, files=files, timeout=120, proxies={'http': None, 'https': None})
                resp.raise_for_status()
                access_logger.info(f"上传文件 {self._filename} ({len(data)} 字节) (via Bot API)")
            else:
                # Telethon 上传
                def _upload():
                    async def _inner():
                        await self.client.send_file(
                            self.chat_id,
                            data,
                            file_name=self._filename,
                        )
                    return _inner()
                _run_async(self.client, _upload)
                access_logger.info(f"上传文件 {self._filename} ({len(data)} 字节)")
        except Exception as e:
            logger.error(f"上传文件 {self._filename} 失败: {e}")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))

    def delete(self):
        """删除文件（从聊天中删除对应消息）"""
        if not self.msg:
            return
        try:
            if self._is_bot_api and self._api_base_url and self._bot_token:
                # Bot API 删除
                msg_id = self.msg.get('message_id')
                chat_id = self.msg.get('chat_id') or self.chat_id
                self._bot_req('deleteMessage', json={
                    'chat_id': int(chat_id),
                    'message_id': msg_id,
                })
                access_logger.info(f"删除文件 {self._filename} (消息ID {msg_id}) (via Bot API)")
            else:
                # Telethon 删除
                def _del():
                    async def _inner():
                        await self.client.delete_messages(self.chat_id, [self.msg.id], revoke=True)
                    return _inner()
                _run_async(self.client, _del)
                access_logger.info(f"删除文件 {self._filename} (消息ID {self.msg.id})")
        except Exception as e:
            logger.error(f"删除文件 {self._filename} 失败: {e}")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))


class TelegramDAVRoot(dav_provider.DAVCollection):
    """根目录资源，列出所有媒体文件"""

    def __init__(self, path, environ, files: List[TelegramDAVFile], client=None, chat_id=None,
                 bot_token=None, api_base_url=None):
        super().__init__(path, environ)
        self._files = files
        self._client = client or (files[0].client if files else None)
        self._chat_id = chat_id or (files[0].chat_id if files else 0)
        self._bot_token = bot_token
        self._api_base_url = api_base_url
        self._name_map = {}
        # 处理同名文件，添加序号
        name_counts = {}
        for f in self._files:
            base = f.get_display_name()
            if base in name_counts:
                name_counts[base] += 1
                name = f"{name_counts[base]}_{base}"
            else:
                name_counts[base] = 0
                name = base
            self._name_map[name] = f

    def get_member_names(self):
        access_logger.info(f"列目录 / ({len(self._name_map)} 个文件)")
        return list(self._name_map.keys())

    def get_member(self, name):
        f = self._name_map.get(name)
        if f:
            access_logger.info(f"访问文件 /{name}")
            return TelegramDAVFile(
                f'/{name}', self.environ, f.msg, f.client, f.chat_id, name,
                bot_token=self._bot_token, api_base_url=self._api_base_url
            )
        return None

    def get_member_list(self):
        access_logger.info(f"列目录 / ({len(self._name_map)} 个文件)")
        result = []
        for name, f in self._name_map.items():
            result.append(TelegramDAVFile(
                f'/{name}', self.environ, f.msg, f.client, f.chat_id, name,
                bot_token=self._bot_token, api_base_url=self._api_base_url
            ))
        return result

    def create_empty_resource(self, name):
        """创建空资源（PUT 上传时调用）"""
        access_logger.info(f"创建文件 /{name}")
        return TelegramDAVFile(
            f'/{name}', self.environ, None, self._client, self._chat_id, name,
            bot_token=self._bot_token,
            api_base_url=self._api_base_url,
        )


class TelegramDAVProvider(dav_provider.DAVProvider):
    """WebDAV Provider，桥接 Telegram 聊天消息"""

    def __init__(self, client, chat_id: int):
        super().__init__()
        self.client = client
        self.chat_id = chat_id
        self._messages = None
        self._files = None

    def _load_files(self):
        if self._files is not None:
            return
        messages = _iter_media_messages(self.client, self.chat_id)
        self._files = []
        for i, msg in enumerate(messages):
            filename = _get_filename(msg, i)
            f = TelegramDAVFile(
                f'/{filename}', {}, msg, self.client, self.chat_id, filename
            )
            self._files.append(f)

    def get_resource_inst(self, path, environ):
        self._load_files()
        environ = environ or {}
        # 根目录
        if path == '/' or path == '':
            return TelegramDAVRoot('/', environ, self._files)
        # 文件
        name = path.lstrip('/')
        for f in self._files:
            if f.get_display_name() == name:
                return TelegramDAVFile(path, environ, f.msg, f.client, f.chat_id, name)
        return None