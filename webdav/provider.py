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


class _StreamFile:
    """流式文件读取，读取后自动删除临时文件。"""
    def __init__(self, path):
        self._path = path
        self._file = open(path, 'rb')

    def read(self, size=-1):
        return self._file.read(size)

    def close(self):
        try:
            self._file.close()
        finally:
            if os.path.exists(self._path):
                os.unlink(self._path)

    def __iter__(self):
        return self._file

    def __next__(self):
        return next(self._file)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class TelegramDAVFile(dav_provider.DAVNonCollection):
    """单个文件资源。"""

    def __init__(self, path, environ, msg, client, chat_id: int, filename: str,
                 bot_token=None, api_base_url=None, file_id=None):
        super().__init__(path, environ)
        self.msg = msg
        self.client = client
        self.chat_id = chat_id
        self._filename = filename
        self._bot_token = bot_token
        self._api_base_url = api_base_url
        self._file_id = file_id or (msg.get('file_id') if isinstance(msg, dict) else None)

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
        if self._file_id:
            return 0  # 由 Bot API 返回，长度未知
        if not self.msg or not self.msg.media:
            return 0
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.size
        if isinstance(media, MessageMediaPhoto):
            return 0
        return 0

    def get_content_type(self):
        if self._file_id:
            return 'application/octet-stream'
        if not self.msg or not self.msg.media:
            return 'application/octet-stream'
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.mime_type or 'application/octet-stream'
        if isinstance(media, MessageMediaPhoto):
            return 'image/jpeg'
        return 'application/octet-stream'

    def get_creation_date(self):
        if self._file_id:
            return 0
        return self.msg.date.timestamp() if self.msg and self.msg.date else 0

    def get_last_modified(self):
        return self.get_creation_date()

    def get_display_name(self):
        return self._filename

    def get_etag(self):
        if self._file_id:
            return self._file_id
        return str(self.msg.id) if self.msg else self._filename

    def support_etag(self):
        return True

    def _get_file_id_via_bot_api(self):
        """通过 Bot API forwardMessage 获取 file_id，缓存到 _file_id。"""
        if not self._api_base_url or not self._bot_token or not self.msg:
            return None
        try:
            import requests
            from_chat_id = int(self.msg.chat_id) if hasattr(self.msg, 'chat_id') else self.chat_id
            msg_id = self.msg.id if hasattr(self.msg, 'id') else None
            if not msg_id:
                return None

            # 转发到源聊天（获取 file_id），然后立即删除
            base = self._api_base_url.rstrip('/')
            resp = requests.post(f"{base}/bot{self._bot_token}/forwardMessage", json={
                'chat_id': from_chat_id,
                'from_chat_id': from_chat_id,
                'message_id': msg_id,
            }, timeout=30, proxies={'http': None, 'https': None})
            resp.raise_for_status()
            data = resp.json()
            if not data.get('ok'):
                logger.warning(f"forwardMessage 获取 file_id 失败: {data.get('description', 'unknown')}")
                return None

            result = data['result']
            # 提取 file_id（document / photo / video / audio）
            doc = result.get('document') or result.get('video') or result.get('audio')
            if not doc:
                photo = result.get('photo')
                if isinstance(photo, list) and photo:
                    doc = photo[-1]
            if not doc:
                return None

            file_id = doc.get('file_id')
            if not file_id:
                return None

            # 删除刚转发的消息
            fwd_msg_id = result.get('message_id')
            if fwd_msg_id:
                try:
                    requests.post(f"{base}/bot{self._bot_token}/deleteMessage", json={
                        'chat_id': from_chat_id,
                        'message_id': fwd_msg_id,
                    }, timeout=10, proxies={'http': None, 'https': None})
                except Exception:
                    pass

            self._file_id = file_id
            access_logger.info(f"通过 Bot API 获取 file_id 成功: {file_id[:20]}...")
            return file_id
        except Exception as e:
            logger.warning(f"获取 Bot API file_id 失败: {e}")
            return None

    def get_content(self):
        """下载文件内容（仅 Bot API，不走 Telethon）"""
        if not self._file_id:
            self._get_file_id_via_bot_api()

        if not self._file_id or not self._api_base_url or not self._bot_token:
            raise dav_error.DAVError(dav_error.HTTP_NOT_FOUND, "No Bot API file_id available")

        try:
            data = self._bot_req('getFile', params={'file_id': self._file_id})
            if not data.get('ok'):
                raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                         f"getFile failed: {data.get('description', 'unknown')}")
            file_path = data['result']['file_path']
            dl_url = f"{self._api_base_url.rstrip('/')}/file/bot{self._bot_token}/{file_path}"
            import requests
            resp = requests.get(dl_url, stream=True, timeout=120, proxies={'http': None, 'https': None})
            resp.raise_for_status()
            access_logger.info(f"流式下载文件 {self._filename} (via Bot API)")
            return resp.raw
        except Exception as e:
            logger.error(f"下载文件 {self._filename} 失败: {e}")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))

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
            if not self._api_base_url or not self._bot_token:
                raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                         "Bot API not configured for upload")
            # Bot API 上传（不走系统代理）
            url = f"{self._api_base_url.rstrip('/')}/bot{self._bot_token}/sendDocument"
            import requests
            files = {'document': (self._filename, data)}
            resp = requests.post(url, files=files, timeout=120, proxies={'http': None, 'https': None})
            resp.raise_for_status()
            result = resp.json()
            if result.get('ok'):
                # 缓存上传后的 file_id
                doc = result.get('result', {}).get('document', {})
                if doc.get('file_id'):
                    self._file_id = doc['file_id']
                access_logger.info(f"上传文件 {self._filename} ({len(data)} 字节) (via Bot API)")
            else:
                logger.error(f"上传文件 {self._filename} 失败: {result.get('description', 'unknown')}")
        except Exception as e:
            logger.error(f"上传文件 {self._filename} 失败: {e}")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))

    def delete(self):
        """删除文件（从聊天中删除对应消息）"""
        if not self.msg and not self._file_id:
            return
        try:
            if self._file_id and self._api_base_url and self._bot_token:
                # Bot API 删除
                msg_id = None
                chat_id = self.chat_id
                if isinstance(self.msg, dict):
                    msg_id = self.msg.get('message_id')
                    chat_id = self.msg.get('chat_id') or chat_id
                elif self.msg:
                    msg_id = self.msg.id
                self._bot_req('deleteMessage', json={
                    'chat_id': int(chat_id),
                    'message_id': msg_id,
                })
                access_logger.info(f"删除文件 {self._filename} (消息ID {msg_id}) (via Bot API)")
            else:
                raise dav_error.DAVError(dav_error.HTTP_METHOD_NOT_ALLOWED,
                                         "Deletion requires Bot API configuration")
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
                bot_token=self._bot_token, api_base_url=self._api_base_url,
                file_id=f._file_id
            )
        return None

    def get_member_list(self):
        access_logger.info(f"列目录 / ({len(self._name_map)} 个文件)")
        result = []
        for name, f in self._name_map.items():
            result.append(TelegramDAVFile(
                f'/{name}', self.environ, f.msg, f.client, f.chat_id, name,
                bot_token=self._bot_token, api_base_url=self._api_base_url,
                file_id=f._file_id
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