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
    messages = []
    try:
        # 使用同步方式获取（wsgidav 是同步的）
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def _fetch():
                msgs = []
                async for msg in client.iter_messages(chat_id, limit=1000):
                    if msg.media and not hasattr(msg, 'action'):
                        msgs.append(msg)
                return msgs
            messages = loop.run_until_complete(_fetch())
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"获取聊天消息失败: {e}")
    return messages


class TelegramDAVFile(dav_provider.DAVNonCollection):
    """单个文件资源"""

    def __init__(self, path, environ, msg: Message, client, chat_id: int, filename: str):
        super().__init__(path, environ)
        self.msg = msg
        self.client = client
        self.chat_id = chat_id
        self._filename = filename
        self._content = None  # 按需加载

    def get_content_length(self):
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.size
        return 0  # 图片大小未知

    def get_content_type(self):
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.mime_type or 'application/octet-stream'
        if isinstance(media, MessageMediaPhoto):
            return 'image/jpeg'
        return 'application/octet-stream'

    def get_creation_date(self):
        return self.msg.date.timestamp() if self.msg.date else 0

    def get_last_modified(self):
        return self.msg.date.timestamp() if self.msg.date else 0

    def get_display_name(self):
        return self._filename

    def get_etag(self):
        return f'"{self.msg.id}"'

    def get_content(self):
        """下载文件内容"""
        if self._content is None:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                async def _download():
                    data = await self.client.download_media(self.msg, bytes)
                    return data
                self._content = loop.run_until_complete(_download())
                access_logger.info(f"下载文件 {self._filename} ({len(self._content)} 字节)")
            except Exception as e:
                logger.error(f"下载文件 {self._filename} 失败: {e}")
                raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))
            finally:
                loop.close()
        return io.BytesIO(self._content) if self._content else io.BytesIO()

    def begin_write(self, content_type=None):
        """上传文件"""
        self._upload_buffer = io.BytesIO()
        return self._upload_buffer

    def end_write(self, with_errors):
        """完成上传，发送到 Telegram"""
        if with_errors or not hasattr(self, '_upload_buffer'):
            return
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            data = self._upload_buffer.getvalue()
            async def _upload():
                await self.client.send_file(
                    self.chat_id,
                    data,
                    file_name=self._filename,
                )
            loop.run_until_complete(_upload())
            access_logger.info(f"上传文件 {self._filename} ({len(data)} 字节) 到聊天 {self.chat_id}")
        except Exception as e:
            logger.error(f"上传文件 {self._filename} 失败: {e}")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))
        finally:
            loop.close()


class TelegramDAVRoot(dav_provider.DAVCollection):
    """根目录资源，列出所有媒体文件"""

    def __init__(self, path, environ, files: List[TelegramDAVFile]):
        super().__init__(path, environ)
        self._files = files
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
                f'/{name}', self.environ, f.msg, f.client, f.chat_id, name
            )
        return None

    def get_member_list(self):
        access_logger.info(f"列目录 / ({len(self._name_map)} 个文件)")
        result = []
        for name, f in self._name_map.items():
            result.append(TelegramDAVFile(
                f'/{name}', self.environ, f.msg, f.client, f.chat_id, name
            ))
        return result


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