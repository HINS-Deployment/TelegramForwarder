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

# Telethon 全局速率限制（每周期 N 次大文件上下传，避免账号风控）
_telethon_period_start = 0.0
_telethon_operation_count = 0
TELEHON_COOLDOWN_SECONDS = int(os.getenv("WEBDAV_TELEHON_COOLDOWN", "300"))
TELEHON_MAX_PER_CYCLE = int(os.getenv("WEBDAV_TELEHON_MAX_PER_CYCLE", "10"))


def set_main_loop(loop):
    global _main_loop
    _main_loop = loop


def _check_telethon_rate_limit():
    """检查 Telethon 速率限制，等待直到允许操作。"""
    global _telethon_period_start, _telethon_operation_count
    import time
    now = time.time()

    # 首次使用或周期已过期，重置计数器
    if _telethon_period_start == 0 or now - _telethon_period_start >= TELEHON_COOLDOWN_SECONDS:
        _telethon_period_start = now
        _telethon_operation_count = 0
        return

    # 已达到周期内最大次数，等待周期结束
    if _telethon_operation_count >= TELEHON_MAX_PER_CYCLE:
        elapsed = now - _telethon_period_start
        wait = TELEHON_COOLDOWN_SECONDS - elapsed
        if wait > 0:
            logger.info(f"Telethon 周期内已达上限 ({_telethon_operation_count}/{TELEHON_MAX_PER_CYCLE})，等待 {wait:.0f} 秒...")
            time.sleep(wait)
        # 重置新周期
        _telethon_period_start = time.time()
        _telethon_operation_count = 0


def _increment_telethon_count():
    """增加 Telethon 操作计数并记录日志。"""
    global _telethon_operation_count
    _telethon_operation_count += 1
    remain = TELEHON_MAX_PER_CYCLE - _telethon_operation_count
    logger.info(f"Telethon 操作计数: {_telethon_operation_count}/{TELEHON_MAX_PER_CYCLE} (本周期剩余 {remain} 次)")


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


class _TelethonStream:
    """流式读取 Telethon 下载数据，边下载边透传给客户端。"""

    def __init__(self, client, msg, timeout=3600):
        import asyncio
        self._loop = _main_loop
        self._queue = asyncio.Queue(maxsize=64)
        self._error = None
        self._done = False
        self._timeout = timeout

        # 在事件循环中启动后台下载任务
        async def _producer():
            try:
                async for chunk in client.iter_download(msg.media):
                    await self._queue.put(chunk)
                await self._queue.put(None)  # 结束标记
            except Exception as e:
                self._error = e
                await self._queue.put(None)

        self._task = asyncio.run_coroutine_threadsafe(_producer(), self._loop)

    def read(self, size=-1):
        import asyncio
        if self._done:
            return b''

        chunks = []
        remaining = size
        while remaining != 0:
            future = asyncio.run_coroutine_threadsafe(self._queue.get(), self._loop)
            try:
                chunk = future.result(timeout=self._timeout)
            except Exception as e:
                raise TimeoutError(f"下载流读取超时: {e}")

            if chunk is None:
                self._done = True
                break

            if self._error:
                raise self._error

            if size == -1:
                chunks.append(chunk)
            else:
                take = min(len(chunk), remaining)
                chunks.append(chunk[:take])
                remaining -= take
                if take < len(chunk):
                    # 未消费的部分放回队列
                    asyncio.run_coroutine_threadsafe(
                        self._queue.put(chunk[take:]), self._loop
                    )
                    break

        return b''.join(chunks)

    def close(self):
        self._done = True
        if hasattr(self, '_task') and self._task:
            self._task.cancel()

    def __iter__(self):
        return self

    def __next__(self):
        data = self.read(65536)
        if not data:
            raise StopIteration
        return data

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

    def _get_file_data(self, api_base_url, bot_token, file_id):
        """调用 Bot API getFile 获取文件信息，先 POST 再 GET 回退。"""
        import requests
        base = api_base_url.rstrip('/')
        url = f"{base}/bot{bot_token}/getFile"
        kwargs = {'timeout': 30, 'proxies': {'http': None, 'https': None}}

        # 先尝试 POST + JSON body（某些代理不支持 GET）
        try:
            gf_resp = requests.post(url, json={'file_id': file_id}, **kwargs)
            data = gf_resp.json()
            if data.get('ok'):
                return data
            logger.warning(f"getFile POST 失败: {data.get('description', 'unknown')}")
        except Exception as e:
            logger.warning(f"getFile POST 异常: {e}")

        # 回退到 GET + query params
        try:
            gf_resp = requests.get(url, params={'file_id': file_id}, **kwargs)
            data = gf_resp.json()
            if data.get('ok'):
                return data
            logger.warning(f"getFile GET 失败: {data.get('description', 'unknown')}")
        except Exception as e:
            logger.warning(f"getFile GET 异常: {e}")

        # 都失败时，尝试官方 API（不走系统代理）
        if 'api.telegram.org' not in api_base_url:
            try:
                official_url = f"https://api.telegram.org/bot{bot_token}/getFile"
                gf_resp = requests.post(official_url, json={'file_id': file_id},
                                        timeout=30, proxies={'http': None, 'https': None})
                data = gf_resp.json()
                if data.get('ok'):
                    access_logger.info("getFile 通过官方 API 回退成功")
                    return data
                logger.error(f"getFile 官方 API 也失败: {data.get('description', 'unknown')}")
            except Exception as e:
                logger.error(f"getFile 官方 API 异常: {e}")

        return None

    def _download_via_telethon(self):
        """通过 Telethon MTProto 流式下载大文件，实时透传客户端。"""
        if not self.msg or not self.client:
            return None

        try:
            _check_telethon_rate_limit()
            _increment_telethon_count()
            access_logger.info(f"通过 Telethon 流式下载文件 {self._filename} (大文件回退)")
            return _TelethonStream(self.client, self.msg, timeout=3600)
        except Exception as e:
            logger.error(f"Telethon 下载 {self._filename} 失败: {e}")
            return None

    def get_content(self):
        """下载文件内容（优先 Bot API，大文件回退到 Telethon）"""
        if not self._file_id:
            self._get_file_id_via_bot_api()

        if not self._api_base_url or not self._bot_token:
            raise dav_error.DAVError(dav_error.HTTP_NOT_FOUND, "Bot API not configured")

        try:
            import requests
            base = self._api_base_url.rstrip('/')

            data = self._get_file_data(self._api_base_url, self._bot_token, self._file_id)
            if data:
                file_path = data['result']['file_path']
                dl_url = f"{base}/file/bot{self._bot_token}/{file_path}"
                resp = requests.get(dl_url, stream=True, timeout=3600,
                                    proxies={'http': None, 'https': None})
                resp.raise_for_status()
                access_logger.info(f"流式下载文件 {self._filename} (via Bot API)")
                return resp.raw

            # Bot API 失败（通常是大文件 > 20MB），回退到 Telethon
            logger.warning(f"Bot API 无法下载 {self._filename}，回退到 Telethon")
            telethon_result = self._download_via_telethon()
            if telethon_result:
                return telethon_result

            logger.error(f"getFile 所有方式均失败 (file_id={self._file_id[:20]}...)")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "getFile failed after all retries")
        except dav_error.DAVError:
            raise
        except Exception as e:
            logger.error(f"下载文件 {self._filename} 失败: {e}")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR, str(e))

    def begin_write(self, content_type=None):
        """上传文件"""
        self._upload_buffer = _WriteBuffer()
        return self._upload_buffer

    def end_write(self, with_errors):
        """完成上传，发送到 Telegram（优先 Bot API，大文件回退到 Telethon）。"""
        if with_errors or not hasattr(self, '_upload_buffer'):
            return
        data = self._upload_buffer.getvalue()
        if not self._api_base_url or not self._bot_token:
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "Bot API not configured for upload")

        try:
            # Bot API 上传（不走系统代理）
            url = f"{self._api_base_url.rstrip('/')}/bot{self._bot_token}/sendDocument"
            import requests
            files = {'document': (self._filename, data)}
            resp = requests.post(url, files=files, timeout=120, proxies={'http': None, 'https': None})
            resp.raise_for_status()
            result = resp.json()
            if result.get('ok'):
                doc = result.get('result', {}).get('document', {})
                if doc.get('file_id'):
                    self._file_id = doc['file_id']
                access_logger.info(f"上传文件 {self._filename} ({len(data)} 字节) (via Bot API)")
                return

            # Bot API 上传失败，尝试 Telethon 回退
            err_desc = result.get('description', 'unknown')
            logger.warning(f"Bot API 上传失败 ({err_desc})，回退到 Telethon")
        except Exception as e:
            # 如果 Bot API 请求本身异常（如文件过大），也尝试 Telethon 回退
            logger.warning(f"Bot API 上传异常 ({e})，回退到 Telethon")

        self._upload_via_telethon(data)

    def _upload_via_telethon(self, data):
        """通过 Telethon MTProto 上传大文件，计入全局冷却。"""
        if not self.client:
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "Telethon 客户端不可用，无法上传")

        _check_telethon_rate_limit()
        import io
        from telethon.tl.types import DocumentAttributeFilename

        file_data = io.BytesIO(data)
        file_data.name = self._filename

        def _do_upload():
            async def _inner():
                return await self.client.send_file(
                    int(self.chat_id),
                    file_data,
                    attributes=[DocumentAttributeFilename(self._filename)],
                )
            return _inner()

        result = _run_async(self.client, _do_upload, timeout=3600)
        if result:
            _increment_telethon_count()
            # 尝试从结果中提取 file_id
            try:
                media = result.media
                if hasattr(media, 'document') and media.document:
                    self._file_id = media.document.id
                elif hasattr(media, 'photo') and media.photo:
                    self._file_id = str(media.photo.id)
            except Exception:
                pass
            access_logger.info(f"通过 Telethon 上传文件 {self._filename} ({len(data)} 字节) (大文件回退)")
        else:
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "Telethon 上传失败")

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