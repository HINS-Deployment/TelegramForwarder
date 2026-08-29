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

# Bot API 文件大小限制（20MB）
BOT_API_SIZE_LIMIT = 20 * 1024 * 1024

# 由 server.py 在启动时设置，供 _run_async 使用
_main_loop = None

# Telethon 全局速率限制（每周期 N 次大文件上下传，避免账号风控）
_telethon_period_start = 0.0
_telethon_operation_count = 0
TELEHON_COOLDOWN_SECONDS = int(os.getenv("WEBDAV_TELEHON_COOLDOWN", "300"))
TELEHON_MAX_PER_CYCLE = int(os.getenv("WEBDAV_TELEHON_MAX_PER_CYCLE", "10"))

# 下载缓存（断点续传）：(chat_id, msg_id) -> {"path": str, "expires": float, "done": bool}
_download_cache = {}
DOWNLOAD_CACHE_TTL = int(os.getenv("WEBDAV_DOWNLOAD_CACHE_TTL", "600"))  # 默认 10 分钟


def set_main_loop(loop):
    global _main_loop
    _main_loop = loop


def _cache_key(msg):
    """生成缓存键 (chat_id, msg_id)。"""
    if msg is None:
        return None
    chat_id = getattr(msg, 'chat_id', None) or getattr(msg, 'chat_id', 0)
    msg_id = getattr(msg, 'id', None) or getattr(msg, 'message_id', 0)
    if not chat_id or not msg_id:
        return None
    return (int(chat_id), int(msg_id))


def _cache_get(key):
    """获取缓存，过期或不存在返回 None。"""
    import time
    if not key or key not in _download_cache:
        return None
    entry = _download_cache[key]
    if time.time() >= entry["expires"]:
        _cache_cleanup(key)
        return None
    if not os.path.exists(entry["path"]):
        _cache_cleanup(key)
        return None
    return entry


def _cache_set(key, path, done=False):
    """设置缓存。"""
    if not key:
        return
    import time
    _download_cache[key] = {
        "path": path,
        "expires": time.time() + DOWNLOAD_CACHE_TTL,
        "done": done,
    }


def _cache_extend(key):
    """延长缓存 TTL。"""
    if not key or key not in _download_cache:
        return
    import time
    _download_cache[key]["expires"] = time.time() + DOWNLOAD_CACHE_TTL


def _cache_cleanup(key=None):
    """清理单个或全部过期缓存。"""
    import time
    if key:
        entry = _download_cache.pop(key, None)
        if entry and os.path.exists(entry["path"]):
            try:
                os.unlink(entry["path"])
            except Exception:
                pass
        return
    # 全量清理
    now = time.time()
    expired = [k for k, v in _download_cache.items() if now >= v["expires"]]
    for k in expired:
        _cache_cleanup(k)


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


class _NamedFile:
    """给文件对象附加 str 类型 name 的代理（Telethon 需要 file.name 为字符串）。

    SpooledTemporaryFile 滚动到磁盘后底层是 TemporaryFile，其 name 是
    int（文件描述符），Telethon 会把它当文件名导致 os.path.splitext(int)
    崩溃。本类包装后返回真实文件名，其余 IO 方法转发到底层对象。
    """

    def __init__(self, f, name):
        self._f = f
        self._name = str(name)

    @property
    def name(self):
        return self._name

    def seekable(self):
        # SpooledTemporaryFile 没有 seekable 方法，但底层确实可 seek。
        # Telethon _FileStream 靠它决定是否用 seek/tell 探测大小；
        # 缺失会导致整文件 read() 进内存 + 复制 BytesIO（大文件内存爆炸）。
        return True

    def __getattr__(self, item):
        return getattr(self._f, item)

    def __iter__(self):
        return iter(self._f)


class _WriteBuffer:
    """写入缓冲区（请求体临时落盘），close 后仍可读取数据。"""
    def __init__(self):
        import tempfile
        # 超过 8MB 自动落盘，避免大文件全量驻留内存
        self._tmp = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)
        self._size = 0
        self._closed = False

    def write(self, data):
        if self._closed:
            raise ValueError("write to closed buffer")
        self._tmp.write(data)
        self._size += len(data)
        return len(data)

    def close(self):
        self._closed = True
        self._tmp.flush()

    def getvalue(self):
        self._tmp.seek(0)
        return self._tmp.read()

    def get_size(self):
        return self._size

    def get_file(self, filename=None):
        """返回已定位到文件头的可读文件对象（供流式上传）。

        SpooledTemporaryFile 超过阈值滚动到磁盘后，底层 TemporaryFile 的
        ``name`` 是 int（文件描述符），Telethon 会把它当文件名导致
        ``os.path.splitext(int)`` 崩溃。这里用代理返回真实文件名。
        """
        self._tmp.flush()
        self._tmp.seek(0)
        if filename:
            return _NamedFile(self._tmp, filename)
        return self._tmp

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _TelethonStream:
    """流式下载 + 缓存到临时文件，支持 seek（断点续传）。
    
    下载完成后临时文件保留在缓存中，同文件后续请求可直接复用。
    """

    def __init__(self, client, msg, cache_key, timeout=3600):
        import asyncio
        import tempfile
        import os
        import threading

        self._loop = _main_loop
        self._timeout = timeout
        self._error = None
        self._done = False
        self._lock = threading.Lock()
        self._cache_key = cache_key

        # 创建临时文件作为缓存
        fd, self._path = tempfile.mkstemp(suffix='.webdav_download')
        os.close(fd)
        self._file = open(self._path, 'wb+')
        self._write_pos = 0
        self._read_pos = 0

        # 注册到缓存
        _cache_set(cache_key, self._path, done=False)

        # 后台下载协程：iter_download → 写入临时文件
        async def _producer():
            import time
            _last_extend = time.time()
            try:
                async for chunk in client.iter_download(msg.media):
                    with self._lock:
                        self._file.write(chunk)
                        self._write_pos += len(chunk)
                        self._file.flush()
                    # 下载中定期刷新缓存 TTL，防止大文件下载超时过期
                    now = time.time()
                    if now - _last_extend > 60:
                        _cache_set(cache_key, self._path, done=False)
                        _last_extend = now
                self._done = True
                # 更新缓存标记为已完成
                _cache_set(cache_key, self._path, done=True)
            except Exception as e:
                self._error = e
                self._done = True

        self._task = asyncio.run_coroutine_threadsafe(_producer(), self._loop)
        # 读取侧的 TTL 刷新
        self._last_read_extend = 0.0

    def _wait_data(self, needed):
        """等待缓存中有足够数据，返回可读字节数。"""
        import time
        start = time.time()
        while True:
            with self._lock:
                if self._error:
                    raise self._error
                if self._done:
                    return max(0, self._write_pos - self._read_pos)
                available = self._write_pos - self._read_pos
                if available >= needed:
                    return available
            # 等待中定期刷新缓存 TTL
            now = time.time()
            if now - self._last_read_extend > 60:
                _cache_extend(self._cache_key)
                self._last_read_extend = now
            if time.time() - start > self._timeout:
                raise TimeoutError("下载流读取超时")
            time.sleep(0.1)

    def read(self, size=-1):
        if self._done and self._error:
            raise self._error

        if size == -1:
            # 等待下载完成，然后返回所有数据
            self._wait_data(float('inf'))
            with self._lock:
                self._file.seek(self._read_pos)
                data = self._file.read()
                self._read_pos = self._file.tell()
                return data
        else:
            # 等待至少 size 字节可用，返回实际可读的数量
            available = self._wait_data(size)
            with self._lock:
                self._file.seek(self._read_pos)
                data = self._file.read(min(size, available))
                actual = len(data)
                self._read_pos += actual
                return data

    def seek(self, offset, whence=0):
        with self._lock:
            if whence == 0:
                self._read_pos = offset
            elif whence == 1:
                self._read_pos += offset
            elif whence == 2:
                self._read_pos = self._write_pos + offset
            # 确保不越界
            self._read_pos = max(0, min(self._read_pos, self._write_pos))
            return self._read_pos

    def tell(self):
        with self._lock:
            return self._read_pos

    def close(self):
        """关闭流，临时文件保留在缓存中供后续断点续传。"""
        self._done = True
        if hasattr(self, '_task') and self._task:
            self._task.cancel()
        if hasattr(self, '_file') and self._file:
            try:
                self._file.close()
            except Exception:
                pass
        # 不删除临时文件，保留在缓存中直到过期
        # 由 _cache_cleanup 负责清理

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
    """流式文件读取，缓存文件不自动删除。"""

    def __init__(self, path, cache_key=None):
        self._path = path
        self._cache_key = cache_key
        self._file = open(path, 'rb')
        self._last_extend = 0.0

    def read(self, size=-1):
        import time
        # 缓存文件读取中定期刷新 TTL
        if self._cache_key:
            now = time.time()
            if now - self._last_extend > 60:
                _cache_extend(self._cache_key)
                self._last_extend = now
        return self._file.read(size)

    def close(self):
        try:
            self._file.close()
        finally:
            # 缓存文件不删除，由 _cache_cleanup 管理
            if not self._cache_key and os.path.exists(self._path):
                try:
                    os.unlink(self._path)
                except Exception:
                    pass

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
        # 上传后记录的消息信息（用于删除操作）
        self._upload_msg_id = None
        self._upload_chat_id = None

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

    def _get_file_size(self):
        """获取文件大小（字节），未知返回 0。"""
        if self._file_id:
            return 0
        if not self.msg or not self.msg.media:
            return 0
        media = self.msg.media
        if isinstance(media, MessageMediaDocument):
            return media.document.size
        if isinstance(media, MessageMediaPhoto):
            return 0
        return 0

    def _is_large_file(self):
        """文件是否超过 Bot API 大小限制（20MB），需要走 Telethon。"""
        size = self._get_file_size()
        return size > BOT_API_SIZE_LIMIT

    def _get_bot_api_chat_id(self):
        """获取 Bot API 兼容的 chat_id（频道需要 -100 前缀）。"""
        cid = self.chat_id
        # 优先从消息获取（已有消息时最为准确）
        if self.msg and hasattr(self.msg, 'chat_id'):
            try:
                return int(self.msg.chat_id)
            except (ValueError, TypeError):
                pass
        # 正数 ID 可能是频道，尝试添加 -100 前缀
        if cid > 0 and not str(cid).startswith('-100'):
            try:
                from telethon.tl.types import Channel
                entity = _run_async(self.client, lambda: self.client.get_entity(cid))
                if isinstance(entity, Channel):
                    return int(f'-100{abs(cid)}')
            except Exception:
                pass
        return cid

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
            return str(self._file_id)
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
        """通过 Telethon MTProto 流式下载大文件，优先复用缓存。"""
        if not self.msg or not self.client:
            return None

        key = _cache_key(self.msg)
        if key:
            cached = _cache_get(key)
            if cached:
                done = cached.get("done", False)
                access_logger.info(f"复用缓存文件 {self._filename} (已完成={done})")
                return _StreamFile(cached["path"], cache_key=key)

        try:
            _check_telethon_rate_limit()
            _increment_telethon_count()
            access_logger.info(f"通过 Telethon 流式下载文件 {self._filename} (大文件回退)")
            return _TelethonStream(self.client, self.msg, key, timeout=3600)
        except Exception as e:
            logger.error(f"Telethon 下载 {self._filename} 失败: {e}")
            return None

    def get_content(self):
        """下载文件内容（>20MB 直接走 Telethon 流式，否则走 Bot API）。"""
        if not self._api_base_url or not self._bot_token:
            raise dav_error.DAVError(dav_error.HTTP_NOT_FOUND, "Bot API not configured")

        # 大文件直接绕过 Bot API
        if self._is_large_file():
            logger.info(f"文件 {self._filename} 超过 {BOT_API_SIZE_LIMIT // 1024 // 1024}MB，直接走 Telethon")
            telethon_result = self._download_via_telethon()
            if telethon_result:
                return telethon_result
            logger.error(f"Telethon 下载 {self._filename} 失败")
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "Telethon download failed")

        try:
            import requests
            base = self._api_base_url.rstrip('/')

            # 小文件走 Bot API（获取 file_id 后再下载）
            if not self._file_id:
                self._get_file_id_via_bot_api()

            data = self._get_file_data(self._api_base_url, self._bot_token, self._file_id)
            if data:
                file_path = data['result']['file_path']
                dl_url = f"{base}/file/bot{self._bot_token}/{file_path}"
                resp = requests.get(dl_url, stream=True, timeout=3600,
                                    proxies={'http': None, 'https': None})
                resp.raise_for_status()
                access_logger.info(f"流式下载文件 {self._filename} (via Bot API)")
                return resp.raw

            # Bot API 失败，回退到 Telethon
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
        logger.info(f"begin_write: {self._filename} (content_type={content_type})")
        self._upload_buffer = _WriteBuffer()
        return self._upload_buffer

    def end_write(self, with_errors):
        """完成上传，发送到 Telegram（>20MB 直接走 Telethon，否则走 Bot API）。"""
        logger.info(f"end_write: {self._filename} (with_errors={with_errors}, has_buffer={hasattr(self, '_upload_buffer')})")
        buffer = getattr(self, '_upload_buffer', None)
        if with_errors or buffer is None:
            logger.info(f"end_write 跳过: with_errors={with_errors}, buffer={buffer}")
            return
        try:
            size = buffer.get_size()
            if not self._api_base_url or not self._bot_token:
                raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                         "Bot API not configured for upload")

            # 大文件直接绕过 Bot API：流式读取临时文件上传，避免整块进内存
            if size > BOT_API_SIZE_LIMIT:
                logger.info(f"上传文件 {self._filename} ({size} 字节) 超过 {BOT_API_SIZE_LIMIT // 1024 // 1024}MB，直接走 Telethon")
                try:
                    self._upload_via_telethon(buffer.get_file(self._filename), size)
                except Exception:
                    logger.error(f"端到端上传 {self._filename} 失败 (via Telethon)", exc_info=True)
                    raise
                from webdav.server import invalidate_cache
                invalidate_cache(self.chat_id)
                return

            # 小文件（<=20MB）进内存走 Bot API
            data = buffer.getvalue()
            try:
                # Bot API 上传（不走系统代理）
                url = f"{self._api_base_url.rstrip('/')}/bot{self._bot_token}/sendDocument"
                import requests
                files = {'document': (self._filename, data)}
                # 必须指定 chat_id，否则 Bot API 不知道发到哪
                bot_chat_id = self._get_bot_api_chat_id()
                data_form = {'chat_id': str(bot_chat_id)}
                resp = requests.post(url, files=files, data=data_form, timeout=120,
                                     proxies={'http': None, 'https': None})
                resp.raise_for_status()
                result = resp.json()
                if result.get('ok'):
                    doc = result.get('result', {}).get('document', {})
                    if doc.get('file_id'):
                        self._file_id = doc['file_id']
                    access_logger.info(f"上传文件 {self._filename} ({len(data)} 字节) (via Bot API)")
                    from webdav.server import invalidate_cache
                    invalidate_cache(self.chat_id)
                    return

                # Bot API 上传失败，尝试 Telethon 回退
                err_desc = result.get('description', 'unknown')
                logger.warning(f"Bot API 上传失败 ({err_desc})，回退到 Telethon")
            except Exception as e:
                # 如果 Bot API 请求本身异常（如文件过大），也尝试 Telethon 回退
                logger.warning(f"Bot API 上传异常 ({e})，回退到 Telethon")

            self._upload_via_telethon(buffer.get_file(self._filename), size)
            from webdav.server import invalidate_cache
            invalidate_cache(self.chat_id)
        finally:
            # 释放缓冲区（临时文件在 GC 时自动删除）
            try:
                buffer.close()
            except Exception:
                pass
            self._upload_buffer = None

    def _upload_via_telethon(self, file_obj, size=0):
        """通过 Telethon MTProto 上传大文件（file_obj 为可读文件对象，流式分片），计入全局冷却。"""
        if not self.client:
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "Telethon 客户端不可用，无法上传")

        _check_telethon_rate_limit()
        from telethon.tl.types import DocumentAttributeFilename

        def _do_upload():
            async def _inner():
                return await self.client.send_file(
                    int(self.chat_id),
                    file_obj,
                    attributes=[DocumentAttributeFilename(self._filename)],
                )
            return _inner()

        try:
            result = _run_async(self.client, _do_upload, timeout=3600)
        except dav_error.DAVError:
            raise
        except Exception as e:
            logger.error(f"Telethon 上传 {self._filename} 异常: {e}", exc_info=True)
            raise dav_error.DAVError(
                dav_error.HTTP_INTERNAL_ERROR,
                f"Telethon 上传失败: {e}",
            )
        if result:
            _increment_telethon_count()
            # 保存上传消息信息（用于删除）
            self._upload_msg_id = result.id
            self._upload_chat_id = getattr(result, 'chat_id', None) or self.chat_id
            # 尝试从结果中提取 file_id
            try:
                media = result.media
                if hasattr(media, 'document') and media.document:
                    self._file_id = media.document.id
                elif hasattr(media, 'photo') and media.photo:
                    self._file_id = str(media.photo.id)
            except Exception:
                pass
            access_logger.info(f"通过 Telethon 上传文件 {self._filename} ({size} 字节) (大文件回退)")
        else:
            raise dav_error.DAVError(dav_error.HTTP_INTERNAL_ERROR,
                                     "Telethon 上传失败")

    def handle_delete(self):
        """处理 DELETE 请求（返回 True 表示已处理，wsgidav 不再递归）"""
        if not self.msg and not self._file_id and not self._upload_msg_id:
            return False
        try:
            # 优先使用上传时记录的消息信息
            msg_id = self._upload_msg_id
            chat_id = self._upload_chat_id or self.chat_id

            if not msg_id and self.msg:
                if isinstance(self.msg, dict):
                    msg_id = self.msg.get('message_id')
                    chat_id = self.msg.get('chat_id') or chat_id
                else:
                    msg_id = self.msg.id

            if not msg_id:
                logger.warning(f"无法删除 {self._filename}：无消息 ID")
                return False

            if self._api_base_url and self._bot_token:
                # Bot API 删除
                bot_chat_id = self._get_bot_api_chat_id()
                self._bot_req('deleteMessage', json={
                    'chat_id': int(bot_chat_id) if bot_chat_id else int(chat_id),
                    'message_id': int(msg_id),
                })
                # 清除缓存，下次请求重新扫描
                from webdav.server import invalidate_cache
                invalidate_cache(self.chat_id)
                access_logger.info(f"删除文件 {self._filename} (消息ID {msg_id}) (via Bot API)")
                return True
            else:
                logger.warning(f"无法删除 {self._filename}：未配置 Bot API")
                return False
        except Exception as e:
            logger.error(f"删除文件 {self._filename} 失败: {e}")
            return False


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