import base64
import logging
import os
import threading
from wsgidav import dav_provider
from wsgidav.wsgidav_app import WsgiDAVApp

from models.models import WebDAVAccount, get_session
from webdav.provider import TelegramDAVRoot, TelegramDAVFile, _get_filename, set_main_loop

logger = logging.getLogger(__name__)

WEBDAV_HOST = os.getenv('WEBDAV_HOST', '0.0.0.0')
WEBDAV_PORT = int(os.getenv('WEBDAV_PORT', '8080'))


class AuthMiddleware:
    """HTTP Basic Auth 中间件，校验 WebDAV 账号"""

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        auth = environ.get('HTTP_AUTHORIZATION', '')
        if not auth.startswith('Basic '):
            start_response('401 Unauthorized', [
                ('WWW-Authenticate', 'Basic realm="Telegram WebDAV"'),
                ('Content-Type', 'text/plain'),
            ])
            return [b'Authentication required']

        try:
            decoded = base64.b64decode(auth[6:]).decode('utf-8')
            username, password = decoded.split(':', 1)
        except Exception:
            start_response('401 Unauthorized', [
                ('WWW-Authenticate', 'Basic realm="Telegram WebDAV"'),
                ('Content-Type', 'text/plain'),
            ])
            return [b'Invalid authentication format']

        logger.info(f"WebDAV 认证尝试: username={username!r}")

        session = get_session()
        try:
            # 先查用户名
            account = session.query(WebDAVAccount).filter(
                WebDAVAccount.username == username,
                WebDAVAccount.enabled == True,
            ).first()
            if not account:
                logger.warning(f"WebDAV 认证失败: 找不到用户 {username!r}")
                start_response('401 Unauthorized', [
                    ('WWW-Authenticate', 'Basic realm="Telegram WebDAV"'),
                    ('Content-Type', 'text/plain'),
                ])
                return [b'Invalid credentials']
            # 验证密码
            if account.token != password:
                logger.warning(f"WebDAV 认证失败: 用户 {username!r} 密码错误")
                start_response('401 Unauthorized', [
                    ('WWW-Authenticate', 'Basic realm="Telegram WebDAV"'),
                    ('Content-Type', 'text/plain'),
                ])
                return [b'Invalid credentials']
        finally:
            session.close()

        environ['webdav.account'] = account
        # 同时设置 REMOTE_USER，让 WsgiDAVApp 内部认证通过
        environ['REMOTE_USER'] = account.username
        return self.app(environ, start_response)


class _WebDAVServer:
    """WebDAV 服务器管理"""

    def __init__(self, user_client, bot_client):
        import asyncio
        self.user_client = user_client
        self.bot_client = bot_client
        self._main_loop = asyncio.get_event_loop()
        # 设置全局主循环引用，供 provider 使用
        set_main_loop(self._main_loop)
        self._thread = None
        self._server = None

    def start(self):
        """启动 WebDAV 服务器（后台线程）"""
        if self._thread and self._thread.is_alive():
            logger.info("WebDAV 服务器已在运行")
            return

        def _make_app():
            config = {
                'host': WEBDAV_HOST,
                'port': WEBDAV_PORT,
                'provider_mapping': {
                    '/': _AccountProvider(self.user_client, self.bot_client, self._main_loop),
                },
                'http_authenticator': {
                    'accept_basic': False,
                    'accept_digest': False,
                    'default_to_digest': False,
                    'trusted_auth_header': 'REMOTE_USER',
                },
                'verbose': 0,
                'dir_browser': {
                    'enable': True,
                    'davmount': False,
                },
                'hotfixes': {
                    'recreate_null_resource_on_copy': False,
                    'rename_as_move': False,
                },
            }
            return AuthMiddleware(WsgiDAVApp(config))

        def _run():
            try:
                app = _make_app()
                # 使用标准库 wsgiref 作为 WSGI 服务器
                from wsgiref.simple_server import make_server
                self._server = make_server(WEBDAV_HOST, WEBDAV_PORT, app)
                logger.info(f"WebDAV 服务器已启动在 {WEBDAV_HOST}:{WEBDAV_PORT}")
                self._server.serve_forever()
            except Exception as e:
                logger.error(f"WebDAV 服务器异常: {e}", exc_info=True)

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self):
        """停止 WebDAV 服务器"""
        if self._server:
            try:
                self._server.shutdown()
                logger.info("WebDAV 服务器已停止")
            except Exception as e:
                logger.error(f"停止 WebDAV 服务器失败: {e}")


class _AccountProvider(dav_provider.DAVProvider):
    """根据已认证账号信息创建对应聊天的 Provider"""

    def __init__(self, user_client, bot_client, main_loop):
        super().__init__()
        self.user_client = user_client
        self.bot_client = bot_client
        self._main_loop = main_loop
        self._cache = {}  # chat_id -> (files, timestamp)

    def _get_files_via_bot_api(self, bot_token, api_base_url, chat_id):
        """通过 Bot API HTTP 获取聊天中的媒体文件列表（走代理域名）。"""
        import httpx
        # 用 getUpdates 获取最近消息
        url = f"{api_base_url.rstrip('/')}/bot{bot_token}/getUpdates"
        params = {'limit': 100, 'allowed_updates': ['message']}
        try:
            resp = httpx.get(url, params=params, timeout=30)
            data = resp.json()
        except Exception as e:
            logger.warning(f"Bot API getUpdates 失败: {e}")
            return []

        if not data.get('ok'):
            logger.warning(f"Bot API 返回错误: {data}")
            return []

        files = []
        name_counts = {}
        for update in data.get('result', []):
            msg = update.get('message') or update.get('channel_post') or {}
            if not msg:
                continue
            # 检查是否有媒体
            media = None
            filename = None
            for media_type in ('document', 'photo', 'video', 'audio', 'voice'):
                m = msg.get(media_type)
                if m:
                    media = m
                    if media_type == 'photo':
                        # photo 是数组，取最后一张最大尺寸
                        if isinstance(m, list) and m:
                            media = m[-1]
                        filename = f"photo_{msg['message_id']}.jpg"
                    elif media_type == 'voice':
                        filename = f"voice_{msg['message_id']}.ogg"
                    else:
                        filename = media.get('file_name') or f"{media_type}_{msg['message_id']}"
                    break
            if not media:
                continue

            # 处理同名文件
            base = filename
            if base in name_counts:
                name_counts[base] += 1
                name = f"{name_counts[base]}_{base}"
            else:
                name_counts[base] = 0
                name = base

            files.append((name, {
                'file_id': media.get('file_id'),
                'file_name': filename,
                'file_size': media.get('file_size', 0),
                'mime_type': media.get('mime_type', 'application/octet-stream'),
                'message_id': msg['message_id'],
                'date': msg.get('date', 0),
                'chat_id': str(chat_id),
            }))
        return files

    def _get_files(self, chat_id, client, bot_token=None, api_base_url=None):
        """获取缓存的文件列表，带 30 秒 TTL。"""
        import time
        now = time.time()
        if chat_id in self._cache:
            files, ts = self._cache[chat_id]
            if now - ts < 30:
                return files

        logger.info(f"WebDAV 扫描聊天 {chat_id} 的媒体文件...")

        if bot_token and api_base_url:
            # 使用 Bot API HTTP 获取（走代理域名）
            files = self._get_files_via_bot_api(bot_token, api_base_url, chat_id)
            logger.info(f"WebDAV 聊天 {chat_id} Bot API 扫描完成，共 {len(files)} 个媒体文件")
        else:
            # 使用 Telethon MTProto 获取
            import asyncio
            async def _fetch():
                msgs = []
                async for msg in client.iter_messages(chat_id, limit=1000):
                    if msg.media and not hasattr(msg, 'action'):
                        msgs.append(msg)
                return msgs
            future = asyncio.run_coroutine_threadsafe(_fetch(), self._main_loop)
            messages = future.result(timeout=60)
            files = []
            name_counts = {}
            for i, msg in enumerate(messages):
                base = _get_filename(msg, i)
                if base in name_counts:
                    name_counts[base] += 1
                    name = f"{name_counts[base]}_{base}"
                else:
                    name_counts[base] = 0
                    name = base
                files.append((name, msg))
            logger.info(f"WebDAV 聊天 {chat_id} MTProto 扫描完成，共 {len(files)} 个媒体文件")

        self._cache[chat_id] = (files, now)
        return files

    def get_resource_inst(self, path, environ):
        account = environ.get('webdav.account')
        if not account:
            logger.warning(f"WebDAV 请求未认证: {path}")
            return None

        chat_id = int(account.chat_id)
        client = self.user_client
        bot_token = account.bot_token
        api_base_url = account.api_base_url

        logger.info(f"WebDAV 请求: path={path!r} chat_id={chat_id}")

        files = self._get_files(chat_id, client, bot_token=bot_token, api_base_url=api_base_url)

        if path == '/' or path == '':
            logger.info(f"WebDAV 返回根目录 ({len(files)} 个文件)")
            name_map = {name: msg for name, msg in files}
            return TelegramDAVRoot('/', environ, [
                TelegramDAVFile(f'/{name}', environ, msg, client, chat_id, name,
                                bot_token=bot_token, api_base_url=api_base_url)
                for name, msg in name_map.items()
            ], client=client, chat_id=chat_id, bot_token=bot_token, api_base_url=api_base_url)

        name = path.lstrip('/')
        for n, msg in files:
            if n == name:
                return TelegramDAVFile(path, environ, msg, client, chat_id, name,
                                       bot_token=bot_token, api_base_url=api_base_url)

        # 文件不存在时，如果是 PUT 请求，返回一个可写入的空资源
        # 这由 create_empty_resource 处理，但 get_resource_inst 可能被先调用
        logger.warning(f"WebDAV 文件未找到: {path!r}")
        return None


# 全局管理器实例
_webdav_server = None


def get_webdav_server():
    global _webdav_server
    return _webdav_server


def init_webdav_server(user_client, bot_client):
    global _webdav_server
    if _webdav_server is None:
        _webdav_server = _WebDAVServer(user_client, bot_client)
    return _webdav_server