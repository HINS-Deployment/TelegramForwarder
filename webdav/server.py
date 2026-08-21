import base64
import logging
import os
import threading
from wsgidav.wsgidav_app import WsgiDAVApp

from models.models import WebDAVAccount, get_session
from webdav.provider import TelegramDAVProvider, TelegramDAVRoot, TelegramDAVFile, _iter_media_messages, _get_filename

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
        self.user_client = user_client
        self.bot_client = bot_client
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
                'provider_mount': {
                    '/': _AccountProvider(self.user_client, self.bot_client),
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
            # 用自定义认证中间件包裹 WsgiDAVApp（不通过 middleware_stack，它只接受 BaseMiddleware 子类）
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


class _AccountProvider(TelegramDAVProvider):
    """根据已认证账号信息创建对应聊天的 Provider"""

    def __init__(self, user_client, bot_client):
        self.user_client = user_client
        self.bot_client = bot_client
        self._cache = {}  # chat_id -> (files, provider)

    def get_resource_inst(self, path, environ):
        account = environ.get('webdav.account')
        if not account:
            logger.warning(f"WebDAV 请求未认证: {path}")
            return None

        chat_id = int(account.chat_id)
        client = self.user_client

        logger.info(f"WebDAV 请求: path={path!r} chat_id={chat_id}")

        # 根据账号缓存文件列表
        if chat_id not in self._cache:
            logger.info(f"WebDAV 扫描聊天 {chat_id} 的媒体文件...")
            messages = _iter_media_messages(client, chat_id)
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
            self._cache[chat_id] = files
            logger.info(f"WebDAV 聊天 {chat_id} 扫描完成，共 {len(files)} 个媒体文件")

        files = self._cache[chat_id]

        if path == '/' or path == '':
            logger.info(f"WebDAV 返回根目录 ({len(files)} 个文件)")
            name_map = {name: msg for name, msg in files}
            return TelegramDAVRoot('/', environ, [
                TelegramDAVFile(f'/{name}', environ, msg, client, chat_id, name)
                for name, msg in name_map.items()
            ])

        name = path.lstrip('/')
        for n, msg in files:
            if n == name:
                return TelegramDAVFile(path, environ, msg, client, chat_id, name)

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