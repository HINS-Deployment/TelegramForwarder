import base64
import logging
import os
import threading
from wsgidav.wsgidav_app import WsgiDAVApp

from models.models import WebDAVAccount, get_session
from webdav.provider import TelegramDAVProvider

logger = logging.getLogger(__name__)

WEBDAV_HOST = os.getenv('WEBDAV_HOST', '0.0.0.0')
WEBDAV_PORT = int(os.getenv('WEBDAV_PORT', '8080'))


class AuthMiddleware:
    """HTTP Basic Auth 中间件，校验 WebDAV 账号"""

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        # 解析 Authorization header
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

        # 验证账号
        session = get_session()
        try:
            account = session.query(WebDAVAccount).filter(
                WebDAVAccount.username == username,
                WebDAVAccount.token == password,
                WebDAVAccount.enabled == True,
            ).first()
            if not account:
                start_response('401 Unauthorized', [
                    ('WWW-Authenticate', 'Basic realm="Telegram WebDAV"'),
                    ('Content-Type', 'text/plain'),
                ])
                return [b'Invalid credentials']
        finally:
            session.close()

        # 将账号信息存入 environ，供 provider 使用
        environ['webdav.account'] = account
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
            # 创建 provider 工厂（每个请求根据账号信息创建独立的 provider）
            class FactoryProvider:
                def __init__(self, user_client, bot_client):
                    self.user_client = user_client
                    self.bot_client = bot_client

                def __call__(self, path, environ):
                    account = environ.get('webdav.account')
                    if not account:
                        return None
                    chat_id = int(account.chat_id)
                    # 如果账号有独立的 bot_token，使用该 bot 客户端
                    if account.bot_token:
                        from telethon import TelegramClient
                        import asyncio
                        # 为这个账号创建一个临时 bot 客户端
                        # 简化处理：使用用户客户端
                        client = self.user_client
                    else:
                        client = self.bot_client if account.bot_token is None else self.user_client
                    provider = TelegramDAVProvider(self.user_client, chat_id)
                    return provider.get_resource_inst(path, environ)

            config = {
                'host': WEBDAV_HOST,
                'port': WEBDAV_PORT,
                'provider_mount': {
                    '/': FactoryProvider(self.user_client, self.bot_client),
                },
                'middleware_stack': [
                    AuthMiddleware,
                ],
                'verbose': 0,
                'acceptbasic': True,
                'acceptdigest': False,
                'defaultdigest': False,
                'dir_browser': {
                    'enable': True,
                    'davmount': False,
                },
                'mount_path': '/',
                'hotfixes': {
                    'recreate_null_resource_on_copy': False,
                    'rename_as_move': False,
                },
            }
            return WsgiDAVApp(config)

        def _run():
            try:
                app = _make_app()
                from wsgidav.server.server_cli import run
                run(app, host=WEBDAV_HOST, port=WEBDAV_PORT)
            except Exception as e:
                logger.error(f"WebDAV 服务器异常: {e}")

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        logger.info(f"WebDAV 服务器已启动在 {WEBDAV_HOST}:{WEBDAV_PORT}")

    def stop(self):
        """停止 WebDAV 服务器"""
        logger.info("WebDAV 服务器已停止")
        # 线程是 daemon 的，主进程退出时自动结束


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