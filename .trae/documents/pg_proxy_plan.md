# SQLite/PostgreSQL 共存 + 代理支持 实施计划

## Context

当前项目数据库层硬编码使用 SQLite，且迁移函数 `migrate_db()` 深度依赖 SQLite 专有语法；TelegramClient 也未配置代理。用户希望：

1. SQLite 与 PostgreSQL 共存，通过 `DATABASE_URL` 切换。
2. PostgreSQL 使用连接 URI 并启用连接池。
3. PostgreSQL 支持自定义表前缀，便于多个部署共享同一个数据库。
4. TelegramClient 支持代理；同时 AI 请求也能走系统代理环境变量。

本计划保持 SQLite 默认行为 100% 兼容，仅对必要的数据库初始化、迁移、客户端创建和依赖配置做最小改动。

## Requirements

- `DATABASE_URL` 驱动数据库选择，默认仍回退到 `sqlite:///./db/forward.db`。
- PostgreSQL 通过 `postgresql+psycopg://...` URI 连接，启用连接池。
- PostgreSQL 下通过 `DATABASE_TABLE_PREFIX` 给所有表名加前缀；SQLite 下前缀无效。
- `TelegramClient`（user+bot）通过 `TELEGRAM_PROXY` 配置 SOCKS5/HTTP 代理。
- AI 请求（OpenAI/Claude/Gemini 等）默认走 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量（httpx 原生支持）。
- 迁移逻辑改为跨数据库通用实现，移除 `sqlite_master`、`AUTOINCREMENT` 等 SQLite 专有代码。

## Recommended Approach

### 1. 新增/调整环境变量（`.env.example`）

```text
# 数据库连接
DATABASE_URL=sqlite:///./db/forward.db
# PostgreSQL 示例：
# DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/forwarder

# PostgreSQL 专用：自定义表前缀（SQLite 无效）
# DATABASE_TABLE_PREFIX=tf1_

# PostgreSQL 连接池参数
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_POOL_TIMEOUT=30
DB_POOL_PRE_PING=true

# Telegram 代理（SOCKS5/HTTP，支持认证）
# TELEGRAM_PROXY=socks5://127.0.0.1:1080
# TELEGRAM_PROXY=socks5://user:pass@127.0.0.1:1080
# TELEGRAM_PROXY=http://127.0.0.1:8080

# AI/HTTP 请求代理（httpx/openai 等会读取）
# HTTP_PROXY=http://127.0.0.1:8080
# HTTPS_PROXY=http://127.0.0.1:8080
# NO_PROXY=localhost,127.0.0.1
```

### 2. 数据库层改造（`models/models.py`）

#### 2.1 环境读取与表名/约束辅助函数

在模型类定义之前添加：

```python
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./db/forward.db').strip()
DATABASE_TABLE_PREFIX = os.getenv('DATABASE_TABLE_PREFIX', '').strip()

_is_pg = DATABASE_URL.startswith(
    ('postgresql://', 'postgres://', 'postgresql+psycopg://', 'postgresql+psycopg2://')
)

def _tablename(base: str) -> str:
    prefix = DATABASE_TABLE_PREFIX if _is_pg else ''
    return f'{prefix}{base}'

def _unique_name(name: str) -> str:
    prefix = DATABASE_TABLE_PREFIX if _is_pg else ''
    return f'{prefix}{name}'
```

#### 2.2 模型表名、外键、唯一约束全部使用辅助函数

所有模型从 `__tablename__ = 'chats'` 改为 `__tablename__ = _tablename('chats')`。

`ForwardRule` 外键示例：

```python
source_chat_id = Column(
    Integer,
    ForeignKey(f"{_tablename('chats')}.id"),
    nullable=False,
)
```

唯一约束示例：

```python
__table_args__ = (
    UniqueConstraint(
        'source_chat_id', 'target_chat_id',
        name=_unique_name('unique_source_target'),
    ),
)
```

对所有模型（`Chat`, `ForwardRule`, `Keyword`, `ReplaceRule`, `MediaTypes`, `MediaExtensions`, `RuleSync`, `PushConfig`, `RSSConfig`, `RSSPattern`, `User`）重复该模式。

#### 2.3 引擎创建函数

```python
def _create_db_engine(url: str):
    if url.startswith(('postgresql://', 'postgres://',
                       'postgresql+psycopg://', 'postgresql+psycopg2://')):
        return create_engine(
            url,
            pool_size=int(os.getenv('DB_POOL_SIZE', 5)),
            max_overflow=int(os.getenv('DB_MAX_OVERFLOW', 10)),
            pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', 30)),
            pool_pre_ping=os.getenv('DB_POOL_PRE_PING', 'true').lower() == 'true',
            pool_recycle=3600,
        )
    return create_engine(
        url,
        connect_args={'check_same_thread': False},
    )
```

#### 2.4 `init_db()` 与 `get_session()`

```python
_engine = None

def init_db():
    global _engine
    url = DATABASE_URL or 'sqlite:///./db/forward.db'

    if url.startswith('sqlite:///') and not url.startswith('sqlite:///:memory:'):
        db_path = url[len('sqlite:///'):]
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    engine = _create_db_engine(url)
    _engine = engine

    Base.metadata.create_all(engine)
    migrate_db(engine)
    return engine

def get_session():
    global _engine
    engine = _engine or init_db()
    return sessionmaker(bind=engine)()
```

#### 2.5 跨数据库迁移 `migrate_db()`

- 所有表名使用 `_tablename()`；裸 SQL 中的表名统一替换为带前缀的表名。
- 检测表存在：用 `inspector.has_table(_tablename('xxx'))` 替代 `sqlite_master` 查询。
- 检测列存在：用 `inspector.get_columns(_tablename('xxx'))`。
- `ALTER TABLE ... ADD COLUMN` 语句改为跨数据库写法，字符串默认值使用单引号：`DEFAULT '07:00'`。
- `mode` -> `forward_mode` 重命名：`ALTER TABLE {table} RENAME COLUMN mode TO forward_mode`（SQLite 与 PG 均支持）。
- `keywords` 唯一约束：先按 `(rule_id, keyword, is_regex, is_blacklist)` 去重，再执行：
  ```sql
  CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
  ON {table} (rule_id, keyword, is_regex, is_blacklist)
  ```
- `selected_media_types` 历史数据迁移中的表名也改用 `_tablename()`。

### 3. `models/db_operations.py` 去裸 SQL

将 `add_media_extensions`、`get_media_extensions`、`delete_media_extensions` 中的 `text()` 原生 SQL 改为 ORM 操作：

```python
existing = session.query(MediaExtensions).filter_by(rule_id=rule_id, extension=ext).first()

extensions = session.query(MediaExtensions).filter_by(rule_id=rule_id).order_by(MediaExtensions.id).all()

session.query(MediaExtensions).filter_by(id=index, rule_id=rule_id).delete()
```

这样表名前缀变更时无需修改此处。

### 4. TelegramClient 代理支持（`main.py`）

新增解析函数：

```python
from urllib.parse import urlparse

def _parse_telegram_proxy(proxy_url: str):
    parsed = urlparse(proxy_url)
    scheme = parsed.scheme.lower()
    if scheme not in ('socks5', 'socks4', 'http', 'https'):
        raise ValueError(f'不支持的代理协议: {scheme}')
    host = parsed.hostname
    port = parsed.port
    username = parsed.username
    password = parsed.password
    if username and password:
        return (scheme, host, port, True, username, password)
    return (scheme, host, port)
```

创建客户端时注入：

```python
proxy_config = _parse_telegram_proxy(os.getenv('TELEGRAM_PROXY')) if os.getenv('TELEGRAM_PROXY') else None

user_client = TelegramClient('./sessions/user', api_id, api_hash, proxy=proxy_config)
bot_client = TelegramClient('./sessions/bot', api_id, api_hash, proxy=proxy_config)
```

AI 请求代理无需代码改动，依赖 `HTTP_PROXY`/`HTTPS_PROXY` 环境变量由 `httpx` 读取。

### 5. 依赖更新（`requirements.txt`）

新增：

```text
psycopg[binary]==3.1.20
python-socks[asyncio]>=2.5.0
```

- `psycopg[binary]`：SQLAlchemy 2.0 官方推荐的 PostgreSQL 驱动。
- `python-socks[asyncio]`：Telethon 可用的异步 SOCKS5/HTTP 代理库。

## Critical Files

- `/home/hins/文档/TelegramForwarder/models/models.py`
- `/home/hins/文档/TelegramForwarder/models/db_operations.py`
- `/home/hins/文档/TelegramForwarder/main.py`
- `/home/hins/文档/TelegramForwarder/requirements.txt`
- `/home/hins/文档/TelegramForwarder/.env.example`

## Verification Plan

### SQLite 默认路径

保持 `.env` 中 `DATABASE_URL=sqlite:///./db/forward.db`，运行：

```bash
python - <<'PY'
from models.models import init_db, get_session, Chat
engine = init_db()
session = get_session()
session.add(Chat(telegram_chat_id='-100123', name='test'))
session.commit()
print(session.query(Chat).first().name)
session.close()
PY
```

预期：表名为 `chats`、`forward_rules` 等，无 `DATABASE_TABLE_PREFIX` 影响，现有数据库可直接使用。

### PostgreSQL 路径

```bash
docker run --rm -d --name pg-forward \
  -e POSTGRES_DB=forwarder \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=pass \
  -p 5432:5432 postgres:15

export DATABASE_URL="postgresql+psycopg://user:pass@localhost:5432/forwarder"
export DATABASE_TABLE_PREFIX="t1_"
export DB_POOL_SIZE=3
export DB_MAX_OVERFLOW=5
```

运行与 SQLite 相同的脚本，验证：

- 表名带前缀：`t1_chats`、`t1_forward_rules` 等。
- 外键约束正常。
- 连接池参数生效（可查看 PG 活动连接数）。

### 代理路径

```bash
python - <<'PY'
import os
os.environ['TELEGRAM_PROXY'] = 'socks5://user:pass@127.0.0.1:1080'
from main import _parse_telegram_proxy
print(_parse_telegram_proxy(os.environ['TELEGRAM_PROXY']))
PY
```

预期输出：`('socks5', '127.0.0.1', 1080, True, 'user', 'pass')`。

启动本地 SOCKS5/HTTP 代理并运行主程序，观察连接是否经过代理；或配置一个不可达代理，确认报错信息中包含代理地址，证明配置已生效。

## Risks & Notes

- `migrate_db()` 中的字符串默认值双引号必须改为单引号，否则 PostgreSQL 会将其解析为标识符。
- 创建 `keywords` 唯一索引前必须先清理重复数据，否则索引创建会失败。
- `DATABASE_TABLE_PREFIX` 是部署时配置，首次运行后不应随意更改，否则旧数据表不会自动迁移。
- SQLite 仍只适用于单实例低并发；PostgreSQL 才是多实例/高并发场景的推荐选择。
