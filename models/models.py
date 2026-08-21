from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, Enum, UniqueConstraint, inspect, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from enums.enums import ForwardMode, PreviewMode, MessageMode, AddMode, HandleMode
import logging
import os
from dotenv import load_dotenv

load_dotenv()
Base = declarative_base()

# ---------------------------------------------------------------------------
# 数据库连接配置与表名辅助函数
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./db/forward.db').strip()
DATABASE_TABLE_PREFIX = os.getenv('DATABASE_TABLE_PREFIX', '').strip()

_is_pg = DATABASE_URL.startswith(
    ('postgresql://', 'postgres://', 'postgresql+psycopg://', 'postgresql+psycopg2://')
)


def _tablename(base: str) -> str:
    """根据数据库类型返回带前缀或不带前缀的表名。"""
    prefix = DATABASE_TABLE_PREFIX if _is_pg else ''
    return f'{prefix}{base}'


def _unique_name(name: str) -> str:
    """唯一约束/索引名称也加前缀，避免多部署共享 PG 时冲突。"""
    prefix = DATABASE_TABLE_PREFIX if _is_pg else ''
    return f'{prefix}{name}'


# ---------------------------------------------------------------------------
# 模型定义
# ---------------------------------------------------------------------------
class Chat(Base):
    __tablename__ = _tablename('chats')

    id = Column(Integer, primary_key=True)
    telegram_chat_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=True)
    current_add_id = Column(String, nullable=True)

    # 关系
    source_rules = relationship('ForwardRule', foreign_keys='ForwardRule.source_chat_id', back_populates='source_chat')
    target_rules = relationship('ForwardRule', foreign_keys='ForwardRule.target_chat_id', back_populates='target_chat')


class ForwardRule(Base):
    __tablename__ = _tablename('forward_rules')

    id = Column(Integer, primary_key=True)
    source_chat_id = Column(Integer, ForeignKey(f"{_tablename('chats')}.id"), nullable=False)
    target_chat_id = Column(Integer, ForeignKey(f"{_tablename('chats')}.id"), nullable=False)
    forward_mode = Column(Enum(ForwardMode), nullable=False, default=ForwardMode.BLACKLIST)
    use_bot = Column(Boolean, default=True)
    message_mode = Column(Enum(MessageMode), nullable=False, default=MessageMode.MARKDOWN)
    is_replace = Column(Boolean, default=False)
    is_preview = Column(Enum(PreviewMode), nullable=False, default=PreviewMode.FOLLOW)  # 三个值，开，关，按照原消息
    is_original_link = Column(Boolean, default=False)   # 是否附带原消息链接
    is_ufb = Column(Boolean, default=False)
    ufb_domain = Column(String, nullable=True)
    ufb_item = Column(String, nullable=True, default='main')
    is_delete_original = Column(Boolean, default=False)  # 是否删除原始消息
    is_original_sender = Column(Boolean, default=False)  # 是否附带原始消息发送人名称
    userinfo_template = Column(String, default='**{name}**', nullable=True)  # 用户信息模板
    time_template = Column(String, default='{time}', nullable=True)  # 时间模板
    original_link_template = Column(String, default='原始连接：{original_link}', nullable=True)  # 原始链接模板
    is_original_time = Column(Boolean, default=False)  # 是否附带原始消息发送时间
    add_mode = Column(Enum(AddMode), nullable=False, default=AddMode.BLACKLIST)  # 添加模式,默认黑名单
    enable_rule = Column(Boolean, default=True)  # 是否启用规则
    is_filter_user_info = Column(Boolean, default=False)  # 是否过滤用户信息
    handle_mode = Column(Enum(HandleMode), nullable=False, default=HandleMode.FORWARD)  # 处理模式,编辑模式和转发模式，默认转发
    enable_comment_button = Column(Boolean, default=False)  # 是否添加对应消息的评论区直达按钮
    enable_media_type_filter = Column(Boolean, default=False)  # 是否启用媒体类型过滤
    enable_media_size_filter = Column(Boolean, default=False)  # 是否启用媒体大小过滤
    max_media_size = Column(Integer, default=os.getenv('DEFAULT_MAX_MEDIA_SIZE', 10))  # 媒体大小限制，单位MB
    is_send_over_media_size_message = Column(Boolean, default=True)  # 超过限制的媒体是否发送提示消息
    enable_extension_filter = Column(Boolean, default=False)  # 是否启用媒体扩展名过滤
    extension_filter_mode = Column(Enum(AddMode), nullable=False, default=AddMode.BLACKLIST)  # 媒体扩展名过滤模式，默认黑名单
    enable_reverse_blacklist = Column(Boolean, default=False)  # 是否反转黑名单
    enable_reverse_whitelist = Column(Boolean, default=False)  # 是否反转白名单
    media_allow_text = Column(Boolean, default=False)  # 是否放行文本
    # 推送相关字段
    enable_push = Column(Boolean, default=False)  # 是否启用推送
    enable_only_push = Column(Boolean, default=False)  # 是否只转发到推送配置

    # AI相关字段
    is_ai = Column(Boolean, default=False)  # 是否启用AI处理
    ai_model = Column(String, nullable=True)  # 使用的AI模型
    ai_prompt = Column(String, nullable=True)  # AI处理的prompt
    enable_ai_upload_image = Column(Boolean, default=False)  # 是否启用AI图片上传功能
    is_summary = Column(Boolean, default=False)  # 是否启用AI总结
    summary_time = Column(String(5), default=os.getenv('DEFAULT_SUMMARY_TIME', '07:00'))
    summary_prompt = Column(String, nullable=True)  # AI总结的prompt
    is_keyword_after_ai = Column(Boolean, default=False)  # AI处理后是否再次执行关键字过滤
    is_top_summary = Column(Boolean, default=True)  # 是否顶置总结消息
    enable_delay = Column(Boolean, default=False)  # 是否启用延迟处理
    delay_seconds = Column(Integer, default=5)  # 延迟处理秒数
    # RSS相关字段
    only_rss = Column(Boolean, default=False)  # 是否只转发RSS
    # 同步功能相关
    enable_sync = Column(Boolean, default=False)  # 是否启用规则同步功能

    # 添加唯一约束
    __table_args__ = (
        UniqueConstraint('source_chat_id', 'target_chat_id', name=_unique_name('unique_source_target')),
    )

    # 关系
    source_chat = relationship('Chat', foreign_keys=[source_chat_id], back_populates='source_rules')
    target_chat = relationship('Chat', foreign_keys=[target_chat_id], back_populates='target_rules')
    keywords = relationship('Keyword', back_populates='rule')
    replace_rules = relationship('ReplaceRule', back_populates='rule', cascade="all, delete-orphan")
    media_types = relationship('MediaTypes', uselist=False, back_populates='rule', cascade="all, delete-orphan")
    media_extensions = relationship('MediaExtensions', back_populates='rule', cascade="all, delete-orphan")
    rss_config = relationship('RSSConfig', uselist=False, back_populates='rule', cascade="all, delete-orphan")
    rule_syncs = relationship('RuleSync', back_populates='rule', cascade="all, delete-orphan")
    push_config = relationship('PushConfig', uselist=False, back_populates='rule', cascade="all, delete-orphan")


class Keyword(Base):
    __tablename__ = _tablename('keywords')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False)
    keyword = Column(String, nullable=True)
    is_regex = Column(Boolean, default=False)
    is_blacklist = Column(Boolean, default=True)

    # 关系
    rule = relationship('ForwardRule', back_populates='keywords')

    # 添加唯一约束
    __table_args__ = (
        UniqueConstraint('rule_id', 'keyword', 'is_regex', 'is_blacklist', name=_unique_name('unique_rule_keyword_is_regex_is_blacklist')),
    )


class ReplaceRule(Base):
    __tablename__ = _tablename('replace_rules')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False)
    pattern = Column(String, nullable=False)  # 替换模式
    content = Column(String, nullable=True)   # 替换内容

    # 关系
    rule = relationship('ForwardRule', back_populates='replace_rules')

    # 添加唯一约束
    __table_args__ = (
        UniqueConstraint('rule_id', 'pattern', 'content', name=_unique_name('unique_rule_pattern_content')),
    )


class MediaTypes(Base):
    __tablename__ = _tablename('media_types')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False, unique=True)
    photo = Column(Boolean, default=False)
    document = Column(Boolean, default=False)
    video = Column(Boolean, default=False)
    audio = Column(Boolean, default=False)
    voice = Column(Boolean, default=False)

    # 关系
    rule = relationship('ForwardRule', back_populates='media_types')


class MediaExtensions(Base):
    __tablename__ = _tablename('media_extensions')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False)
    extension = Column(String, nullable=False)  # 存储不带点的扩展名，如 "jpg", "pdf"

    # 关系
    rule = relationship('ForwardRule', back_populates='media_extensions')

    # 添加唯一约束
    __table_args__ = (
        UniqueConstraint('rule_id', 'extension', name=_unique_name('unique_rule_extension')),
    )


class RuleSync(Base):
    __tablename__ = _tablename('rule_syncs')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False)
    sync_rule_id = Column(Integer, nullable=False)

    # 关系
    rule = relationship('ForwardRule', back_populates='rule_syncs')


class PushConfig(Base):
    __tablename__ = _tablename('push_configs')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False)
    enable_push_channel = Column(Boolean, default=False)
    push_channel = Column(String, nullable=False)
    # 媒体发送方式，一次一张Single还是多张Multiple
    media_send_mode = Column(String, nullable=False, default='Single')

    # 关系
    rule = relationship('ForwardRule', back_populates='push_config')


class RSSConfig(Base):
    __tablename__ = _tablename('rss_configs')

    id = Column(Integer, primary_key=True)
    rule_id = Column(Integer, ForeignKey(f"{_tablename('forward_rules')}.id"), nullable=False, unique=True)
    enable_rss = Column(Boolean, default=False)  # 是否启用RSS
    rule_title = Column(String, nullable=True)  # RSS feed 标题
    rule_description = Column(String, nullable=True)  # RSS feed 描述
    language = Column(String, default='zh-CN')  # RSS feed 语言
    max_items = Column(Integer, default=50)  # RSS feed 最大条目数
    # 是否启用自动提取标题和内容
    is_auto_title = Column(Boolean, default=False)
    is_auto_content = Column(Boolean, default=False)
    # 是否启用ai提取标题和内容
    is_ai_extract = Column(Boolean, default=False)
    # ai提取标题和内容的prompt
    ai_extract_prompt = Column(String, nullable=True)
    is_auto_markdown_to_html = Column(Boolean, default=False)
    # 是否启用自定义提取标题和内容的正则表达式
    enable_custom_title_pattern = Column(Boolean, default=False)
    enable_custom_content_pattern = Column(Boolean, default=False)

    # 关系
    rule = relationship('ForwardRule', back_populates='rss_config')
    patterns = relationship('RSSPattern', back_populates='rss_config', cascade="all, delete-orphan")


class RSSPattern(Base):
    __tablename__ = _tablename('rss_patterns')

    id = Column(Integer, primary_key=True)
    rss_config_id = Column(Integer, ForeignKey(f"{_tablename('rss_configs')}.id"), nullable=False)
    pattern = Column(String, nullable=False)  # 正则表达式模式
    pattern_type = Column(String, nullable=False)  # 模式类型: 'title' 或 'content'
    priority = Column(Integer, default=0)  # 执行优先级,数字越小优先级越高

    # 关系
    rss_config = relationship('RSSConfig', back_populates='patterns')

    # 添加联合唯一约束
    __table_args__ = (
        UniqueConstraint('rss_config_id', 'pattern', 'pattern_type', name=_unique_name('unique_rss_pattern')),
    )


class User(Base):
    __tablename__ = _tablename('users')

    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    password = Column(String, nullable=False)


# ---------------------------------------------------------------------------
# 引擎创建 / 初始化 / 会话
# ---------------------------------------------------------------------------
_engine = None


def _create_db_engine(url: str):
    """根据数据库 URI 创建合适的引擎。"""
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


def init_db():
    """初始化数据库"""
    global _engine
    url = DATABASE_URL or 'sqlite:///./db/forward.db'

    if url.startswith('sqlite:///') and not url.startswith('sqlite:///:memory:'):
        db_path = url[len('sqlite:///'):]
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    engine = _create_db_engine(url)
    _engine = engine

    # 首先创建所有表
    Base.metadata.create_all(engine)

    # 然后进行必要的迁移
    migrate_db(engine)

    return engine


def get_session():
    """创建会话工厂"""
    global _engine
    engine = _engine or init_db()
    Session = sessionmaker(bind=engine)
    return Session()


# ---------------------------------------------------------------------------
# 跨数据库迁移
# ---------------------------------------------------------------------------
def migrate_db(engine):
    """数据库迁移函数，确保新字段的添加以及历史数据迁移。"""
    inspector = inspect(engine)
    dialect = engine.dialect.name

    t_rule_syncs = _tablename('rule_syncs')
    t_users = _tablename('users')
    t_rss_configs = _tablename('rss_configs')
    t_rss_patterns = _tablename('rss_patterns')
    t_push_configs = _tablename('push_configs')
    t_media_types = _tablename('media_types')
    t_media_extensions = _tablename('media_extensions')
    t_forward_rules = _tablename('forward_rules')
    t_keywords = _tablename('keywords')

    # -----------------------------------------------------------------------
    # 1. 创建缺失的表（兼容从旧版本升级的情况）
    # -----------------------------------------------------------------------
    with engine.begin() as connection:
        for table_name, model in [
            (t_rule_syncs, RuleSync),
            (t_users, User),
            (t_rss_configs, RSSConfig),
            (t_rss_patterns, RSSPattern),
            (t_push_configs, PushConfig),
            (t_media_types, MediaTypes),
            (t_media_extensions, MediaExtensions),
        ]:
            if not inspector.has_table(table_name):
                logging.info(f"创建 {table_name} 表...")
                model.__table__.create(engine)

        # -------------------------------------------------------------------
        # 2. 旧版 selected_media_types 字段迁移到 media_types 表
        # -------------------------------------------------------------------
        forward_rules_columns = {
            column['name']
            for column in inspector.get_columns(t_forward_rules)
        } if inspector.has_table(t_forward_rules) else set()

        if inspector.has_table(t_media_types) and 'selected_media_types' in forward_rules_columns:
            logging.info("迁移媒体类型数据到新表...")
            rules = connection.execute(text(
                f"SELECT id, selected_media_types FROM {t_forward_rules} WHERE selected_media_types IS NOT NULL"
            ))

            for rule in rules:
                rule_id = rule[0]
                selected_types = rule[1]
                if selected_types:
                    media_types_data = {
                        'photo': 'photo' in selected_types,
                        'document': 'document' in selected_types,
                        'video': 'video' in selected_types,
                        'audio': 'audio' in selected_types,
                        'voice': 'voice' in selected_types,
                    }
                    connection.execute(
                        text(f"""
                            INSERT INTO {t_media_types} (rule_id, photo, document, video, audio, voice)
                            VALUES (:rule_id, :photo, :document, :video, :audio, :voice)
                        """),
                        {
                            'rule_id': rule_id,
                            'photo': media_types_data['photo'],
                            'document': media_types_data['document'],
                            'video': media_types_data['video'],
                            'audio': media_types_data['audio'],
                            'voice': media_types_data['voice'],
                        }
                    )

    # -----------------------------------------------------------------------
    # 3. 增量添加缺失列
    # -----------------------------------------------------------------------
    keyword_columns = {
        column['name']
        for column in inspector.get_columns(t_keywords)
    } if inspector.has_table(t_keywords) else set()

    forward_rules_new_columns = {
        'is_ai': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_ai BOOLEAN DEFAULT FALSE',
        'ai_model': f'ALTER TABLE {t_forward_rules} ADD COLUMN ai_model VARCHAR DEFAULT NULL',
        'ai_prompt': f'ALTER TABLE {t_forward_rules} ADD COLUMN ai_prompt VARCHAR DEFAULT NULL',
        'is_summary': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_summary BOOLEAN DEFAULT FALSE',
        'summary_time': f"ALTER TABLE {t_forward_rules} ADD COLUMN summary_time VARCHAR DEFAULT '07:00'",
        'summary_prompt': f'ALTER TABLE {t_forward_rules} ADD COLUMN summary_prompt VARCHAR DEFAULT NULL',
        'is_delete_original': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_delete_original BOOLEAN DEFAULT FALSE',
        'is_original_sender': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_original_sender BOOLEAN DEFAULT FALSE',
        'is_original_time': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_original_time BOOLEAN DEFAULT FALSE',
        'is_keyword_after_ai': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_keyword_after_ai BOOLEAN DEFAULT FALSE',
        'add_mode': f"ALTER TABLE {t_forward_rules} ADD COLUMN add_mode VARCHAR DEFAULT 'BLACKLIST'",
        'enable_rule': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_rule BOOLEAN DEFAULT TRUE',
        'is_top_summary': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_top_summary BOOLEAN DEFAULT TRUE',
        'is_filter_user_info': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_filter_user_info BOOLEAN DEFAULT FALSE',
        'enable_delay': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_delay BOOLEAN DEFAULT FALSE',
        'delay_seconds': f'ALTER TABLE {t_forward_rules} ADD COLUMN delay_seconds INTEGER DEFAULT 5',
        'handle_mode': f"ALTER TABLE {t_forward_rules} ADD COLUMN handle_mode VARCHAR DEFAULT 'FORWARD'",
        'enable_comment_button': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_comment_button BOOLEAN DEFAULT FALSE',
        'enable_media_type_filter': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_media_type_filter BOOLEAN DEFAULT FALSE',
        'enable_media_size_filter': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_media_size_filter BOOLEAN DEFAULT FALSE',
        'max_media_size': f'ALTER TABLE {t_forward_rules} ADD COLUMN max_media_size INTEGER DEFAULT {os.getenv("DEFAULT_MAX_MEDIA_SIZE", 10)}',
        'is_send_over_media_size_message': f'ALTER TABLE {t_forward_rules} ADD COLUMN is_send_over_media_size_message BOOLEAN DEFAULT TRUE',
        'enable_extension_filter': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_extension_filter BOOLEAN DEFAULT FALSE',
        'extension_filter_mode': f"ALTER TABLE {t_forward_rules} ADD COLUMN extension_filter_mode VARCHAR DEFAULT 'BLACKLIST'",
        'enable_reverse_blacklist': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_reverse_blacklist BOOLEAN DEFAULT FALSE',
        'enable_reverse_whitelist': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_reverse_whitelist BOOLEAN DEFAULT FALSE',
        'only_rss': f'ALTER TABLE {t_forward_rules} ADD COLUMN only_rss BOOLEAN DEFAULT FALSE',
        'enable_sync': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_sync BOOLEAN DEFAULT FALSE',
        'userinfo_template': f"ALTER TABLE {t_forward_rules} ADD COLUMN userinfo_template VARCHAR DEFAULT '**{{name}}**'",
        'time_template': f"ALTER TABLE {t_forward_rules} ADD COLUMN time_template VARCHAR DEFAULT '{{time}}'",
        'original_link_template': f"ALTER TABLE {t_forward_rules} ADD COLUMN original_link_template VARCHAR DEFAULT '原始连接：{{original_link}}'",
        'enable_push': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_push BOOLEAN DEFAULT FALSE',
        'enable_only_push': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_only_push BOOLEAN DEFAULT FALSE',
        'media_allow_text': f'ALTER TABLE {t_forward_rules} ADD COLUMN media_allow_text BOOLEAN DEFAULT FALSE',
        'enable_ai_upload_image': f'ALTER TABLE {t_forward_rules} ADD COLUMN enable_ai_upload_image BOOLEAN DEFAULT FALSE',
    }

    keywords_new_columns = {
        'is_blacklist': f'ALTER TABLE {t_keywords} ADD COLUMN is_blacklist BOOLEAN DEFAULT TRUE',
    }

    with engine.begin() as connection:
        # 添加 forward_rules 表的列
        for column, sql in forward_rules_new_columns.items():
            if column not in forward_rules_columns:
                try:
                    connection.execute(text(sql))
                    logging.info(f'已添加列: {column}')
                except Exception as e:
                    logging.error(f'添加列 {column} 时出错: {str(e)}')

        # 添加 keywords 表的列
        for column, sql in keywords_new_columns.items():
            if column not in keyword_columns:
                try:
                    connection.execute(text(sql))
                    logging.info(f'已添加列: {column}')
                except Exception as e:
                    logging.error(f'添加列 {column} 时出错: {str(e)}')

        # 修改 forward_rules 表的列 mode 为 forward_mode
        if inspector.has_table(t_forward_rules) and 'forward_mode' not in forward_rules_columns:
            connection.execute(text(f"ALTER TABLE {t_forward_rules} RENAME COLUMN mode TO forward_mode"))
            logging.info('修改 forward_rules 表的列 mode 为 forward_mode 成功')

        # -------------------------------------------------------------------
        # 4. keywords 唯一约束/索引（跨数据库通用实现）
        # -------------------------------------------------------------------
        if inspector.has_table(t_keywords):
            index_name = _unique_name('unique_rule_keyword_is_regex_is_blacklist')
            existing_indexes = {idx['name'] for idx in inspector.get_indexes(t_keywords)}

            if index_name not in existing_indexes:
                logging.info('开始更新 keywords 表的唯一约束...')
                try:
                    # 先按 (rule_id, keyword, is_regex, is_blacklist) 去重
                    if dialect == 'sqlite':
                        connection.execute(text(f"""
                            DELETE FROM {t_keywords}
                            WHERE rowid NOT IN (
                                SELECT MIN(rowid) FROM {t_keywords}
                                GROUP BY rule_id, keyword, is_regex, is_blacklist
                            )
                        """))
                    else:
                        connection.execute(text(f"""
                            DELETE FROM {t_keywords} a
                            USING {t_keywords} b
                            WHERE a.id > b.id
                              AND a.rule_id = b.rule_id
                              AND a.keyword IS NOT DISTINCT FROM b.keyword
                              AND a.is_regex IS NOT DISTINCT FROM b.is_regex
                              AND a.is_blacklist IS NOT DISTINCT FROM b.is_blacklist
                        """))

                    connection.execute(text(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                        ON {t_keywords} (rule_id, keyword, is_regex, is_blacklist)
                    """))
                    logging.info(f'添加唯一约束 {index_name} 成功')
                except Exception as e:
                    logging.error(f'更新 keywords 表结构时出错: {str(e)}')
            else:
                logging.info('唯一约束已存在，跳过创建')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    engine = init_db()
    session = get_session()
    logging.info("数据库初始化和迁移完成。")
