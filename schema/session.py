# File: backend/database/session.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import os

class DatabaseManager:
    def __init__(self, repo_path=None):
        self.engines = {}
        self.Session = {}
        self._init_config(repo_path)

    def _init_config(self, repo_path):
        """读取数据库配置"""
        self.db_type = os.getenv('DB_TYPE', 'sqlite').strip().lower()

        # SQLite专用配置
        self.db_dir = os.path.join(repo_path, "database") if repo_path else None
        if self.db_dir and not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)

        # MariaDB连接配置
        self.db_host = os.getenv('MARIADB_HOST', 'localhost').strip()
        self.db_port = os.getenv('MARIADB_PORT', '3306').strip()
        self.db_user = os.getenv('MARIADB_USER', 'root').strip()
        self.db_pass = os.getenv('MARIADB_PASSWORD', '').strip()
        self.db_schema = os.getenv('MARIADB_SCHEMA', '').strip()

    def get_engine(self, db_name):
        if db_name not in self.engines:
            if self.db_type == 'sqlite':
                engine = self._create_sqlite_engine(db_name)
            else:
                engine = self._create_mariadb_engine(db_name)

            self.engines[db_name] = engine
        return self.engines[db_name]

    def _create_sqlite_engine(self, db_name):
        """创建SQLite引擎"""
        db_path = os.path.join(self.db_dir, f"{db_name}.db")
        return create_engine(
            f"sqlite:///{db_path}",
            pool_size=10,
            connect_args={"check_same_thread": False}
        )

    def _create_mariadb_engine(self, db_name):
        station_name = db_name

        """创建MariaDB引擎"""
        connection_str = (
            f"mysql+pymysql://{self.db_user}:{self.db_pass}@"
            f"{self.db_host}:{self.db_port}/{self.db_schema}"
            "?charset=utf8mb4&autocommit=false"
        )
        return create_engine(
            connection_str,
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            isolation_level="READ COMMITTED"
        )

    def get_session(self, db_name):
        if db_name not in self.Session:
            engine = self.get_engine(db_name)
            session_factory = sessionmaker(
                bind=engine,
                autoflush=False,
                autocommit=False
            )
            self.Session[db_name] = scoped_session(session_factory)
        return self.Session[db_name]()

    def close_all(self):
        for session in self.Session.values():
            session.remove()
        for engine in self.engines.values():
            engine.dispose()
