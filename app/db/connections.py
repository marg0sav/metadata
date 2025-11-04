import os
import time
from typing import Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

BASE_DSN = os.getenv("BASE_DSN")
if not BASE_DSN:
    raise RuntimeError("BASE_DSN is not set. Please configure it in your .env file.")

# кеш движков по имени БД
_engines: Dict[str, Engine] = {}


def get_engine(dbname: str) -> Engine:
    """
    Возвращает (или создаёт) SQLAlchemy Engine для конкретной базы.
    """
    if dbname in _engines:
        return _engines[dbname]

    print("[dbg] BASE_DSN raw:", repr(BASE_DSN))
    dsn = f"{BASE_DSN}{dbname}"
    print("[dbg] DSN:", repr(dsn))
    engine = create_engine(
        dsn,
        echo=False,
        pool_pre_ping=True,             # пинг перед выдачей соединения из пула
        connect_args={"connect_timeout": 5},
    )
    _engines[dbname] = engine
    return engine


def test_connection(dbname: str, *, timeout_sql: float = 5.0) -> bool:
    """
    Пинг базы: выполняет SELECT 1. Возвращает True/False.
    """
    try:
        eng = get_engine(dbname)
        t0 = time.perf_counter()
        with eng.connect() as conn:
            # лёгкий пинг
            conn.execute(text("SELECT 1"))
        dt = (time.perf_counter() - t0) * 1000
        print(f"[db] OK  '{dbname}' ({dt:.1f} ms)")
        return True
    except Exception as e:
        print(f"[db] ERR '{dbname}': {e}")
        return False


def startup_healthcheck():
    """
    Автопроверка соединений при старте:
    - берёт список БД из переменной окружения STARTUP_CHECK_DBS
    - пингует каждую
    - при STARTUP_STRICT=1 падает, если есть ошибки
    """
    do_check = os.getenv("STARTUP_CHECK", "1") == "1"
    if not do_check:
        return

    raw = os.getenv("STARTUP_CHECK_DBS", "").strip()
    if not raw:
        return

    dbs = [x.strip() for x in raw.split(",") if x.strip()]
    if not dbs:
        return

    print(f"[startup] health check for: {', '.join(dbs)}")
    failures = 0
    for db in dbs:
        ok = test_connection(db)
        if not ok:
            failures += 1

    if failures:
        msg = f"[startup] {failures} connection(s) failed"
        if os.getenv("STARTUP_STRICT", "0") == "1":
            raise RuntimeError(msg)
        else:
            print(msg)


# запустить автопроверку при импорте модуля
startup_healthcheck()
