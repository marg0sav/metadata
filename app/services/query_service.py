import time
from sqlalchemy import text
from app.db.connections import get_engine
from app.repositories.query_repository import QueryRepository
from app.repositories.meta_repository import MetaRepository

class QueryService:
    def __init__(self, meta_repo: MetaRepository | None = None,
                 query_repo: QueryRepository | None = None):
        self.meta_repo = meta_repo
        self.query_repo = query_repo
        self.on_logged = None

    def run(self, dbname: str, sql: str):
        engine = get_engine(dbname)
        t0 = time.perf_counter()
        ok, rows, cols, err = True, [], [], None
        try:
            with engine.connect() as conn:
                res = conn.execute(text(sql))
                if res.returns_rows:
                    cols = list(res.keys())
                    rows = [tuple(r) for r in res]
        except Exception as e:
            ok, err = False, str(e)
        dt = round((time.perf_counter() - t0) * 1000)

        history_entry = None
        if self.meta_repo and self.query_repo:
            try:
                db_id = self.meta_repo.get_database_id(dbname)
                hist_id = self.query_repo.add_history(
                    database_id=db_id, sql_text=sql, ok=ok, duration_ms=dt, error_text=err
                )
                history_entry = {
                    "id": hist_id,
                    "database_id": db_id,
                    "sql_text": sql,
                    "ok": ok,
                    "duration_ms": dt,
                    "error_text": err,
                }
            except Exception:
                pass

        # уведомим UI, чтобы он обновил список истории
        cb = getattr(self, "on_logged", None)
        if callable(cb) and history_entry is not None:
            try:
                cb(history_entry)
            except Exception:
                pass

        return {"ok": ok, "rows": rows, "columns": cols, "duration_ms": dt, "error": err}

