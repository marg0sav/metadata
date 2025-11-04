# app/repositories/query_repository.py
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from app.db.connections import get_engine

class QueryRepository:
    def __init__(self):
        self.engine = get_engine("metadata")

    def save_query(self, database_id: int, title: str, sql_text: str) -> int:
        with self.engine.begin() as conn:
            row = conn.execute(
                text("""
                    INSERT INTO app.saved_queries (database_id, title, sql_text)
                    VALUES (:db, :t, :s)
                    RETURNING id
                """),
                {"db": database_id, "t": title, "s": sql_text},
            ).fetchone()
        return int(row[0])

    def list_saved(self, database_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Если database_id=None — вернуть все сохранённые запросы."""
        where = "WHERE q.database_id = :db" if database_id is not None else ""
        params = {"db": database_id} if database_id is not None else {}
        sql = f"""
            SELECT q.id, q.title, q.sql_text, q.created_at,
                   d.name AS db_name, q.database_id
            FROM app.saved_queries q
            JOIN meta_databases d ON d.id = q.database_id
            {where}
            ORDER BY q.created_at DESC
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]

    def add_history(self, database_id: int, sql_text: str, ok: bool, duration_ms: int, error_text: str | None) -> int:
        sql = text("""
            INSERT INTO app.run_history (database_id, sql_text, ok, duration_ms, error_text)
            VALUES (:db_id, :sql_text, :ok, :duration_ms, :error_text)
            RETURNING id
        """)
        with self.engine.begin() as conn:
            rid = conn.execute(sql, {
                "db_id": database_id,
                "sql_text": sql_text,
                "ok": ok,
                "duration_ms": duration_ms,
                "error_text": error_text,
            }).scalar_one()
            return int(rid)

    def list_history(self, database_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Если database_id=None — вся история."""
        where = "WHERE h.database_id = :db" if database_id is not None else ""
        params = {"db": database_id, "lim": limit} if database_id is not None else {"lim": limit}
        sql = f"""
            SELECT h.id, h.sql_text, h.ok, h.duration_ms, h.error_text, h.created_at,
                   d.name AS db_name, h.database_id
            FROM app.run_history h
            JOIN meta_databases d ON d.id = h.database_id
            {where}
            ORDER BY h.created_at DESC
            LIMIT :lim
        """
        with self.engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
        return [dict(r) for r in rows]

    def delete_saved(self, saved_id: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM app.saved_queries WHERE id = :id"), {"id": saved_id})
