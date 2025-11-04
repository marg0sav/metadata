# app/repositories/query_repository.py
from typing import List, Dict, Any
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

    def list_saved(self, database_id: int) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, title, sql_text, created_at
                    FROM app.saved_queries
                    WHERE database_id = :db
                    ORDER BY created_at DESC
                """),
                {"db": database_id},
            ).mappings().all()
        return [dict(r) for r in rows]

    def add_history(self, database_id: int, sql_text: str, ok: bool,
                    duration_ms: int | None, error_text: str | None) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO app.run_history (database_id, sql_text, ok, duration_ms, error_text)
                    VALUES (:db, :s, :ok, :d, :e)
                """),
                {"db": database_id, "s": sql_text, "ok": ok, "d": duration_ms, "e": error_text},
            )

    def list_history(self, database_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT id, sql_text, ok, duration_ms, error_text, created_at
                    FROM app.run_history
                    WHERE database_id = :db
                    ORDER BY created_at DESC
                    LIMIT :lim
                """),
                {"db": database_id, "lim": limit},
            ).mappings().all()
        return [dict(r) for r in rows]

    def delete_saved(self, saved_id: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM app.saved_queries WHERE id = :id"), {"id": saved_id})
