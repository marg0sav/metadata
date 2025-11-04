from __future__ import annotations
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection
from ..db.connections import get_engine

from .base import (
    BaseExtractor,
    TableInfo,
    ColumnInfo,
    PrimaryKeyInfo,
    ForeignKeyInfo,
)


class PostgresExtractor(BaseExtractor):
    """
    реализация BaseExtractor для PostgreSQL.
    достаёт метаданные из information_schema и pg_catalog.
    """

    def __init__(self, conn_params: Dict[str, Any]):
        """
        conn_params ожидает хотя бы 'dbname'.
        Остальное берётся из BASE_DSN (connections.py).
        """
        super().__init__(conn_params)
        self._engine = None
        self.conn: Optional[PGConnection] = None
        self.cursor = None

    def connect(self) -> None:
        if self.conn is not None:
            return
        dbname = self.conn_params["dbname"]  # обязательный ключ
        self._engine = get_engine(dbname)    # общий пул
        # берём raw psycopg2 connection, чтобы работать с cursor как раньше
        raw = self._engine.raw_connection()
        # на всякий случай включим autocommit для read-only SELECT’ов
        try:
            raw.autocommit = True
        except Exception:
            pass
        self.conn = raw
        self.cursor = raw.cursor()

    def close(self) -> None:
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.cursor = None
        self.conn = None
        self._engine = None

    # --- API из BaseExtractor -------------------------------------------------

    def list_tables(
        self,
        database: Optional[str] = None,
        *,
        schemas: Optional[List[str]] = None,
        include_system_schemas: bool = False,
    ) -> List[TableInfo]:
        """
        Возвращает список таблиц/представлений.
        Параметр database игнорируем (мы уже подключены к конкретной БД).
        """
        self.connect()

        where = []
        params: List[Any] = []

        if not include_system_schemas:
            where.append("table_schema NOT IN ('pg_catalog','information_schema')")

        if schemas:
            where.append("table_schema = ANY(%s)")
            params.append(schemas)

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            {where_sql}
            ORDER BY table_schema, table_name
        """

        self.cursor.execute(sql, params or None)
        rows = self.cursor.fetchall()

        return [
            TableInfo(schema=r[0], table_name=r[1], table_type=r[2])
            for r in rows
        ]

    def list_columns(self, table_schema: str, table_name: str) -> List[ColumnInfo]:
        """
        Колонки с типами, nullability и default; с порядком.
        """
        self.connect()

        sql = """
            SELECT
                a.attnum AS ordinal_position,
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
                NOT a.attnotnull AS is_nullable,
                pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS column_default
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_catalog.pg_attrdef ad
              ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
        """
        self.cursor.execute(sql, (table_schema, table_name))
        rows = self.cursor.fetchall()

        return [
            ColumnInfo(
                name=r[1],
                data_type=r[2],
                is_nullable=bool(r[3]),
                ordinal_position=int(r[0]),
                default=r[4],
            )
            for r in rows
        ]

    def list_primary_keys(self, table_schema: str, table_name: str) -> List[PrimaryKeyInfo]:
        """
        Описание PK (обычно одно на таблицу), с упорядоченными колонками.
        """
        self.connect()

        sql = """
            SELECT
                tc.constraint_name,
                kcu.column_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema   = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name   = %s
            ORDER BY kcu.ordinal_position
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (table_schema, table_name))
            rows = cur.fetchall()

        if not rows:
            return []

        acc: dict[str, PrimaryKeyInfo] = {}
        for name, col, pos in rows:
            if name not in acc:
                acc[name] = PrimaryKeyInfo(
                    constraint_name=name,
                    columns=[],
                    ordinal_positions=[],
                )
            acc[name]["columns"].append(col)
            acc[name]["ordinal_positions"].append(int(pos))

        return list(acc.values())

    def list_foreign_keys(self, table_schema: str, table_name: str) -> List[ForeignKeyInfo]:
        """
        Описание FK с сохранением порядка колонок и картой src->tgt.
        """
        self.connect()

        sql = """
            SELECT
                con.conname AS constraint_name,
                src_ns.nspname AS src_schema,
                src_rel.relname AS src_table,
                tgt_ns.nspname AS tgt_schema,
                tgt_rel.relname AS tgt_table,
                src_att.attname AS src_col,
                tgt_att.attname AS tgt_col,
                ord.n AS position
            FROM pg_constraint con
            JOIN pg_class src_rel ON con.conrelid = src_rel.oid
            JOIN pg_namespace src_ns ON src_rel.relnamespace = src_ns.oid
            JOIN pg_class tgt_rel ON con.confrelid = tgt_rel.oid
            JOIN pg_namespace tgt_ns ON tgt_rel.relnamespace = tgt_ns.oid
            JOIN LATERAL generate_subscripts(con.conkey, 1) AS ord(n) ON TRUE
            LEFT JOIN pg_attribute src_att
              ON src_att.attrelid = src_rel.oid AND src_att.attnum = con.conkey[ord.n]
            LEFT JOIN pg_attribute tgt_att
              ON tgt_att.attrelid = tgt_rel.oid AND tgt_att.attnum = con.confkey[ord.n]
            WHERE con.contype = 'f'
              AND src_ns.nspname = %s
              AND src_rel.relname = %s
            ORDER BY con.conname, ord.n
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (table_schema, table_name))
            rows = cur.fetchall()

        fks: dict[str, ForeignKeyInfo] = {}
        for name, _ss, _st, tgt_schema, tgt_table, src_col, tgt_col, _pos in rows:
            if name not in fks:
                fks[name] = ForeignKeyInfo(
                    constraint_name=name,
                    columns=[],
                    referenced_schema=tgt_schema,
                    referenced_table=tgt_table,
                    referenced_columns=[],
                    column_pairs=[],
                )
            fks[name]["columns"].append(src_col)
            fks[name]["referenced_columns"].append(tgt_col)

        # дополним явной парой src->tgt, сохраняя порядок
        for fk in fks.values():
            fk["column_pairs"] = list(zip(fk["columns"], fk["referenced_columns"]))

        return list(fks.values())
