# app/repositories/meta_repository.py
from __future__ import annotations
from typing import Dict, List, Tuple

from sqlalchemy import text
from app.db.connections import get_engine, test_connection
from app.extractors.postgres import PostgresExtractor


def _fq(schema: str, table: str) -> str:
    """Собрать полное имя 'schema.table' для хранения в meta_tables.name."""
    return f"{schema}.{table}"


class MetaRepository:
    """
    Репозиторий для работы с мета-БД.
    Предполагается, что мета-схема уже развёрнута (DDL из сообщения).
    """

    def __init__(self):
        # Подключение к мета-БД (где лежат meta_* таблицы).
        self.engine = get_engine("metadata")

    # ---------- служебные пом helpers ----------

    def _get_database_id(self, conn, name: str) -> int:
        """Получить id записи в meta_databases по имени (или кинуть ошибку)."""
        row = conn.execute(
            text("SELECT id FROM meta_databases WHERE name = :n"),
            {"n": name},
        ).fetchone()
        if not row:
            raise ValueError(f"Database '{name}' is not registered in meta_databases")
        return int(row[0])

    def _upsert_database(self, conn, name: str) -> int:
        """
        Добавить БД в реестр (если её ещё нет) и вернуть её id.
        UNIQUE(name) уже есть → используем ON CONFLICT DO NOTHING + RETURNING id.
        """
        row = conn.execute(
            text("""
                INSERT INTO meta_databases (name)
                VALUES (:n)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
            """),
            {"n": name},
        ).fetchone()
        if row:
            return int(row[0])
        # если вставка не вернула id (запись уже была) — достанем id SELECT'ом
        return self._get_database_id(conn, name)

    # ---------- регистрация БД ----------

    def add_database(self, name: str) -> int:
        """
        Добавить БД в реестр (с предварительным тестом соединения) и вернуть её id.
        """
        if not name:
            raise ValueError("Database name is empty")
        if not test_connection(name):
            raise ValueError(f"Can't connect to database '{name}'")

        with self.engine.begin() as conn:
            db_id = self._upsert_database(conn, name)
        return db_id

    # ---------- пересканирование схемы ----------

    def rescan_schema(self, dbname: str) -> None:
        """
        Считает метаданные из реальной БД `dbname` (через PostgresExtractor) и
        полностью пересобирает записи в meta_* для этой БД.

        Стратегия:
          1) получаем/гарантируем meta_databases.id для dbname;
          2) удаляем meta_tables по database_id (CASCADE очистит дочерние таблицы);
          3) создаём записи:
             - meta_tables (name = 'schema.table')
             - meta_columns
             - meta_primary_keys + meta_primary_key_columns
             - meta_foreign_keys + meta_foreign_key_columns
        """
        # 1) читаем метаданные источника
        ext = PostgresExtractor({"dbname": dbname})
        with ext:
            tables = ext.list_tables()
            # Предзагрузим всё, чтобы один раз обращаться к источнику:
            per_table_columns: Dict[str, List[dict]] = {}
            per_table_pks: Dict[str, List[dict]] = {}
            per_table_fks: Dict[str, List[dict]] = {}
            for t in tables:
                fq = _fq(t["schema"], t["table_name"])
                per_table_columns[fq] = ext.list_columns(t["schema"], t["table_name"])
                per_table_pks[fq] = ext.list_primary_keys(t["schema"], t["table_name"])
                per_table_fks[fq] = ext.list_foreign_keys(t["schema"], t["table_name"])

        # 2) пишем в мета-БД
        with self.engine.begin() as conn:
            # гарантируем запись в meta_databases
            database_id = self._upsert_database(conn, dbname)

            # удаляем существующие таблицы этой БД (каскады очистят колонки/ключи)
            conn.execute(
                text("DELETE FROM meta_tables WHERE database_id = :db_id"),
                {"db_id": database_id},
            )

            # маппинги для id
            table_id_by_fq: Dict[str, int] = {}
            column_id_by_fq_and_name: Dict[Tuple[str, str], int] = {}

            # 2.1) таблицы
            for t in tables:
                fq = _fq(t["schema"], t["table_name"])
                row = conn.execute(
                    text("""
                        INSERT INTO meta_tables (database_id, name)
                        VALUES (:db_id, :name)
                        RETURNING id
                    """),
                    {"db_id": database_id, "name": fq},
                ).fetchone()
                table_id = int(row[0])
                table_id_by_fq[fq] = table_id

            # 2.2) колонки
            for fq, cols in per_table_columns.items():
                table_id = table_id_by_fq[fq]
                for c in cols:
                    row = conn.execute(
                        text("""
                            INSERT INTO meta_columns (table_id, name, data_type)
                            VALUES (:t_id, :c_name, :c_dtype)
                            RETURNING id
                        """),
                        {"t_id": table_id, "c_name": c["name"], "c_dtype": c["data_type"]},
                    ).fetchone()
                    col_id = int(row[0])
                    column_id_by_fq_and_name[(fq, c["name"])] = col_id

            # 2.3) первичные ключи (+ порядок колонок)
            for fq, pk_defs in per_table_pks.items():
                if not pk_defs:
                    continue
                table_id = table_id_by_fq[fq]

                # В большинстве СУБД PK один, но интерфейс позволяет несколько → проставим все.
                for pk in pk_defs:
                    row = conn.execute(
                        text("""
                            INSERT INTO meta_primary_keys (table_id)
                            VALUES (:t_id)
                            RETURNING id
                        """),
                        {"t_id": table_id},
                    ).fetchone()
                    pk_id = int(row[0])

                    # Колонки PK по порядку
                    # В BaseExtractor есть columns + ordinal_positions одинаковой длины
                    cols: List[str] = pk["columns"]
                    ords: List[int] = pk["ordinal_positions"]
                    for col_name, ord_pos in zip(cols, ords):
                        col_id = column_id_by_fq_and_name[(fq, col_name)]
                        conn.execute(
                            text("""
                                INSERT INTO meta_primary_key_columns
                                    (pk_id, column_id, ordinal_position)
                                VALUES (:pk_id, :col_id, :ord)
                            """),
                            {"pk_id": pk_id, "col_id": col_id, "ord": int(ord_pos)},
                        )

            # 2.4) внешние ключи (+ порядок и пары колонок)
            for src_fq, fk_list in per_table_fks.items():
                if not fk_list:
                    continue
                src_table_id = table_id_by_fq[src_fq]

                for fk in fk_list:
                    tgt_fq = _fq(fk["referenced_schema"], fk["referenced_table"])
                    # Убедимся, что цель есть среди таблиц (должна быть, так как мы прошли по всем)
                    if tgt_fq not in table_id_by_fq:
                        # На всякий случай можно создать запись (но логичнее считать это ошибкой входных метаданных)
                        raise RuntimeError(f"Referenced table '{tgt_fq}' not found in scan result")
                    tgt_table_id = table_id_by_fq[tgt_fq]

                    row = conn.execute(
                        text("""
                            INSERT INTO meta_foreign_keys (table_id, referenced_table_id)
                            VALUES (:src_id, :tgt_id)
                            RETURNING id
                        """),
                        {"src_id": src_table_id, "tgt_id": tgt_table_id},
                    ).fetchone()
                    fk_id = int(row[0])

                    # Пары колонок и их порядок.
                    # Если extractor дал 'column_pairs' — используем его (сохраняет соответствие и порядок),
                    # иначе сопоставим по позиции.
                    pairs = fk.get("column_pairs") or list(
                        zip(fk["columns"], fk["referenced_columns"])
                    )
                    for idx, (src_col, tgt_col) in enumerate(pairs, start=1):
                        src_col_id = column_id_by_fq_and_name[(src_fq, src_col)]
                        tgt_col_id = column_id_by_fq_and_name[(tgt_fq, tgt_col)]
                        conn.execute(
                            text("""
                                INSERT INTO meta_foreign_key_columns
                                    (fk_id, column_id, referenced_column_id, ordinal_position)
                                VALUES (:fk_id, :c_id, :rc_id, :ord)
                            """),
                            {
                                "fk_id": fk_id,
                                "c_id": src_col_id,
                                "rc_id": tgt_col_id,
                                "ord": idx,
                            },
                        )

    # ---------- чтение для UI ----------

    def list_databases(self) -> List[str]:
        """Список БД из мета-реестра по имени."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT name FROM meta_databases ORDER BY name")
            ).fetchall()
        return [r[0] for r in rows]

    def list_tables(self, dbname: str) -> List[str]:
        """
        Таблицы зарегистрированной БД (имена в формате 'schema.table').
        """
        with self.engine.connect() as conn:
            db_id = self._get_database_id(conn, dbname)
            rows = conn.execute(
                text("""
                    SELECT name
                    FROM meta_tables
                    WHERE database_id = :db
                    ORDER BY name
                """),
                {"db": db_id},
            ).fetchall()
        return [r[0] for r in rows]

    def list_columns(self, dbname: str, table: str) -> List[Tuple[str, str]]:
        """
        Колонки таблицы (имя, тип).
        Аргумент `table` ожидается в формате 'schema.table'. Если без схемы — будет ошибка,
        т.к. в мета-схеме имя хранится как FQN.
        """
        if "." not in table:
            raise ValueError("Укажите имя таблицы как 'schema.table'")

        with self.engine.connect() as conn:
            db_id = self._get_database_id(conn, dbname)
            row = conn.execute(
                text("""
                    SELECT id
                    FROM meta_tables
                    WHERE database_id = :db AND name = :t
                """),
                {"db": db_id, "t": table},
            ).fetchone()
            if not row:
                return []

            table_id = int(row[0])
            rows = conn.execute(
                text("""
                    SELECT name, data_type
                    FROM meta_columns
                    WHERE table_id = :t
                    ORDER BY name
                """),
                {"t": table_id},
            ).fetchall()

        return [(r[0], r[1]) for r in rows]

    def get_database_id(self, name: str) -> int:
        """Публичный резолвер id по имени БД (обёртка над приватным)."""
        with self.engine.connect() as conn:
            return self._get_database_id(conn, name)
