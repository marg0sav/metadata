from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict, Tuple, Iterable


# ---- типизированные структуры данных

class TableInfo(TypedDict):
    schema: str           # схема, к которой принадлежит таблица
    table_name: str       # имя таблицы
    table_type: str       # тип таблицы: 'BASE TABLE', 'VIEW' или др.


class ColumnInfo(TypedDict, total=False):
    name: str                     # имя колонки
    data_type: str                # тип данных (например: "numeric(10,2)" или "timestamp with time zone")
    is_nullable: bool             # может ли колонка быть NULL
    ordinal_position: int         # порядковый номер колонки в таблице
    default: Optional[str] = None # значение по умолчанию (если есть)


class PrimaryKeyInfo(TypedDict):
    constraint_name: str           # имя ограничения первичного ключа
    columns: List[str]             # список колонок, входящих в первичный ключ (в порядке определения)
    ordinal_positions: List[int]   # порядковые позиции тех же колонок


class ForeignKeyInfo(TypedDict):
    constraint_name: str             # имя ограничения внешнего ключа
    columns: List[str]               # список исходных колонок (в таблице-источнике)
    referenced_schema: str           # схема, на которую ссылается внешний ключ
    referenced_table: str            # таблица, на которую ссылается внешний ключ
    referenced_columns: List[str]    # список колонок целевой таблицы
    column_pairs: List[Tuple[str, str]]  # отображение пар (исходная -> целевая колонка)


class BaseExtractor(ABC):
    """
    Абстрактный базовый класс для извлечения метаданных из разных СУБД
    (Postgres / MySQL / MSSQL и т.д.).

    Реализации этого класса должны возвращать нормализованные,
    независимые от СУБД структуры данных (см. TypedDict выше).
    """

    def __init__(self, conn_params: Dict[str, Any]):
        """
        Параметры подключения к базе данных (специфичны для каждой СУБД).
        Пример для PostgreSQL:
            {
                'host': 'localhost',
                'port': 5432,
                'user': 'appuser',
                'password': 'secret',
                'dbname': 'app'
            }
        """
        self.conn_params = conn_params

    def connect(self) -> None:
        """подключение к базе данных"""
        raise NotImplementedError("connect() — необязательный метод, переопределите при необходимости.")

    def close(self) -> None:
        """закрытие соединения и освобождение ресурсов."""
        raise NotImplementedError("close() — необязательный метод, переопределите при необходимости.")

    def __enter__(self) -> "BaseExtractor":
        """Поддержка использования в контексте with (автоматическое подключение)."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Автоматическое закрытие соединения при выходе из блока with."""
        try:
            self.close()
        except NotImplementedError:
            pass

    # ---- основное API ----

    @abstractmethod
    def list_tables(
        self,
        database: Optional[str] = None,
        *,
        schemas: Optional[List[str]] = None,
        include_system_schemas: bool = False,
    ) -> List[TableInfo]:
        """
        возвращает список таблиц и представлений.
        параметры:
          - database: имя базы данных (если нужно)
          - schemas: ограничение по конкретным схемам
          - include_system_schemas: включать ли системные схемы (например, information_schema)
        """

    @abstractmethod
    def list_columns(
        self,
        table_schema: str,
        table_name: str,
    ) -> List[ColumnInfo]:
        """
        возвращает список колонок таблицы с типами данных, nullability и порядком следования.
        """

    @abstractmethod
    def list_primary_keys(
        self,
        table_schema: str,
        table_name: str,
    ) -> List[PrimaryKeyInfo]:
        """
        возвращает информацию о первичных ключах таблицы.
        """

    @abstractmethod
    def list_foreign_keys(
        self,
        table_schema: str,
        table_name: str,
    ) -> List[ForeignKeyInfo]:
        """
        возвращает список внешних ключей для таблицы, включая:
          - схему и таблицу, на которые они ссылаются
          - соответствие колонок (src -> tgt)
        """

    # ---- потоковая версия для больших каталогов ----
    def iter_tables(
        self,
        database: Optional[str] = None,
        *,
        schemas: Optional[List[str]] = None,
        include_system_schemas: bool = False,
    ) -> Iterable[TableInfo]:
       
        return iter(self.list_tables(database, schemas=schemas, include_system_schemas=include_system_schemas))
