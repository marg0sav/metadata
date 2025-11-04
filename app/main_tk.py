import tkinter as tk
from tkinter import ttk


from app.repositories.meta_repository import MetaRepository
from app.services.query_service import QueryService
from app.state.query_builder_state import QueryBuilderState
from app.repositories.query_repository import QueryRepository

# вкладки
from app.ui.tab_databases import TabDatabases
from app.ui.tab_builder import TabBuilder
from app.ui.tab_library import TabLibrary


class App(tk.Tk):
    def __init__(self, meta_repo: MetaRepository, query_service: QueryService, query_repo: QueryRepository):
        super().__init__()
        self.title("Mini SQL Studio")
        self.geometry("960x640")

        # зависимости/сервисы
        self.meta_repo = meta_repo
        self.query_repo = query_repo
        self.query_service = query_service
        self.query_service.meta_repo = self.meta_repo
        self.query_service.query_repo = self.query_repo
        self.state = QueryBuilderState()

        # общие списки
        self.saved_queries = []  # [{title,db, sql}]
        self.run_history = []    # [{db, sql, ok, duration_ms}]

        # Notebook
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True)

        # вкладки
        self.tab_db = TabDatabases(
            parent=self.nb,
            meta_repo=self.meta_repo,
            on_registry_changed=self._on_registry_changed,
            on_rescan=lambda dbname: None,  # можно привязать реальный рескан
        )
        self.nb.add(self.tab_db, text="Databases")

        self.tab_builder = TabBuilder(
            parent=self.nb,
            meta_repo=self.meta_repo,
            state=self.state,
            on_saved=self._on_saved_query,
            query_repo=self.query_repo,
        )
        self.nb.add(self.tab_builder, text="Query Builder")

        def current_db_id():
            # если нужно показывать списки для выбранной БД в билдере
            dbname = self.tab_builder.state.dbname
            return self.meta_repo.get_database_id(dbname) if dbname else None

        def current_db_choices():
            # список (id, name) для комбобокса фильтра
            names = meta_repo.list_databases()
            return [(meta_repo.get_database_id(n), n) for n in names]

        def db_choices():
            # [(id, name)] из мета-реестра
            return self.meta_repo.list_databases_with_ids()

        self.tab_lib = TabLibrary(
            parent=self.nb,
            get_db_choices=db_choices,
            get_saved=lambda db_id: query_repo.list_saved(db_id),
            get_history=lambda db_id: query_repo.list_history(db_id),
            delete_saved=lambda saved_id: self.query_repo.delete_saved(saved_id),
            query_service=self.query_service,
        )
        self.nb.add(self.tab_lib, text="Library / History")

        # стартовая вкладка — конструктор
        self.nb.select(self.tab_builder)

    # --- callbacks wiring ---


    def _on_registry_changed(self):
        """Когда список БД изменился (добавили/удалили) — обновим выпадашку в билдоре."""
        self.tab_builder.refresh_databases()

    def _on_run_logged(self, entry: dict):
        # История уже записывается QueryService → просто обновим вкладку
        self.tab_lib.refresh_lists()

    def _on_saved_query(self, entry: dict):
        # Сохранённые уже в БД → просто обновим вкладку
        self.tab_lib.refresh_lists()


if __name__ == "__main__":
    meta_repo = MetaRepository()
    query_repo = QueryRepository()
    query_service = QueryService(meta_repo=meta_repo, query_repo=query_repo)
    app = App(meta_repo, query_service, query_repo)
    app.mainloop()
