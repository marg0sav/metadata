import re
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from typing import Callable

from app.repositories.meta_repository import MetaRepository
from app.state.query_builder_state import QueryBuilderState
from app.repositories.query_repository import QueryRepository


class TabBuilder(ttk.Frame):
    """
    Вкладка 2: Конструктор SELECT
    - выбор БД, таблицы
    - SELECT: чекбоксы + AS
    - WHERE (AND-only в прототипе)
    - превью SQL
    - Save
    """

    def __init__(
      self,
      parent: ttk.Notebook,
      meta_repo: MetaRepository,
      state: QueryBuilderState,
      on_saved: Callable[[dict], None],
      query_repo: QueryRepository,
    ):
        super().__init__(parent)
        self.meta_repo = meta_repo
        self.state = state
        self.on_saved = on_saved
        self.query_repo = query_repo

        # верхняя панель
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=8)

        ttk.Label(top, text="Database:").pack(side="left")
        self.cmb_db = ttk.Combobox(top, values=self.meta_repo.list_databases(), state="readonly", width=24)
        self.cmb_db.pack(side="left", padx=6)
        self.cmb_db.bind("<<ComboboxSelected>>", self._on_db_change)

        ttk.Label(top, text="Table:").pack(side="left", padx=(16, 0))
        self.cmb_table = ttk.Combobox(top, values=[], state="readonly", width=28)
        self.cmb_table.pack(side="left", padx=6)
        self.cmb_table.bind("<<ComboboxSelected>>", self._on_table_change)

        # средний панель-сплиттер
        mid = ttk.Panedwindow(self, orient="horizontal")
        mid.pack(fill="both", expand=True, padx=10, pady=8)

        # SELECT
        left = ttk.Labelframe(mid, text="SELECT columns")
        mid.add(left, weight=1)

        self.columns_frame = ttk.Frame(left)
        self.columns_frame.pack(fill="both", expand=True, padx=8, pady=8)

        # WHERE
        right = ttk.Labelframe(mid, text="WHERE (AND-only in prototype)")
        mid.add(right, weight=1)

        self.where_rows_container = ttk.Frame(right)
        self.where_rows_container.pack(fill="both", expand=True, padx=8, pady=8)

        where_btns = ttk.Frame(right)
        where_btns.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(where_btns, text="+ Add filter", command=self._add_where_row).pack(side="left")

        bottom = ttk.Panedwindow(self, orient="vertical")
        bottom.pack(fill="both", expand=True, padx=10, pady=8)

        # SQL превью
        frm_sql = ttk.Frame(bottom)
        bottom.add(frm_sql, weight=1)
        self.txt_preview = tk.Text(frm_sql, height=6)
        self.txt_preview.pack(fill="both", expand=True)

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        #ttk.Button(btns, text="Run", command=self._run_query).pack(side="left")
        ttk.Button(btns, text="Save", command=self._save_query).pack(side="left", padx=6)

    # --- public ---

    def refresh_databases(self):
        """Вызывается, когда в реестре БД появились новые элементы."""
        cur = self.cmb_db.get()
        self.cmb_db["values"] = self.meta_repo.list_databases()
        if cur and cur in self.cmb_db["values"]:
            self.cmb_db.set(cur)

    # --- internal ---

    def _on_db_change(self, _evt=None):
        self.state.dbname = self.cmb_db.get()
        self.state.table = None
        self.cmb_table.set("")
        self.cmb_table["values"] = self.meta_repo.list_tables(self.state.dbname)
        self._clear_select_where()
        self._update_preview()

    def _on_table_change(self, _evt=None):
        self.state.table = self.cmb_table.get()
        self._render_select_columns()
        self._refresh_where_comboboxes()
        self._update_preview()

    def _clear_select_where(self):
        for w in self.columns_frame.winfo_children():
            w.destroy()
        for w in self.where_rows_container.winfo_children():
            w.destroy()
        self.state.selected_columns.clear()
        self.state.filters.clear()

    def _render_select_columns(self):
        for w in self.columns_frame.winfo_children():
            w.destroy()
        self.state.selected_columns.clear()

        cols = self.meta_repo.list_columns(self.state.dbname, self.state.table)
        for (col, type_family) in cols:
            row = ttk.Frame(self.columns_frame)
            row.pack(fill="x", pady=2)

            var_chk = tk.BooleanVar(value=False)
            var_alias = tk.StringVar(value="")

            chk = ttk.Checkbutton(
                row,
                text=f'{col}  ({type_family})',
                variable=var_chk,
                command=self._on_select_changed,
            )
            chk.pack(side="left")

            ttk.Label(row, text="AS").pack(side="left", padx=(10, 2))
            ent = ttk.Entry(row, width=16, textvariable=var_alias)
            ent.pack(side="left")

            # обновляем превью "на лету" при наборе
            var_alias.trace_add("write", lambda *_: self._on_alias_changed())

            # валидируем по Enter и по уходу фокуса (и НЕ затираем другие обработчики)
            ent.bind("<Return>",
                     lambda e, v=var_alias, w=ent: self._on_alias_commit(v, w),
                     add="+")
            ent.bind("<FocusOut>",
                     lambda e, v=var_alias, w=ent: self._on_alias_commit(v, w, silent=True),
                     add="+")

            # На случай, если где-то меняется значение переменной без клика по чекбоксу
            var_chk.trace_add("write", lambda *_: self._on_select_changed())

            self.state.selected_columns[col] = {"checked_var": var_chk, "alias_var": var_alias}

        # после первого рендера тоже синхронизируем WHERE
        self._refresh_where_comboboxes()

    def _is_valid_identifier(self, name: str) -> bool:
        """
        Пустая строка допустима (алиас не задан).
        Иначе: латиница/цифры/_, но начинаться с буквы или _.
        """
        if not name:
            return True
        return re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name) is not None

    def _on_alias_commit(self, var: tk.StringVar, entry: ttk.Entry, silent: bool = False):
        """
        Вызывается по Enter и по уходу фокуса из поля алиаса.
        Валидируем; при ошибке показываем messagebox и возвращаем фокус.
        """
        alias = (var.get() or "").strip()

        if not self._is_valid_identifier(alias):
            if not silent:
                messagebox.showerror(
                    "Invalid alias",
                    "Алиас должен быть пустым или соответствовать шаблону:\n"
                    "• начинаться с буквы или _\n"
                    "• содержать только буквы, цифры и _"
                )
            # вернуть фокус и выделить текст для правки
            try:
                entry.focus_set()
                entry.selection_range(0, 'end')
            except Exception:
                pass
            return "break"  # отменяем обработку Enter дальше

        # всё ок — обновим превью SQL
        self._update_preview()

    def _on_alias_changed(self):
        """Алиас изменился — обновим превью (и только его)."""
        self._update_preview()

    def _add_where_row(self):
        row = ttk.Frame(self.where_rows_container)
        row.pack(fill="x", pady=2)

        # показываем только отмеченные в SELECT; если их нет — все колонки таблицы
        selected = self._selected_columns()
        if not selected:
            selected = [c for (c, _t) in self.meta_repo.list_columns(self.state.dbname or "", self.state.table or "")]

        cmb_col = ttk.Combobox(row, values=selected, state="readonly", width=18)
        cmb_col.pack(side="left")

        ops = ["=", "<>", "<", ">", "<=", ">=", "LIKE", "ILIKE", "IN", "IS NULL", "IS NOT NULL", "BETWEEN"]
        cmb_op = ttk.Combobox(row, values=ops, state="readonly", width=10)
        cmb_op.pack(side="left", padx=4)

        ent_val = ttk.Entry(row, width=22)
        ent_val.pack(side="left", padx=4)

        def remove_row():
            row.destroy()
            self._update_preview()
        ttk.Button(row, text="×", width=3, command=remove_row).pack(side="left", padx=4)

        # обновление превью
        cmb_col.bind("<<ComboboxSelected>>", lambda e: self._update_preview())
        cmb_op.bind("<<ComboboxSelected>>", lambda e: self._update_preview())
        ent_val.bind("<KeyRelease>", lambda e: self._update_preview())

        self._update_preview()

    def _collect_state(self):
        # SELECT
        for name, meta in list(self.state.selected_columns.items()):
            meta["checked"] = meta["checked_var"].get()
            meta["alias"] = meta["alias_var"].get().strip()

        # WHERE
        filters = []
        for row in self.where_rows_container.winfo_children():
            widgets = row.winfo_children()
            if len(widgets) < 3:
                continue
            cmb_col, cmb_op, ent_val = widgets[0], widgets[1], widgets[2]
            col = cmb_col.get().strip()
            op = cmb_op.get().strip()
            val = ent_val.get().strip()
            if not col or not op:
                continue
            filters.append({"column": col, "op": op, "value": val})
        self.state.filters = filters

    def _update_preview(self):
        self._collect_state()
        sql = self.state.build_sql()
        self.txt_preview.delete("1.0", "end")
        self.txt_preview.insert("1.0", sql)


    def _save_query(self):
        title = simpledialog.askstring("Save query", "Title:")
        if not title:
            return
        if not self.state.dbname:
            messagebox.showwarning("Select DB", "Choose a database first.")
            return

        sql = self.state.build_sql()
        try:
            db_id = self.meta_repo.get_database_id(self.state.dbname)
            saved_id = self.query_repo.save_query(db_id, title, sql)
            if self.on_saved:
                # колбэк можно использовать просто как «обнови списки»
                self.on_saved({"id": saved_id, "title": title, "db": self.state.dbname, "sql": sql})
            messagebox.showinfo("Saved", f"Saved as '{title}' (id={saved_id})")
        except Exception as e:
            messagebox.showerror("Save error", str(e))

        # --- helpers for WHERE options ---

    def _selected_columns(self) -> list[str]:
        """Возвращает список колонок, отмеченных в SELECT."""
        return [
            name for name, meta in self.state.selected_columns.items()
            if meta["checked_var"].get()
        ]

    def _refresh_where_comboboxes(self):
        """В Combobox'ах WHERE показываем только отмеченные в SELECT (или все, если ничего не отмечено)."""
        selected = self._selected_columns()
        if not selected:
            selected = [c for (c, _t) in self.meta_repo.list_columns(self.state.dbname or "", self.state.table or "")]

        for row in self.where_rows_container.winfo_children():
            widgets = row.winfo_children()
            if not widgets:
                continue
            cmb_col = widgets[0]
            if isinstance(cmb_col, ttk.Combobox):
                cmb_col["values"] = selected
                # если текущего значения больше нет — очистим
                if cmb_col.get() and cmb_col.get() not in selected:
                    cmb_col.set("")

    def _on_select_changed(self):
        """Реакция на переключение чекбоксов SELECT."""
        self._update_preview()
        self._refresh_where_comboboxes()
