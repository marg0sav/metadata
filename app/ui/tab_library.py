import tkinter as tk
from tkinter import ttk, messagebox
from typing import Callable, List, Dict, Optional

from app.services.query_service import QueryService

class TabLibrary(ttk.Frame):
    """
    Library / History:
    - список сохранённых запросов (из БД)
    - история запусков (из БД)
    """

    def __init__(
      self,
      parent: ttk.Notebook,
      get_db_choices: Callable[[], List[tuple[int, str]]],
      get_saved: Callable[[Optional[int]], List[Dict]],
      get_history: Callable[[Optional[int]], List[Dict]],
      delete_saved: Callable[[int], None],
      query_service: QueryService,
    ):
        super().__init__(parent)
        self.get_db_choices = get_db_choices
        self.get_saved = get_saved
        self.get_history = get_history
        self.delete_saved_cb = delete_saved
        self.query_service = query_service

        # --- фильтр по БД ---
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=(10, 0))

        ttk.Label(top, text="Database:").pack(side="left")
        self.cmb_db = ttk.Combobox(top, state="readonly", width=28)
        self.cmb_db.pack(side="left", padx=6)
        self.cmb_db.bind("<<ComboboxSelected>>", lambda e: self.refresh_lists())
        self._db_id_by_name = {}
        self._fill_db_filter()

        # ====== основной Layout: вертикальный сплитер ======
        root_pan = ttk.Panedwindow(self, orient="vertical")
        root_pan.pack(fill="both", expand=True, padx=10, pady=10)

        # ---- верхний горизонтальный сплитер: Saved | History
        top_pan = ttk.Panedwindow(root_pan, orient="horizontal")
        root_pan.add(top_pan, weight=2)

        # Saved (слева)
        left = ttk.Labelframe(top_pan, text="Saved queries")
        top_pan.add(left, weight=1)

        cols = ("created_at", "title", "sql")
        self.tbl_saved = ttk.Treeview(left, show="headings", columns=cols)

        self.tbl_saved.heading("created_at", text="Created", anchor="w")
        self.tbl_saved.heading("title", text="Title", anchor="w")
        self.tbl_saved.heading("sql", text="SQL", anchor="w")

        self.tbl_saved.column("created_at", width=170, minwidth=170, anchor="w", stretch=False)
        self.tbl_saved.column("title", width=180, minwidth=180, anchor="w", stretch=False)
        self.tbl_saved.column("sql", width=1600, minwidth=900, anchor="w", stretch=False)

        # подключаем горизонтальный и вертикальный скроллбары
        vsb_left = ttk.Scrollbar(left, orient="vertical", command=self.tbl_saved.yview)
        hsb_left = ttk.Scrollbar(left, orient="horizontal", command=self.tbl_saved.xview)
        self.tbl_saved.configure(yscrollcommand=vsb_left.set, xscrollcommand=hsb_left.set)

        self.tbl_saved.pack(side="left", fill="both", expand=True, padx=8, pady=8)
        vsb_left.pack(side="right", fill="y")
        hsb_left.pack(side="bottom", fill="x")

        # History (справа)
        right = ttk.Labelframe(top_pan, text="Run history")
        top_pan.add(right, weight=1)

        self.list_history = tk.Listbox(right)
        self.list_history.pack(fill="both", expand=True, padx=8, pady=8)

        # ---- нижняя часть: Result
        res = ttk.Labelframe(root_pan, text="Result")
        root_pan.add(res, weight=3)

        self.result_tree = ttk.Treeview(res, show="headings")
        vsb = ttk.Scrollbar(res, orient="vertical", command=self.result_tree.yview)
        hsb = ttk.Scrollbar(res, orient="horizontal", command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.result_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        # ---- общие кнопки снизу
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Run", command=self._run_saved).pack(side="left", padx=6)
        ttk.Button(btns, text="Delete", command=self._delete_saved).pack(side="left", padx=6)

        self.refresh_lists()

    # --- public ---

    def refresh_lists(self):
        db_id = self._current_db_id()
        self._refresh_saved(db_id)
        self._refresh_history(db_id)

    # --- private ---

    def _refresh_saved(self, db_id):
        self._saved_cache = self.get_saved(db_id)  # [{id, title, sql_text, created_at, db_name?...}]
        # очистка
        for iid in self.tbl_saved.get_children():
            self.tbl_saved.delete(iid)

        # вставка
        for i, q in enumerate(self._saved_cache):
            created = q.get("created_at", "")
            title = q.get("title", "")
            sql_raw = (q.get("sql_text") or "").replace("\u00A0", " ")

            one_line_sql = " ".join(sql_raw.split())
            sql_short = one_line_sql

            iid = str(q.get("id", i))  # стабильный ключ строки
            self.tbl_saved.insert("", "end", iid=iid,
                                  values=(created, title, sql_short))

    def _refresh_history(self, db_id: Optional[int]):
        self._history_cache = self.get_history(db_id)
        self.list_history.delete(0, "end")
        for h in self._history_cache:
            ok = "✔" if h.get("ok") else "✖"
            ms = h.get("duration_ms", 0)
            created = h.get("created_at", "")
            sql_head = (h.get("sql_text", "") or "")[:60]
            self.list_history.insert("end", f"{ok} {ms} ms • {created} • {sql_head}…")

    def _idx(self, listbox: tk.Listbox):
        sel = listbox.curselection()
        return sel[0] if sel else None

    def _view_saved_sql(self):
        idx = self._idx(self.list_saved)
        if idx is None:
            return
        q = self._saved_cache[idx]
        messagebox.showinfo(f"SQL • {q.get('title','')}", q.get("sql_text", ""))

    def _delete_saved(self):
        saved_id = self._saved_selected_id()
        if saved_id is None:
            return
        q = next((x for x in self._saved_cache if int(x["id"]) == saved_id), None)
        title = q.get("title", "") if q else ""
        if messagebox.askyesno("Delete", f"Delete saved query '{title}'?"):
            try:
                self.delete_saved_cb(saved_id)
                self.refresh_lists()
            except Exception as e:
                messagebox.showerror("Delete failed", str(e))

    def _run_saved(self):
        saved_id = self._saved_selected_id()
        if saved_id is None:
            messagebox.showwarning("Run", "Select a saved query.")
            return
        # достанем объект из кэша
        q = next((x for x in self._saved_cache if int(x["id"]) == saved_id), None)
        if not q:
            return

        dbname = q.get("db_name")
        if not dbname:
            messagebox.showerror("Run", "Can't detect database for this query.")
            return

        sql = (q.get("sql_text") or "").replace("\u00A0", " ")
        res = self.query_service.run(dbname, sql)
        if not res.get("ok", True):
            messagebox.showerror("Query error", res.get("error") or "Unknown error")
            return

        self._fill_results(res.get("columns", []), res.get("rows", []))
        messagebox.showinfo("Result", f"Rows: {len(res.get('rows', []))}, duration: {res.get('duration_ms', 0)} ms")

    def _fill_results(self, columns, rows):
        for c in self.result_tree["columns"]:
            self.result_tree.heading(c, text="")
        self.result_tree.delete(*self.result_tree.get_children())
        self.result_tree["columns"] = columns or []
        for c in columns:
            self.result_tree.heading(c, text=c)
            self.result_tree.column(c, width=max(80, len(str(c)) * 8), stretch=True)
        for r in rows:
            self.result_tree.insert("", "end", values=r)

    def _current_db_id(self) -> Optional[int]:
        name = self.cmb_db.get().strip()
        if not name or name == "All":
            return None
        return self._db_id_by_name.get(name)

    def _fill_db_filter(self):
        choices = self.get_db_choices()  # [(id, name)]
        self._db_id_by_name = {name: did for did, name in choices}
        values = ["All"] + [name for _, name in choices]
        self.cmb_db["values"] = values
        if not self.cmb_db.get():
            self.cmb_db.set("All")

    def _saved_selected_id(self) -> int | None:
        sel = self.tbl_saved.selection()
        if not sel:
            return None
        # у нас есть кэш – найдём объект по iid или по индексу
        iid = sel[0]
        # попробуем сопоставить по id
        for q in self._saved_cache:
            if str(q.get("id")) == iid:
                return int(q["id"])
        # fallback: по позиции
        idx = self.tbl_saved.index(iid)
        return int(self._saved_cache[idx]["id"])


