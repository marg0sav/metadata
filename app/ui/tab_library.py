import tkinter as tk
from tkinter import ttk, messagebox
from typing import Callable, List, Dict

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
        get_saved: Callable[[], List[Dict]],
        get_history: Callable[[], List[Dict]],
        delete_saved: Callable[[int], None],
        query_service: QueryService,
    ):
        super().__init__(parent)
        self.get_saved = get_saved
        self.get_history = get_history
        self.delete_saved_cb = delete_saved
        self.query_service = query_service

        pan = ttk.Panedwindow(self, orient="horizontal")
        pan.pack(fill="both", expand=True, padx=10, pady=10)

        # Saved
        left = ttk.Labelframe(pan, text="Saved queries")
        pan.add(left, weight=1)

        self.list_saved = tk.Listbox(left)
        self.list_saved.pack(fill="both", expand=True, padx=8, pady=8)

        btns_left = ttk.Frame(left)
        btns_left.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(btns_left, text="Run", command=self._run_saved).pack(side="left", padx=6)
        ttk.Button(btns_left, text="Delete", command=self._delete_saved).pack(side="left", padx=6)

        # область результатов
        res = ttk.Labelframe(pan, text="Result")
        pan.add(res, weight=2)
        self.result_tree = ttk.Treeview(res, show="headings")
        vsb = ttk.Scrollbar(res, orient="vertical", command=self.result_tree.yview)
        hsb = ttk.Scrollbar(res, orient="horizontal", command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.result_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        hsb.pack(side="bottom", fill="x")

        # History
        right = ttk.Labelframe(pan, text="Run history")
        pan.add(right, weight=1)

        self.list_history = tk.Listbox(right)
        self.list_history.pack(fill="both", expand=True, padx=8, pady=8)

        self.refresh_lists()

    # --- public ---

    def refresh_lists(self):
        self._refresh_saved()
        self._refresh_history()

    # --- private ---

    def _refresh_saved(self):
        self._saved_cache = self.get_saved()  # кэшируем, чтобы по индексу забирать объект
        self.list_saved.delete(0, "end")
        for q in self._saved_cache:
            created = q.get("created_at", "")
            title = q.get("title", "")
            self.list_saved.insert("end", f"{created} • {title}")

    def _refresh_history(self):
        self._history_cache = self.get_history()
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
        idx = self._idx(self.list_saved)
        if idx is None:
            return
        q = self._saved_cache[idx]
        if messagebox.askyesno("Delete", f"Delete saved query '{q.get('title','')}'?"):
            try:
                self.delete_saved_cb(int(q["id"]))   # удаляем в БД
                self.refresh_lists()
            except Exception as e:
                messagebox.showerror("Delete failed", str(e))

    def _run_saved(self):
        idx = self._idx(self.list_saved)
        if idx is None:
            messagebox.showwarning("Run", "Select a saved query.")
            return
        q = self._saved_cache[idx]
        dbname = q.get("db_name")  # см. пункт ниже про list_saved()
        sql = q.get("sql_text", "")

        res = self.query_service.run(dbname, sql)
        if not res.get("ok", True):
            messagebox.showerror("Query error", res.get("error") or "Unknown error")
            return

        self._fill_results(res.get("columns", []), res.get("rows", []))
        messagebox.showinfo("Result",
                            f"Rows: {len(res.get('rows', []))}, duration: {res.get('duration_ms', 0)} ms")

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




