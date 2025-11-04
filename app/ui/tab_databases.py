import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from typing import Callable

from app.repositories.meta_repository import MetaRepository


class TabDatabases(ttk.Frame):
    """
    Вкладка 1: Реестр БД
    - список подключённых БД (из meta_repo)
    - Add database (модалка)
    - Rescan schema (заглушка-колбэк)
    """

    def __init__(
        self,
        parent: ttk.Notebook,
        meta_repo: MetaRepository,
        on_registry_changed: Callable[[], None],
        on_rescan: Callable[[str], None],
    ):
        super().__init__(parent)
        self.meta_repo = meta_repo
        self.on_registry_changed = on_registry_changed
        self.on_rescan = on_rescan

        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=10)

        ttk.Label(top, text="Connected databases:").pack(side="left")

        ttk.Button(top, text="Add database", command=self._add_database_dialog).pack(side="right", padx=5)
        ttk.Button(top, text="Rescan schema", command=self._rescan_selected_db).pack(side="right", padx=5)

        mid = ttk.Frame(self)
        mid.pack(fill="both", expand=True, padx=10, pady=10)

        self.lst = tk.Listbox(mid, height=12)
        self.lst.pack(side="left", fill="both", expand=True)

        sb = ttk.Scrollbar(mid, orient="vertical", command=self.lst.yview)
        sb.pack(side="right", fill="y")
        self.lst.configure(yscrollcommand=sb.set)

        self.refresh_list()

    # --- public ---

    def refresh_list(self):
        self.lst.delete(0, "end")
        for name in self.meta_repo.list_databases():
            self.lst.insert("end", name)

    # --- private ---

    def _add_database_dialog(self):
        name = simpledialog.askstring("Add database", "Database name on this server:")
        if not name:
            return
        try:
            self.meta_repo.add_database(name)
            messagebox.showinfo("OK", f"Database '{name}' added successfully.")
            self.refresh_list()
            if self.on_registry_changed:
                self.on_registry_changed()
            # сразу обновим схему после добавления
            self.meta_repo.rescan_schema(name)
            messagebox.showinfo("Scan", f"Schema for '{name}' scanned and saved.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _rescan_selected_db(self):
        sel = self.lst.curselection()
        if not sel:
            messagebox.showwarning("Select DB", "Please select a database.")
            return
        dbname = self.lst.get(sel[0])
        try:
            messagebox.showinfo("Rescan", f"Scanning schema for '{dbname}'...")
            self.meta_repo.rescan_schema(dbname)
            messagebox.showinfo("Rescan", f"Schema for '{dbname}' updated.")
        except Exception as e:
            messagebox.showerror("Rescan error", str(e))
