class QueryBuilderState:
 def __init__(self):
  self.dbname = None
  self.table = None
  self.selected_columns = {}  # {col_name: {"alias": "", "checked": bool, "checked_var": tkVar?, "alias_var": tkVar?}}
  self.filters = []  # [{"column":..., "op":..., "value":...}]
  self.limit = 100

 @staticmethod
 def _quote_ident(ident: str) -> str:
  # экранируем двойные кавычки внутри идентификатора
  return '"' + ident.replace('"', '""') + '"'

 @classmethod
 def _quote_fqn(cls, fqn: str) -> str:
  """
  'public.branches' -> '"public"."branches"'
  'branches'        -> '"branches"'
  Уже кавыченные части не предполагаем; если надо – расширить.
  """
  if "." in fqn:
   s, t = fqn.split(".", 1)
   return f'{cls._quote_ident(s)}.{cls._quote_ident(t)}'
  return cls._quote_ident(fqn)

 def build_sql(self) -> str:
  if not self.dbname or not self.table:
   return "-- select a database and a table"

  # SELECT
  cols = []
  for col, meta in self.selected_columns.items():
   if meta.get("checked"):
    alias = meta.get("alias") or ""
    part = self._quote_ident(col)
    if alias:
     part += f' AS {self._quote_ident(alias)}'
    cols.append(part)
  if not cols:
   cols = ["*"]

  select_clause = "SELECT " + ", ".join(cols)
  from_clause = f"FROM {self._quote_fqn(self.table)}"

  # WHERE (простой конструктор)
  conds = []
  for f in self.filters:
   c, op, val = f.get("column"), f.get("op"), f.get("value")
   if not c or not op:
    continue
   qcol = self._quote_ident(c)
   up = (op or "").upper()
   if up in ("IS NULL", "IS NOT NULL"):
    conds.append(f"{qcol} {up}")
   else:
    # грубая типизация: число -> без кавычек, иначе строка в одинарных
    try:
     float(val)
     conds.append(f"{qcol} {op} {val}")
    except (TypeError, ValueError):
     sval = str(val or "").replace("'", "''")
     conds.append(f"{qcol} {op} '{sval}'")
  where_clause = ("WHERE " + " AND ".join(conds)) if conds else ""

  limit_clause = f"LIMIT {int(self.limit) if self.limit else 100}"
  return "\n".join([select_clause, from_clause, where_clause, limit_clause]).strip()
