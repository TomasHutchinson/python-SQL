import re

class SQLParser():
	def __init__(self, db):
		self.db = db  # The Database instance

	def execute(self, query):
		query = query.strip().rstrip(";").lower()

		if query.startswith("select"):
			return self._parse_select(query)
		elif query.startswith("insert"):
			return self._parse_insert(query)
		elif query.startswith("update"):
			return self._parse_update(query)
		elif query.startswith("delete"):
			return self._parse_delete(query)
		else:
			print("[Error] Unsupported SQL command")
			return None

	# --- Helpers to parse queries ---
	def _parse_select(self, query):
		# Very simple parser for: SELECT col1, col2 FROM table WHERE col = val ORDER BY col
		select_pattern = r"select\s+(?P<columns>.+?)\s+from\s+(?P<table>\w+)(\s+where\s+(?P<where>.+?))?(\s+order\s+by\s+(?P<order>\w+))?$"
		m = re.match(select_pattern, query, re.IGNORECASE)
		if not m:
			print("[Error] Could not parse SELECT query")
			return None

		columns = m.group("columns").strip()
		table_name = m.group("table").strip()
		where_clause = m.group("where")
		order_by = m.group("order")

		if columns == "*":
			columns = None
		else:
			columns = [col.strip() for col in columns.split(",")]

		# Parse simple where clauses like "col = value"
		where_func = None
		if where_clause:
			where_clause = where_clause.strip()
			if "=" in where_clause:
				col, val = [x.strip() for x in where_clause.split("=", 1)]
				val = val.strip("'\"")  # remove quotes
				where_func = lambda row, c=col, v=val: str(row.get(c, "")) == v

		# Call database select
		result = self.db.select(table_name, columns=columns, where=where_func, order_by=order_by)
		return result

	def _parse_insert(self, query):
		# Simple pattern: INSERT INTO table (col1, col2) VALUES (val1, val2)
		insert_pattern = r"insert\s+into\s+(?P<table>\w+)\s*(\((?P<columns>.+?)\))?\s*values\s*\((?P<values>.+?)\)"
		m = re.match(insert_pattern, query, re.IGNORECASE)
		if not m:
			print("[Error] Could not parse INSERT query")
			return None

		table_name = m.group("table").strip()
		columns = m.group("columns")
		values = m.group("values")

		# Process columns
		if columns:
			columns = [col.strip() for col in columns.split(",")]
		else:
			columns = None  # Will match table columns

		# Process values
		values = [v.strip().strip("'\"") for v in values.split(",")]

		# Map values to table columns if necessary
		if columns:
			row = dict(zip(columns, values))
			db_row = [row.get(col, "") for col in self.db.tables[table_name].columns]
		else:
			db_row = values

		self.db.insert(table_name, db_row)
		return f"[Info] Inserted 1 row into {table_name}"

	def _parse_update(self, query):
		# Simple pattern: UPDATE table SET col1 = val1, col2 = val2 WHERE col = val
		update_pattern = r"update\s+(?P<table>\w+)\s+set\s+(?P<sets>.+?)(\s+where\s+(?P<where>.+))?$"
		m = re.match(update_pattern, query, re.IGNORECASE)
		if not m:
			print("[Error] Could not parse UPDATE query")
			return None

		table_name = m.group("table").strip()
		sets_clause = m.group("sets").strip()
		where_clause = m.group("where")

		# Parse set statements
		updates = {}
		for assign in sets_clause.split(","):
			col, val = [x.strip() for x in assign.split("=", 1)]
			updates[col] = val.strip("'\"")

		# Parse simple where clause
		where_func = None
		if where_clause:
			where_clause = where_clause.strip()
			if "=" in where_clause:
				col, val = [x.strip() for x in where_clause.split("=", 1)]
				val = val.strip("'\"")
				where_func = lambda row, c=col, v=val: str(row.get(c, "")) == v

		self.db.update(table_name, updates, where=where_func)
		return f"[Info] Updated rows in {table_name}"

	def _parse_delete(self, query):
		# Simple pattern: DELETE FROM table WHERE col = val
		delete_pattern = r"delete\s+from\s+(?P<table>\w+)(\s+where\s+(?P<where>.+))?$"
		m = re.match(delete_pattern, query, re.IGNORECASE)
		if not m:
			print("[Error] Could not parse DELETE query")
			return None

		table_name = m.group("table").strip()
		where_clause = m.group("where")

		# Parse simple where clause
		where_func = None
		if where_clause:
			where_clause = where_clause.strip()
			if "=" in where_clause:
				col, val = [x.strip() for x in where_clause.split("=", 1)]
				val = val.strip("'\"")
				where_func = lambda row, c=col, v=val: str(row.get(c, "")) == v

		self.db.delete(table_name, where=where_func)
		return f"[Info] Deleted rows from {table_name}"

