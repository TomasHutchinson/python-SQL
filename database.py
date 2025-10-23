from table import Table

class Database():
	tables = {}
	def load_lcsv(self, lcsv_path):
		with open(lcsv_path, "r", encoding="utf-8") as f:
			for line in f:
				line = line.strip()
				if not line or line.startswith("#"):
					continue
				table_name, csv_path = [part.strip() for part in line.split(",", 1)]
				table = self.load_csv_as_table(table_name, csv_path)
				self.tables[table_name] = table

	def load_csv_as_table(self, table_name, csv_path):
		with open(csv_path, "r", encoding="utf-8") as f:
			lines = [line.strip() for line in f if line.strip()]
			columns = lines[0].split(",")
			rows = [dict(zip(columns, line.split(","))) for line in lines[1:]]
			return Table(table_name, columns, rows)

	def view_db(self):
		for table_name, table in self.tables.items():
			print(f"\n=== Table: {table_name} ===")
			if not table.rows:
				print("[empty]")
				continue

			columns = table.columns

			# Compute column widths
			col_widths = {col: len(col) for col in columns}
			for row in table.rows:
				for col in columns:
					col_widths[col] = max(col_widths[col], len(str(row[col])))

			# Print header
			header = " | ".join(col.ljust(col_widths[col]) for col in columns)
			print(header)
			print("-" * len(header))

			# Print all rows
			for row in table.rows:
				line = " | ".join(str(row[col]).ljust(col_widths[col]) for col in columns)
				print(line)
	
			
	def select(self, table_name, columns=None, where=None, order_by=None):
		if table_name not in self.tables:
			print(f"[Error] Table '{table_name}' does not exist")
			return []

		table = self.tables[table_name]
		if columns is None:
			columns = table.columns

		# Filter rows
		def match(row):
			if where is None:
				return True
			if callable(where):
				return where(row)
			return True

		result = [ {col: row.get(col, "") for col in columns} for row in table.rows if match(row) ]

		# Order rows
		if order_by:
			reverse = False
			if order_by.startswith("-"):
				reverse = True
				order_by = order_by[1:]
			result.sort(key=lambda r: r.get(order_by, ""), reverse=reverse)

		return result
		
	def _check_condition(self, row, condition):
		# condition: (column, operator, value)
		col, op, val = condition
		cell = row[col]
		if op == "=":
			return str(cell) == str(val)
		elif op == "!=":
			return str(cell) != str(val)
		elif op == "<":
			return float(cell) < float(val)
		elif op == "<=":
			return float(cell) <= float(val)
		elif op == ">":
			return float(cell) > float(val)
		elif op == ">=":
			return float(cell) >= float(val)
		else:
			raise ValueError(f"Unknown operator: {op}")

	def insert(self, table_name, values):
		if table_name not in self.tables:
			print(f"[Error] Table '{table_name}' does not exist")
			return

		table = self.tables[table_name]
		if len(values) != len(table.columns):
			print(f"[Error] Column count mismatch")
			return

		table.rows.append(dict(zip(table.columns, values)))

	def update(self, table_name, updates, where=None):
		if table_name not in self.tables:
			print(f"[Error] Table '{table_name}' does not exist")
			return

		table = self.tables[table_name]
		for row in table.rows:
			if where is None or where(row):
				for col, val in updates.items():
					if col in table.columns:
						row[col] = val

	def delete(self, table_name, where=None):
		if table_name not in self.tables:
			print(f"[Error] Table '{table_name}' does not exist")
			return

		table = self.tables[table_name]
		table.rows = [row for row in table.rows if not (where and where(row))]

	def join(self, left_table, right_table, left_key, right_key, columns=None):
		if left_table not in self.tables or right_table not in self.tables:
			print(f"[Error] One of the tables does not exist")
			return []

		left = self.tables[left_table]
		right = self.tables[right_table]

		result = []
		for lrow in left.rows:
			for rrow in right.rows:
				if lrow[left_key] == rrow[right_key]:
					combined = {**lrow, **rrow}
					if columns:
						result.append({col: combined[col] for col in columns})
					else:
						result.append(combined)
		return result

	def aggregate(self, table_name, func, column, where=None):
		rows = self.select(table_name, columns=[column], where=where)
		values = [float(r[column]) for r in rows]
		if not values:
			return None
		func = func.lower()
		if func == "count":
			return len(values)
		elif func == "sum":
			return sum(values)
		elif func == "avg":
			return sum(values)/len(values)
		elif func == "min":
			return min(values)
		elif func == "max":
			return max(values)
		else:
			raise ValueError(f"Unknown aggregation function: {func}")

	def group_by(self, table_name, group_column, agg_column, func):
		if table_name not in self.tables:
			print(f"[Error] Table '{table_name}' does not exist")
			return {}

		table = self.tables[table_name]
		groups = {}
		for row in table.rows:
			key = row[group_column]
			groups.setdefault(key, []).append(float(row[agg_column]))

		result = {}
		for key, values in groups.items():
			func_lower = func.lower()
			if func_lower == "count":
				result[key] = len(values)
			elif func_lower == "sum":
				result[key] = sum(values)
			elif func_lower == "avg":
				result[key] = sum(values)/len(values)
			elif func_lower == "min":
				result[key] = min(values)
			elif func_lower == "max":
				result[key] = max(values)
			else:
				raise ValueError(f"Unknown aggregation function: {func}")
		return result

