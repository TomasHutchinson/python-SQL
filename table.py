class Table():
	name = ""
	columns = []
	rows = []

	def __init__(self, name, columns, rows=None, path=""):
		self.name = name
		self.columns = columns
		self.rows = rows if rows is not None else []
		if self.rows:
			# Automatically extract columns from the first row
			self.columns = list(self.rows[0].keys())

		if path != "":
			self.load_csv(path)
	
	def load_csv(self, path):
		with open(path, "r", encoding="utf-8") as f:
			lines = [line.strip() for line in f if line.strip()]
			if not lines:
				print(f"[Warning] CSV {path} is empty")
				return
			headers = lines[0].split(",")
			self.columns = headers
			self.rows = [dict(zip(headers, line.split(","))) for line in lines[1:]]

	def view_table(self):
		if not self.rows:
			print(f"=== Table: {self.name} ===")
			print("[empty]")
			return

		print(f"=== Table: {self.name} ===")

		# Compute column widths
		col_widths = {col: len(col) for col in self.columns}
		for row in self.rows:
			for col in self.columns:
				col_widths[col] = max(col_widths[col], len(str(row[col])))

		# Print header
		header = " | ".join(col.ljust(col_widths[col]) for col in self.columns)
		print(header)
		print("-" * len(header))

		# Print rows
		for row in self.rows:
			line = " | ".join(str(row[col]).ljust(col_widths[col]) for col in self.columns)
			print(line)

