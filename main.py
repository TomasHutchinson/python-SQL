from database import Database
from table import Table
from parser import SQLParser

db = Database()
db.load_lcsv("db.lcsv")

parser = SQLParser(db)

def input_multiline_sql(prompt="SQL> "):
	lines = []
	while True:
		line = input(prompt)
		if not line:
			continue
		lines.append(line)
		if line.strip().endswith(";"):
			break
	return " ".join(lines).strip()

print("Enter your SQL query (end with ;):")
query = input_multiline_sql()
result = parser.execute(query)

# If SELECT returned rows, show them
if isinstance(result, list):
	t = Table("Result",[], rows=result)
	t.view_table()
else:
	print(result)
