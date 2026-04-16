import sqlite3

conn = sqlite3.connect("database/label_machine.db")

query = """
SELECT shipment_id
FROM shipments
"""

for row in conn.execute(query):
    print(row)

conn.close()