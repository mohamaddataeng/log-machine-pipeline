import sqlite3

print("\n=== DB TEST START ===")

conn = sqlite3.connect("database/label_machine.db")
cursor = conn.cursor()

# -----------------------
# TEST 1: Tabellen existieren
# -----------------------
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]

assert "shipments" in tables, "Tabelle 'shipments' fehlt"
assert "logs_raw" in tables, "Tabelle 'logs_raw' fehlt"

print("Tabellen vorhanden")


# -----------------------
# TEST 2: Zeilenanzahl > 0
# -----------------------
cursor.execute("SELECT COUNT(*) FROM shipments;")
shipments_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM logs_raw;")
raw_count = cursor.fetchone()[0]

assert shipments_count > 0, "shipments ist leer"
assert raw_count > 0, "logs_raw ist leer"

print("Tabellen haben Daten")


# -----------------------
# TEST 3: Wichtige Spalten existieren
# -----------------------
cursor.execute("PRAGMA table_info(shipments);")
cols = [c[1] for c in cursor.fetchall()]

required_cols = [
    "shipment_id",
    "timestamp",
    "stop_after",
    "stop_reason_after"
]

for col in required_cols:
    assert col in cols, f"Spalte fehlt: {col}"

print("Wichtige Spalten vorhanden")


# -----------------------
# TEST 4: stop_after Werte korrekt
# -----------------------
cursor.execute("SELECT DISTINCT stop_after FROM shipments;")
values = [v[0] for v in cursor.fetchall()]

assert set(values).issubset({0, 1}), "stop_after enthält ungültige Werte"

print("stop_after Werte korrekt")


# -----------------------
# TEST 5: Konsistenz stop_reason
# -----------------------
cursor.execute("""
SELECT COUNT(*) FROM shipments
WHERE stop_after = 1 AND (stop_reason_after IS NULL OR stop_reason_after = '')
""")
bad_1 = cursor.fetchone()[0]

cursor.execute("""
SELECT COUNT(*) FROM shipments
WHERE stop_after = 0 AND stop_reason_after != ''
""")
bad_0 = cursor.fetchone()[0]

assert bad_1 == 0, f"stop_after=1 ohne Grund: {bad_1}"
assert bad_0 == 0, f"stop_after=0 mit Grund: {bad_0}"

print("stop_reason konsistent")


# -----------------------
# TEST 6: Indizes existieren
# -----------------------
cursor.execute("PRAGMA index_list(shipments);")
indexes = [i[1] for i in cursor.fetchall()]

assert any("shipment" in i for i in indexes), " Index auf shipment_id fehlt"
assert any("time" in i for i in indexes), "Index auf timestamp fehlt"
assert any("stop_after" in i for i in indexes), " Index auf stop_after fehlt"

print("Indizes vorhanden")


conn.close()

print("\n=== DB TEST ERFOLGREICH ===")