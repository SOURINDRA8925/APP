import sqlite3
import os

DB_FILE = 'project.db'

print("=== COLD RECORD LOOKUP DIAGNOSTIC ===")
if not os.path.exists(DB_FILE):
    print("❌ Error: project.db file path does not exist.")
    exit()

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

try:
    # 1. Look up how columns are actually spelled inside your current SQLite table structure
    cursor.execute("PRAGMA table_info(transactions)")
    columns_info = cursor.fetchall()
    actual_columns = [col[1] for col in columns_info]
    print(f"📋 Current Database Table Columns: {actual_columns}")

    # 2. Count master records inside the table matrix
    cursor.execute("SELECT COUNT(*) FROM transactions")
    total_rows = cursor.fetchone()[0]
    print(f"📊 Database State Check -> Total records saved inside transactions table: {total_rows}")

    # 3. Safely query whatever features exist by index position to prevent exceptions
    if total_rows > 0:
        print("\n📥 Displaying first 3 database sample rows to check features matrix:")
        cursor.execute("SELECT * FROM transactions LIMIT 3")
        rows = cursor.fetchall()
        for row in rows:
            print(f"  -> Row Data: {row}")
    else:
        print("⚠️ The database transactions table is empty. Turn Data Pulling ON while the server is active.")
except Exception as e:
    print(f"❌ Diagnostic failed: {e}")

conn.close()
print("=====================================")
