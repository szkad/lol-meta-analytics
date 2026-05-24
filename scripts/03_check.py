import duckdb
import os

BASE_DIR = r"C:\Users\Skjeldan\Documents\Analisis de Datos\lol-meta-analytics"
DB_PATH = os.path.join(BASE_DIR, "data", "processed", "lol_meta.duckdb")

con = duckdb.connect(DB_PATH, read_only=True)
print(con.execute("""
    SELECT position, 
           COUNT(*) as total,
           COUNT(gold_diff15) as no_nulos,
           ROUND(AVG(gold_diff15), 1) as avg_diff
    FROM Fact_PlayerGame
    WHERE gold_diff15 IS NOT NULL
    GROUP BY position
""").df())
print(con.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'staging'
    AND column_name LIKE '%gold%diff%'
    OR column_name LIKE '%diff%gold%'
    ORDER BY column_name
""").df())
con.close()