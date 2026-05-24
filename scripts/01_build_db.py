# =============================================================================
# LoL Meta Analytics — 01_build_db.py
# =============================================================================
# Lee el CSV de Oracle Elixir, construye un modelo estrella en DuckDB
# y genera todas las tablas necesarias para Power BI.
#
# Uso:
#   python scripts/01_build_db.py
#
# Cada vez que descargues un CSV nuevo, reemplaza data/raw/oracles_elixir_latest.csv
# y vuelve a correr este script. La base se reconstruye desde cero.
# =============================================================================

import duckdb
import pandas as pd
import os

# ── RUTAS ────────────────────────────────────────────────────────────────────
BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH  = os.path.join(BASE_DIR, "data", "raw",       "oracles_elixir_latest.csv")
DB_PATH   = os.path.join(BASE_DIR, "data", "processed", "lol_meta.duckdb")

# ── CONEXIÓN ─────────────────────────────────────────────────────────────────
print("🔌 Conectando a DuckDB...")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
con = duckdb.connect(DB_PATH)

# =============================================================================
# PASO 1 — Cargar CSV en tabla staging
# =============================================================================
print("📥 Cargando CSV en staging...")

con.execute("DROP TABLE IF EXISTS staging")
con.execute(f"""
    CREATE TABLE staging AS
    SELECT *
    FROM read_csv_auto('{CSV_PATH}', ignore_errors=true)
    WHERE datacompleteness = 'complete'   -- solo partidos con data completa
""")

total = con.execute("SELECT COUNT(*) FROM staging").fetchone()[0]
print(f"   ✅ {total:,} filas cargadas (solo 'complete')")

# =============================================================================
# PASO 2 — Dim_Game  (una fila por partido)
# =============================================================================
print("🏗️  Construyendo Dim_Game...")

con.execute("DROP TABLE IF EXISTS Dim_Game")
con.execute("""
    CREATE TABLE Dim_Game AS
    SELECT DISTINCT
        gameid,
        league,
        year,
        split,
        playoffs,
        TRY_CAST(date AS DATE)        AS date,
        CAST(game     AS INTEGER)     AS game_num,
        CAST(patch    AS VARCHAR)     AS patch,
        CAST(gamelength AS INTEGER)   AS gamelength_sec,
        ROUND(gamelength / 60.0, 1)   AS gamelength_min
    FROM staging
    WHERE position = 'team'           -- filas de resumen de equipo
""")

n = con.execute("SELECT COUNT(*) FROM Dim_Game").fetchone()[0]
print(f"   ✅ Dim_Game: {n:,} partidos")

# =============================================================================
# PASO 3 — Dim_League  (catálogo de ligas)
# =============================================================================
print("🏗️  Construyendo Dim_League...")

con.execute("DROP TABLE IF EXISTS Dim_League")
con.execute("""
    CREATE TABLE Dim_League AS
    SELECT DISTINCT
        league,
        -- Región inferida desde el nombre de liga
        CASE
            WHEN league IN ('LCK','LCK CL')                          THEN 'Korea'
            WHEN league IN ('LPL','LDL')                             THEN 'China'
            WHEN league IN ('LEC','EMEA Masters')                    THEN 'Europe'
            WHEN league IN ('LCS','LCS Challengers','NACL')          THEN 'North America'
            WHEN league IN ('LLA')                                   THEN 'Latin America'
            WHEN league IN ('CBLOL','CBLOL Academy')                 THEN 'Brazil'
            WHEN league IN ('PCS')                                   THEN 'Pacific'
            WHEN league IN ('LJL')                                   THEN 'Japan'
            WHEN league IN ('TCL')                                   THEN 'Turkey'
            WHEN league IN ('LCO')                                   THEN 'Oceania'
            WHEN league IN ('VCS')                                   THEN 'Vietnam'
            WHEN league LIKE '%MSI%' OR league LIKE '%Worlds%'       THEN 'International'
            ELSE 'Other'
        END AS region
    FROM staging
    ORDER BY league
""")

n = con.execute("SELECT COUNT(*) FROM Dim_League").fetchone()[0]
print(f"   ✅ Dim_League: {n:,} ligas")

# =============================================================================
# PASO 4 — Dim_Date  (tabla de fechas para time intelligence en Power BI)
# =============================================================================
print("🏗️  Construyendo Dim_Date...")

con.execute("DROP TABLE IF EXISTS Dim_Date")
con.execute("""
    CREATE TABLE Dim_Date AS
    WITH fechas AS (
        SELECT DISTINCT TRY_CAST(date AS DATE) AS date
        FROM staging
        WHERE date IS NOT NULL
    )
    SELECT
        date,
        YEAR(date)                          AS year,
        MONTH(date)                         AS month,
        MONTHNAME(date)                     AS month_name,
        WEEK(date)                          AS week_num,
        DAYOFWEEK(date)                     AS day_of_week,
        DAYNAME(date)                       AS day_name,
        -- Patch como string limpio (ej: '16.01')
        (
            SELECT CAST(patch AS VARCHAR)
            FROM staging s
            WHERE TRY_CAST(s.date AS DATE) = fechas.date
            LIMIT 1
        )                                   AS patch
    FROM fechas
    ORDER BY date
""")

n = con.execute("SELECT COUNT(*) FROM Dim_Date").fetchone()[0]
print(f"   ✅ Dim_Date: {n:,} fechas")

# =============================================================================
# PASO 5 — Dim_Champion  (catálogo de campeones)
# =============================================================================
print("🏗️  Construyendo Dim_Champion...")

con.execute("DROP TABLE IF EXISTS Dim_Champion")
con.execute("""
    CREATE TABLE Dim_Champion AS
    WITH todos_picks AS (
        -- Campeones jugados (con posición)
        SELECT DISTINCT
            champion AS champion_name,
            position AS primary_role
        FROM staging
        WHERE position != 'team'
          AND champion IS NOT NULL
          AND champion != ''
    ),
    -- En caso de que un campeón aparezca en múltiples roles, quedamos con el más frecuente
    rol_frecuente AS (
        SELECT
            champion                                                  AS champion_name,
            position                                                  AS primary_role,
            ROW_NUMBER() OVER (
                PARTITION BY champion
                ORDER BY COUNT(*) DESC
            )                                                         AS rn
        FROM staging
        WHERE position != 'team'
          AND champion IS NOT NULL
        GROUP BY champion, position
    )
    SELECT
        champion_name,
        primary_role,
        CASE primary_role
            WHEN 'top' THEN 1
            WHEN 'jng' THEN 2
            WHEN 'mid' THEN 3
            WHEN 'bot' THEN 4
            WHEN 'sup' THEN 5
            ELSE 99
        END AS role_order
    FROM rol_frecuente
    WHERE rn = 1
    ORDER BY champion_name
""")

n = con.execute("SELECT COUNT(*) FROM Dim_Champion").fetchone()[0]
print(f"   ✅ Dim_Champion: {n:,} campeones únicos")

# =============================================================================
# PASO 6 — Fact_PlayerGame  (rendimiento individual por partida)
# =============================================================================
print("🏗️  Construyendo Fact_PlayerGame...")

con.execute("DROP TABLE IF EXISTS Fact_PlayerGame")
con.execute("""
    CREATE TABLE Fact_PlayerGame AS
    SELECT
        gameid,
        league,
        split,
        CAST(patch AS VARCHAR)          AS patch,
        playoffs,
        side,
        position,
        playername,
        teamname,
        champion,
        CAST(result     AS INTEGER)     AS result,       -- 1=win, 0=loss
        CAST(kills      AS INTEGER)     AS kills,
        CAST(deaths     AS INTEGER)     AS deaths,
        CAST(assists    AS INTEGER)     AS assists,
        ROUND(kills + assists, 0)       AS kp_raw,       -- para KP% calculado en PBI

        -- Métricas de rendimiento
        CAST(dpm              AS DOUBLE)  AS dpm,
        CAST(damageshare      AS DOUBLE)  AS damage_share,
        CAST(vspm             AS DOUBLE)  AS vspm,
        CAST(cspm             AS DOUBLE)  AS cspm,
        CAST(damagetochampions AS DOUBLE) AS damage_total,

        -- Early game (snapshots)
        CAST(goldat15         AS DOUBLE)  AS gold_at15,
        CAST(xpat15           AS DOUBLE)  AS xp_at15,
        CAST(csat15           AS DOUBLE)  AS cs_at15,
        CAST(golddiffat15     AS DOUBLE)  AS gold_diff15,
        CAST(csdiffat15       AS DOUBLE)  AS cs_diff15,
        CAST(goldat10         AS DOUBLE)  AS gold_at10,
        CAST(golddiffat10     AS DOUBLE)  AS gold_diff10

    FROM staging
    WHERE position != 'team'
      AND playername IS NOT NULL
""")

n = con.execute("SELECT COUNT(*) FROM Fact_PlayerGame").fetchone()[0]
print(f"   ✅ Fact_PlayerGame: {n:,} filas")

# =============================================================================
# PASO 7 — Fact_TeamGame  (objetivos y resultado por equipo por partido)
# =============================================================================
print("🏗️  Construyendo Fact_TeamGame...")

con.execute("DROP TABLE IF EXISTS Fact_TeamGame")
con.execute("""
    CREATE TABLE Fact_TeamGame AS
    SELECT
        gameid,
        league,
        split,
        CAST(patch    AS VARCHAR)       AS patch,
        playoffs,
        side,
        teamname,
        CAST(result   AS INTEGER)       AS result,

        -- Kills globales del equipo
        CAST(teamkills   AS INTEGER)    AS team_kills,
        CAST(teamdeaths  AS INTEGER)    AS team_deaths,

        -- Objetivos
        CAST(firstdragon  AS INTEGER)   AS first_dragon,
        CAST(dragons      AS INTEGER)   AS dragons,
        CAST(firstbaron   AS INTEGER)   AS first_baron,
        CAST(barons       AS INTEGER)   AS barons,
        CAST(firstherald  AS INTEGER)   AS first_herald,
        CAST(heralds      AS INTEGER)   AS heralds,
        CAST(void_grubs   AS INTEGER)   AS void_grubs,
        CAST(towers       AS INTEGER)   AS towers,

        -- First blood y visión
        CAST(firstblood   AS INTEGER)   AS first_blood,
        CAST(visionscore  AS DOUBLE)    AS vision_score,

        -- Early gold
        CAST(goldat15     AS DOUBLE)    AS gold_at15,
        CAST(golddiffat15 AS DOUBLE)    AS gold_diff15,
            
        CAST(gamelength AS INTEGER)       AS gamelength_sec,
        ROUND(gamelength / 60.0, 1)       AS gamelength_min

    FROM staging
    WHERE position = 'team'
""")

n = con.execute("SELECT COUNT(*) FROM Fact_TeamGame").fetchone()[0]
print(f"   ✅ Fact_TeamGame: {n:,} filas")

# =============================================================================
# PASO 8 — Fact_DraftPick  (unpivot de pick1-pick5)
# =============================================================================
print("🏗️  Construyendo Fact_DraftPick...")

con.execute("DROP TABLE IF EXISTS Fact_DraftPick")
con.execute("""
    CREATE TABLE Fact_DraftPick AS
    WITH picks_unpivot AS (
        SELECT gameid, league, split, CAST(patch AS VARCHAR) AS patch,
               side, teamname, CAST(result AS INTEGER) AS result,
               1 AS pick_order, pick1 AS champion FROM staging WHERE position='team' AND pick1 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, CAST(result AS INTEGER),
               2, pick2 FROM staging WHERE position='team' AND pick2 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, CAST(result AS INTEGER),
               3, pick3 FROM staging WHERE position='team' AND pick3 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, CAST(result AS INTEGER),
               4, pick4 FROM staging WHERE position='team' AND pick4 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, CAST(result AS INTEGER),
               5, pick5 FROM staging WHERE position='team' AND pick5 IS NOT NULL
    )
    SELECT * FROM picks_unpivot
    WHERE champion IS NOT NULL AND champion != ''
""")

n = con.execute("SELECT COUNT(*) FROM Fact_DraftPick").fetchone()[0]
print(f"   ✅ Fact_DraftPick: {n:,} picks")

# =============================================================================
# PASO 9 — Fact_DraftBan  (unpivot de ban1-ban5)
# =============================================================================
print("🏗️  Construyendo Fact_DraftBan...")

con.execute("DROP TABLE IF EXISTS Fact_DraftBan")
con.execute("""
    CREATE TABLE Fact_DraftBan AS
    WITH bans_unpivot AS (
        SELECT gameid, league, split, CAST(patch AS VARCHAR) AS patch,
               side, teamname,
               1 AS ban_order, ban1 AS champion FROM staging WHERE position='team' AND ban1 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, 2, ban2 FROM staging WHERE position='team' AND ban2 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, 3, ban3 FROM staging WHERE position='team' AND ban3 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, 4, ban4 FROM staging WHERE position='team' AND ban4 IS NOT NULL
        UNION ALL
        SELECT gameid, league, split, CAST(patch AS VARCHAR),
               side, teamname, 5, ban5 FROM staging WHERE position='team' AND ban5 IS NOT NULL
    )
    SELECT * FROM bans_unpivot
    WHERE champion IS NOT NULL AND champion != ''
""")

n = con.execute("SELECT COUNT(*) FROM Fact_DraftBan").fetchone()[0]
print(f"   ✅ Fact_DraftBan: {n:,} bans")

# =============================================================================
# PASO 10 — Fact_ChampionStats  (vista resumen lista para Power BI)
# Esta tabla pre-calcula todo lo de Meta Overview para que Power BI
# no tenga que hacer joins complejos en cada refresco.
# =============================================================================
print("🏗️  Construyendo Fact_ChampionStats...")

con.execute("DROP TABLE IF EXISTS Fact_ChampionStats")
con.execute("""
    CREATE TABLE Fact_ChampionStats AS
    WITH picks AS (
        SELECT
            league, split, patch, champion,
            COUNT(*)                            AS picks,
            SUM(result)                         AS wins,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
                PARTITION BY league, split, patch
            )                                   AS pick_rate
        FROM Fact_DraftPick
        GROUP BY league, split, patch, champion
    ),
    bans AS (
        SELECT
            league, split, patch, champion,
            COUNT(*)                            AS bans
        FROM Fact_DraftBan
        GROUP BY league, split, patch, champion
    ),
    games_total AS (
        SELECT league, split, patch, COUNT(DISTINCT gameid) AS total_games
        FROM Fact_TeamGame
        GROUP BY league, split, patch
    )
    SELECT
        p.league,
        p.split,
        p.patch,
        p.champion,
        dc.primary_role,
        p.picks,
        p.wins,
        COALESCE(b.bans, 0)                                     AS bans,
        ROUND(p.picks * 100.0 / g.total_games, 1)               AS pick_rate_pct,
        ROUND(COALESCE(b.bans, 0) * 100.0 / g.total_games, 1)   AS ban_rate_pct,
        ROUND(
            (p.picks + COALESCE(b.bans, 0)) * 100.0 / g.total_games,
            1
        )                                                        AS presence_pct,
        ROUND(p.wins * 100.0 / NULLIF(p.picks, 0), 1)           AS winrate_pct,
        g.total_games
    FROM picks p
    LEFT JOIN bans     b  ON p.league=b.league AND p.split=b.split AND p.patch=b.patch AND p.champion=b.champion
    LEFT JOIN games_total g ON p.league=g.league AND p.split=g.split AND p.patch=g.patch
    LEFT JOIN Dim_Champion dc ON p.champion = dc.champion_name
    ORDER BY p.league, p.split, p.patch, presence_pct DESC
""")

n = con.execute("SELECT COUNT(*) FROM Fact_ChampionStats").fetchone()[0]
print(f"   ✅ Fact_ChampionStats: {n:,} filas")

# =============================================================================
# PASO 11 — Fact_Synergies  (dúos, tríos — self join en SQL)
# =============================================================================
print("🏗️  Construyendo Fact_Synergies (duos)...")

con.execute("DROP TABLE IF EXISTS Fact_Synergies_Duo")
con.execute("""
    CREATE TABLE Fact_Synergies_Duo AS
    WITH duo AS (
        SELECT
            a.league,
            a.split,
            a.gameid,
            a.champion   AS champ1,
            b.champion   AS champ2,
            a.result
        FROM Fact_DraftPick a
        JOIN Fact_DraftPick b
            ON  a.gameid  = b.gameid
            AND a.side    = b.side          -- mismo equipo
            AND a.champion < b.champion     -- evitar duplicados (A,B) y (B,A)
    )
    SELECT
        league, split,
        champ1, champ2,
        COUNT(*)                                    AS games,
        SUM(result)                                 AS wins,
        ROUND(SUM(result) * 100.0 / COUNT(*), 1)   AS winrate_pct,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (
            PARTITION BY league, split
        )                                           AS pick_rate_pct
    FROM duo
    GROUP BY league, split, champ1, champ2
    HAVING COUNT(*) >= 3                            -- mínimo 3 apariciones
    ORDER BY league, split, games DESC
""")

n = con.execute("SELECT COUNT(*) FROM Fact_Synergies_Duo").fetchone()[0]
print(f"   ✅ Fact_Synergies_Duo: {n:,} combinaciones")

# =============================================================================
# PASO 12 — Verificación final
# =============================================================================
print("\n📊 RESUMEN FINAL:")
print("-" * 45)
tablas = [
    "Dim_Game", "Dim_League", "Dim_Date", "Dim_Champion",
    "Fact_PlayerGame", "Fact_TeamGame",
    "Fact_DraftPick", "Fact_DraftBan",
    "Fact_ChampionStats", "Fact_Synergies_Duo"
]
for tabla in tablas:
    n = con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    print(f"   {tabla:<30} {n:>8,} filas")

# =============================================================================
# PASO 13 — Exportar a Parquet para Power BI
# =============================================================================
print("\n📦 Exportando a Parquet...")

PARQUET_DIR = os.path.join(BASE_DIR, "data", "processed", "parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)

for tabla in tablas:
    path = os.path.join(PARQUET_DIR, f"{tabla}.parquet").replace("\\", "/")
    con.execute(f"COPY {tabla} TO '{path}' (FORMAT PARQUET)")
    print(f"   ✅ {tabla}.parquet")

print("\n✅ Parquet listos en: data/processed/parquet/")

print(con.execute("SELECT SUM(total_games) FROM Fact_ChampionStats").fetchone())
print(con.execute("SELECT COUNT(DISTINCT gameid) FROM Fact_TeamGame").fetchone())

con.close()
print("\n✅ Base de datos lista en: data/processed/lol_meta.duckdb")
print("   Próximo paso: conectar Power BI al archivo .duckdb")