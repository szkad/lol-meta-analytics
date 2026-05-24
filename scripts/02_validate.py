# =============================================================================
# LoL Meta Analytics — 02_validate.py
# =============================================================================
# Verifica integridad del modelo antes de abrir Power BI.
# Corre esto cada vez que actualices el CSV.
#
# Uso:
#   python scripts/02_validate.py
# =============================================================================

import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "processed", "lol_meta.duckdb")

con = duckdb.connect(DB_PATH, read_only=True)

errores = []
advertencias = []

def check(nombre, query, esperado=None, minimo=None):
    """Ejecuta un check y registra si falla."""
    resultado = con.execute(query).fetchone()[0]
    if esperado is not None and resultado != esperado:
        errores.append(f"❌ {nombre}: esperado={esperado}, obtenido={resultado}")
    elif minimo is not None and resultado < minimo:
        errores.append(f"❌ {nombre}: mínimo={minimo:,}, obtenido={resultado:,}")
    else:
        print(f"   ✅ {nombre}: {resultado:,}")
    return resultado

def warn(nombre, query, umbral_pct):
    """Registra advertencia si el porcentaje supera el umbral."""
    resultado = con.execute(query).fetchone()[0]
    if resultado > umbral_pct:
        advertencias.append(f"⚠️  {nombre}: {resultado:.1f}% (umbral: {umbral_pct}%)")
    else:
        print(f"   ✅ {nombre}: {resultado:.1f}%")

print("=" * 55)
print("🔍 VALIDACIÓN DEL MODELO — LoL Meta Analytics")
print("=" * 55)

# ── 1. CONTEOS MÍNIMOS ────────────────────────────────────────────────────────
print("\n📊 Conteos mínimos esperados:")
check("Partidos en Dim_Game",         "SELECT COUNT(*) FROM Dim_Game",         minimo=1000)
check("Ligas en Dim_League",          "SELECT COUNT(*) FROM Dim_League",        minimo=10)
check("Campeones en Dim_Champion",    "SELECT COUNT(*) FROM Dim_Champion",      minimo=50)
check("Filas en Fact_PlayerGame",     "SELECT COUNT(*) FROM Fact_PlayerGame",   minimo=10000)
check("Filas en Fact_ChampionStats",  "SELECT COUNT(*) FROM Fact_ChampionStats",minimo=1000)
check("Dúos en Fact_Synergies_Duo",   "SELECT COUNT(*) FROM Fact_Synergies_Duo",minimo=500)

# ── 2. CONSISTENCIA DE FILAS ─────────────────────────────────────────────────
print("\n🔗 Consistencia entre tablas:")

partidos = con.execute("SELECT COUNT(*) FROM Dim_Game").fetchone()[0]

check(
    "Fact_PlayerGame = partidos × 10",
    "SELECT COUNT(*) FROM Fact_PlayerGame",
    esperado=partidos * 10
)
check(
    "Fact_TeamGame = partidos × 2",
    "SELECT COUNT(*) FROM Fact_TeamGame",
    esperado=partidos * 2
)

# Picks: deberían ser ~partidos × 10 (5 picks × 2 equipos), toleramos ±5%
picks = con.execute("SELECT COUNT(*) FROM Fact_DraftPick").fetchone()[0]
ratio_picks = picks / (partidos * 10) * 100
if abs(ratio_picks - 100) > 5:
    advertencias.append(f"⚠️  Picks vs esperado: {ratio_picks:.1f}% (esperado ~100%)")
else:
    print(f"   ✅ Picks ratio: {ratio_picks:.1f}% del esperado")

# ── 3. NULOS EN COLUMNAS CRÍTICAS ────────────────────────────────────────────
print("\n🚫 Nulos en columnas críticas:")
warn("Nulos gameid en Fact_PlayerGame",
     "SELECT COUNT(*)*100.0/COUNT(*) FROM Fact_PlayerGame WHERE gameid IS NULL",
     umbral_pct=0)
warn("Nulos champion en Fact_DraftPick",
     "SELECT COUNT(*)*100.0/COUNT(*) FROM Fact_DraftPick WHERE champion IS NULL OR champion=''",
     umbral_pct=1)
warn("Nulos result en Fact_PlayerGame",
     "SELECT COUNT(*)*100.0/COUNT(*) FROM Fact_PlayerGame WHERE result IS NULL",
     umbral_pct=1)
warn("Nulos dpm en Fact_PlayerGame",
     "SELECT COUNT(*)*100.0/COUNT(*) FROM Fact_PlayerGame WHERE dpm IS NULL",
     umbral_pct=5)

# ── 4. INTEGRIDAD REFERENCIAL ────────────────────────────────────────────────
print("\n🔑 Integridad referencial:")

huerfanos_picks = con.execute("""
    SELECT COUNT(*) FROM Fact_DraftPick p
    LEFT JOIN Dim_Champion c ON p.champion = c.champion_name
    WHERE c.champion_name IS NULL
""").fetchone()[0]

if huerfanos_picks > 0:
    advertencias.append(f"⚠️  {huerfanos_picks} picks sin match en Dim_Champion")
else:
    print(f"   ✅ Todos los picks tienen match en Dim_Champion")

huerfanos_games = con.execute("""
    SELECT COUNT(*) FROM Fact_PlayerGame p
    LEFT JOIN Dim_Game g ON p.gameid = g.gameid
    WHERE g.gameid IS NULL
""").fetchone()[0]

if huerfanos_games > 0:
    advertencias.append(f"⚠️  {huerfanos_games} filas en Fact_PlayerGame sin match en Dim_Game")
else:
    print(f"   ✅ Todos los gameid de Fact_PlayerGame están en Dim_Game")

# ── 5. LÓGICA DE NEGOCIO ─────────────────────────────────────────────────────
print("\n🧠 Checks de lógica:")

# Winrate global debe estar entre 40% y 60% para ser razonable
wr = con.execute("""
    SELECT ROUND(AVG(result)*100, 1) FROM Fact_PlayerGame
""").fetchone()[0]
if 40 <= wr <= 60:
    print(f"   ✅ Winrate promedio global: {wr}% (rango esperado 40-60%)")
else:
    errores.append(f"❌ Winrate promedio global fuera de rango: {wr}%")

# Blue side winrate (debería ser ~52-55% históricamente)
wr_blue = con.execute("""
    SELECT ROUND(AVG(result)*100, 1)
    FROM Fact_TeamGame
    WHERE side = 'Blue'
""").fetchone()[0]
if 45 <= wr_blue <= 65:
    print(f"   ✅ Winrate Blue side: {wr_blue}% (histórico ~52-55%)")
else:
    advertencias.append(f"⚠️  Winrate Blue side inusual: {wr_blue}%")

# Presencia máxima no puede superar 200%
max_presence = con.execute("""
    SELECT MAX(presence_pct) FROM Fact_ChampionStats
""").fetchone()[0]
if max_presence <= 200:
    print(f"   ✅ Presencia máxima: {max_presence}% (máximo posible: 200%)")
else:
    errores.append(f"❌ Presencia mayor a 200% detectada: {max_presence}%")

# ── 6. MUESTRA DE DATOS ───────────────────────────────────────────────────────
print("\n📋 Top 5 campeones por presence (todas las ligas):")
top = con.execute("""
    SELECT champion, SUM(picks) AS picks, SUM(bans) AS bans,
           ROUND(AVG(presence_pct),1) AS avg_presence,
           ROUND(SUM(wins)*100.0/NULLIF(SUM(picks),0),1) AS winrate
    FROM Fact_ChampionStats
    GROUP BY champion
    ORDER BY avg_presence DESC
    LIMIT 5
""").fetchall()
print(f"   {'Champion':<15} {'Picks':>6} {'Bans':>6} {'Presence':>10} {'Winrate':>8}")
print("   " + "-" * 50)
for row in top:
    print(f"   {row[0]:<15} {row[1]:>6} {row[2]:>6} {row[3]:>9}% {row[4]:>7}%")

print("\n📋 Ligas con más partidos:")
ligas = con.execute("""
    SELECT league, COUNT(*) AS partidos
    FROM Dim_Game
    GROUP BY league
    ORDER BY partidos DESC
    LIMIT 8
""").fetchall()
for row in ligas:
    print(f"   {row[0]:<20} {row[1]:>5} partidos")

# ── RESUMEN FINAL ─────────────────────────────────────────────────────────────
print("\n" + "=" * 55)
if errores:
    print("🚨 ERRORES CRÍTICOS — corrige antes de abrir Power BI:")
    for e in errores: print(f"   {e}")
else:
    print("✅ Sin errores críticos")

if advertencias:
    print("\n⚠️  ADVERTENCIAS — revisa si es necesario:")
    for a in advertencias: print(f"   {a}")
else:
    print("✅ Sin advertencias")

print("=" * 55)
con.close()