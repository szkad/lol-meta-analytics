# 🎮 LoL Meta Analytics — League of Legends Competitive Dashboard

Dashboard interactivo en **Power BI** que analiza el meta competitivo de League of Legends usando datos reales de Oracle Elixir (Season 2026).

![Meta Overview](capturas/01_meta_overview.jpg)

---

## 📊 ¿Qué responde este dashboard?

- ¿Qué campeones dominan el meta por liga, split y patch?
- ¿Qué combinaciones de dúos tienen mayor winrate?
- ¿Qué objetivos son más decisivos para ganar un partido?
- ¿Cómo evoluciona el meta entre patches?

---

## 🖼️ Páginas del dashboard

### 1. Meta Overview
Tabla completa de campeones con picks, bans, presence% y winrate. Top campeones por winrate y por games jugados. Distribución de campeones únicos por patch.

![Meta Overview](capturas/01_meta_overview.jpg)

### 2. Champion Role Explorer
Análisis por rol con scatter de Pickrate% vs Winrate% y evolución del meta por patch. Filtro interactivo por rol (top/jng/mid/bot/sup).

![Role Explorer](capturas/02_role_explorer.jpg)

### 3. Synergy Analyzer
Top dúos por winrate con splash arts automáticos del mejor dúo según el filtro activo.

![Synergy Analyzer](capturas/03_synergy_analyzer.jpg)

### 4. Early Game & Objectives
Impacto de gold diff @15 por posición, winrate por primer objetivo y dominancia de early game por liga.

![Early Game](capturas/04_early_game.png)

---

## ⚙️ Pipeline de datos

```
Oracle Elixir CSV  →  Python (DuckDB)  →  Parquet  →  Power BI
```

| Etapa | Herramienta | Descripción |
|-------|-------------|-------------|
| Extracción | Python + DuckDB | Lee CSV y construye modelo estrella con SQL |
| Transformación | SQL (CTEs, window functions) | Métricas de meta, sinergias, early game |
| Carga | Parquet | Exportación optimizada para Power BI |
| Visualización | Power BI Desktop | Dashboard interactivo con 4 páginas |

---

## 🗂️ Modelo de datos (estrella)

```
Dim_Champion        → catálogo de campeones con rol principal
Dim_Game            → catálogo de partidos
Dim_League          → ligas y regiones
Dim_Date            → tabla de fechas

Fact_ChampionStats  → picks/bans/presence/winrate por league+split+patch
Fact_PlayerGame     → rendimiento individual por partida
Fact_TeamGame       → objetivos y resultado por equipo
Fact_DraftPick      → picks unpivoteados (pick1-5)
Fact_DraftBan       → bans unpivoteados (ban1-5)
Fact_Synergies_Duo  → dúos con winrate y pickrate
```

---

## 🚀 Cómo usarlo

### Requisitos
```bash
pip install duckdb pandas
```

### Actualizar datos
1. Descarga el CSV más reciente desde [Oracle Elixir](https://oracleselixir.com/tools/downloads)
2. Reemplaza `data/raw/oracles_elixir_latest.csv`
3. Ejecuta el pipeline:
```bash
python scripts/01_build_db.py
python scripts/02_validate.py
```
4. Actualiza Power BI — los Parquet en `data/processed/parquet/` se recargan automáticamente

---

## 📁 Estructura del proyecto

```
lol-meta-analytics/
│
├── data/
│   ├── raw/                    ← CSV de Oracle Elixir (no incluido por tamaño)
│   └── processed/
│       ├── lol_meta.duckdb
│       └── parquet/            ← tablas exportadas para Power BI
│
├── scripts/
│   ├── 01_build_db.py          ← pipeline principal
│   └── 02_validate.py          ← validación de integridad
│
├── capturas/                   ← screenshots del dashboard
└── lol_meta_analytics.pbix     ← archivo Power BI (no incluido por tamaño)
```

---

## 🛠️ Stack técnico

- **Python 3.14** — lógica del pipeline
- **DuckDB 1.5.2** — base de datos analítica con SQL avanzado
- **Pandas** — transformación de datos
- **Parquet** — formato columnar optimizado
- **Power BI Desktop** — visualización y medidas DAX
- **Oracle Elixir** — fuente de datos (Season 2026, 34 ligas, ~3,400 partidos)

---

## 👤 Autor

**Alexis Zapata** — Analista BI  
[linkedin.com/in/alexiszapata19](https://linkedin.com/in/alexiszapata19)
