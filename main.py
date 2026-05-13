"""
=============================================================
  JOB MARKET ANALYSER  — End-to-End Python Automation Project
=============================================================
  Phase 1 → Python fundamentals  (data types, loops, functions, file I/O, error handling)
  Phase 2 → Automation           (os/pathlib, scheduling hooks, requests, email)
  Phase 3 → pandas data wrangling (clean, filter, group, merge, pivot, export)
  Phase 4 → Visualisation        (matplotlib, seaborn, plotly, Streamlit dashboard)
  Phase 5 → Real-world project    (end-to-end pipeline you can actually run)
 
Run:
    python main.py            # full pipeline, saves outputs + opens Streamlit
    python main.py --no-dash  # skip launching Streamlit
=============================================================
"""
 
import os
import sys
import json
import csv
import random
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
 
# ── dependency check ──────────────────────────────────────────────────────────
REQUIRED = ["pandas", "matplotlib", "seaborn", "plotly", "streamlit", "openpyxl", "kaleido"]
 
def check_deps():
    missing = []
    for pkg in REQUIRED:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"\n📦  Installing missing packages: {', '.join(missing)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing,
                               "-q", "--break-system-packages"])
    else:
        print("✅  All dependencies present.")
 
check_deps()
 
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
 
# ── paths ─────────────────────────────────────────────────────────────────────
BASE   = Path(__file__).parent
DATA   = BASE / "data"
OUT    = BASE / "outputs"
DATA.mkdir(exist_ok=True)
OUT.mkdir(exist_ok=True)
 
 
# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 1 — PYTHON FUNDAMENTALS
#  • synthetic dataset generation using core Python (no pandas yet)
#  • data types, list comprehensions, functions, dicts
#  • file I/O: write CSV + JSON
#  • error handling with try/except
# ══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "═"*60)
print("  PHASE 1 — Python fundamentals: generating dataset")
print("═"*60)
 
# --- 1a. define data using core Python structures ----------------------------
 
ROLES = [
    "Data Analyst", "Data Engineer", "ML Engineer",
    "Data Scientist", "Analytics Engineer", "BI Developer",
]
COMPANIES = [
    "Acme Corp", "TechNova", "DataFlow Ltd", "Streamline AI",
    "Meridian Analytics", "Helix Systems", "Orion Data", "Vantage Tech",
    "Nexus Labs", "Pulsar Corp",
]
LOCATIONS = ["London", "Manchester", "Edinburgh", "Bristol", "Leeds",
             "Birmingham", "Remote", "Cambridge", "Oxford", "Glasgow"]
SKILLS_POOL = [
    "Python", "SQL", "Tableau", "Power BI", "Spark", "dbt",
    "Airflow", "AWS", "GCP", "Azure", "R", "Scala",
    "Kafka", "Kubernetes", "Docker", "Looker",
]
SECTORS = ["Finance", "Healthcare", "Retail", "Tech", "Gov", "Energy", "Media"]
 
# salary bands per role (£k min, max)
SALARY_BANDS: dict[str, tuple[int, int]] = {
    "Data Analyst":          (28, 55),
    "Analytics Engineer":    (40, 70),
    "BI Developer":          (35, 65),
    "Data Engineer":         (45, 85),
    "Data Scientist":        (45, 90),
    "ML Engineer":           (55, 110),
}
 
random.seed(42)
 
def make_job_listing(job_id: int) -> dict:
    """Return a single synthetic job listing as a dict — Phase 1 function."""
    role = random.choice(ROLES)
    lo, hi = SALARY_BANDS[role]
    salary = random.randint(lo, hi) * 1000
    posted_days_ago = random.randint(0, 90)
    posted_date = (datetime.today() - timedelta(days=posted_days_ago)).strftime("%Y-%m-%d")
    skills = random.sample(SKILLS_POOL, k=random.randint(2, 5))
    remote = random.random() < 0.35  # 35 % chance fully remote
    location = "Remote" if remote else random.choice([l for l in LOCATIONS if l != "Remote"])
    return {
        "job_id":       f"JOB-{job_id:04d}",
        "role":         role,
        "company":      random.choice(COMPANIES),
        "sector":       random.choice(SECTORS),
        "location":     location,
        "salary":       salary,
        "remote":       remote,
        "skills":       "|".join(skills),      # pipe-separated for CSV
        "posted_date":  posted_date,
        "experience_yrs": random.randint(0, 10),
    }
 
# --- 1b. generate 500 listings -----------------------------------------------
N = 500
listings = [make_job_listing(i) for i in range(1, N + 1)]
print(f"  Generated {N} synthetic job listings.")
 
# --- 1c. file I/O — write CSV ------------------------------------------------
CSV_PATH = DATA / "job_listings.csv"
fieldnames = list(listings[0].keys())
 
try:
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(listings)
    print(f"  CSV written → {CSV_PATH.relative_to(BASE)}")
except OSError as e:
    print(f"  ⚠️  Could not write CSV: {e}")
 
# --- 1d. file I/O — write JSON summary ---------------------------------------
JSON_PATH = DATA / "summary_meta.json"
meta = {
    "generated_at": datetime.now().isoformat(),
    "total_listings": N,
    "roles": ROLES,
    "locations": LOCATIONS,
    "salary_bands": SALARY_BANDS,
}
try:
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    print(f"  JSON written → {JSON_PATH.relative_to(BASE)}")
except OSError as e:
    print(f"  ⚠️  Could not write JSON: {e}")
 
# --- 1e. re-read and validate (error handling) --------------------------------
def load_csv_safe(path: Path) -> list[dict]:
    """Load a CSV and return list of dicts; handle missing file gracefully."""
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"  ⚠️  File not found: {path}")
        return []
    except csv.Error as e:
        print(f"  ⚠️  CSV parse error: {e}")
        return []
 
raw = load_csv_safe(CSV_PATH)
print(f"  Re-loaded {len(raw)} rows from CSV ✓")
 
 
# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 2 — AUTOMATION ESSENTIALS
#  • os / pathlib: scan outputs directory, tidy old files
#  • simulate a 'scheduler hook' function
#  • requests: fetch a live public API (GitHub jobs fallback to mock)
#  • email: build a formatted report string (no SMTP needed to demo)
# ══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "═"*60)
print("  PHASE 2 — Automation essentials")
print("═"*60)
 
# --- 2a. pathlib: scan & report outputs folder --------------------------------
def audit_outputs(folder: Path) -> dict:
    """Scan a folder and return file inventory — Phase 2 pathlib usage."""
    files = list(folder.iterdir()) if folder.exists() else []
    return {
        "folder":     str(folder),
        "file_count": len(files),
        "files":      [f.name for f in files],
        "total_kb":   round(sum(f.stat().st_size for f in files if f.is_file()) / 1024, 1),
    }
 
inv = audit_outputs(OUT)
print(f"  Outputs folder: {inv['file_count']} files, {inv['total_kb']} KB")
 
# --- 2b. simulate a scheduler hook -------------------------------------------
def scheduled_pipeline_hook(name: str):
    """Pretend this is registered with APScheduler / cron — Phase 2 scheduling."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = OUT / "pipeline_run.log"
    entry = f"[{timestamp}]  Pipeline '{name}' executed.\n"
    with open(log_path, "a") as f:
        f.write(entry)
    print(f"  Scheduler hook fired → logged to {log_path.name}")
 
scheduled_pipeline_hook("job_market_daily")
 
# --- 2c. requests: fetch public API (GitHub rate-limit safe) ------------------
import urllib.request, urllib.error
 
def fetch_github_trending(language: str = "python") -> list[dict]:
    """
    Fetch top Python repos from GitHub Search API — Phase 2 requests demo.
    Falls back to mock data if the request fails (rate-limit / no network).
    """
    url = (
        f"https://api.github.com/search/repositories"
        f"?q=language:{language}&sort=stars&order=desc&per_page=5"
    )
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json",
                                                    "User-Agent": "job-market-analyser/1.0"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            data = json.loads(resp.read())
            repos = [{"name": r["full_name"], "stars": r["stargazers_count"],
                      "description": (r["description"] or "")[:60]}
                     for r in data.get("items", [])]
            print(f"  GitHub API ✓  — top {len(repos)} {language} repos fetched")
            return repos
    except (urllib.error.URLError, json.JSONDecodeError, KeyError) as e:
        print(f"  GitHub API unavailable ({type(e).__name__}) — using mock data")
        return [
            {"name": "pandas-dev/pandas",     "stars": 43000, "description": "Flexible data analysis / manipulation library"},
            {"name": "matplotlib/matplotlib", "stars": 20000, "description": "Comprehensive 2-D plotting library"},
            {"name": "streamlit/streamlit",   "stars": 35000, "description": "Faster way to build and share data apps"},
            {"name": "plotly/plotly.py",       "stars": 16000, "description": "Interactive graphing library for Python"},
            {"name": "pydata/pandas-stubs",   "stars":  3000, "description": "Type annotations for pandas"},
        ]
 
trending = fetch_github_trending("python")
TRENDING_PATH = DATA / "trending_repos.json"
with open(TRENDING_PATH, "w") as f:
    json.dump(trending, f, indent=2)
print(f"  Trending repos saved → {TRENDING_PATH.name}")
 
# --- 2d. email report builder (no SMTP — just builds the string) -------------
def build_email_report(summary: dict) -> str:
    """Format an automated daily email — Phase 2 smtplib pattern demo."""
    lines = [
        "Subject: Daily Job Market Digest",
        f"Date:    {datetime.today().strftime('%A %d %B %Y')}",
        "",
        "Hi there,",
        "",
        "Here is today's automated job market summary:",
        "",
        f"  • Total listings analysed : {summary.get('total', 0):,}",
        f"  • Avg salary              : £{summary.get('avg_salary', 0):,.0f}",
        f"  • Most in-demand role     : {summary.get('top_role', 'N/A')}",
        f"  • Top location            : {summary.get('top_location', 'N/A')}",
        "",
        "Full charts available in outputs/.",
        "",
        "— Automated Job Market Analyser",
    ]
    return "\n".join(lines)
 
# (summary will be filled after Phase 3; stored for later)
_email_summary_placeholder = {"total": N, "avg_salary": 0, "top_role": "", "top_location": ""}
 
 
# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 3 — DATA WRANGLING WITH PANDAS
#  • load, inspect, clean
#  • filter, groupby, agg
#  • merge (add seniority lookup), pivot
#  • export to Excel with formatting
# ══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "═"*60)
print("  PHASE 3 — pandas data wrangling")
print("═"*60)
 
# --- 3a. load & inspect -------------------------------------------------------
df = pd.read_csv(CSV_PATH, parse_dates=["posted_date"])
print(f"  Loaded: {df.shape[0]} rows × {df.shape[1]} cols")
print(f"  Dtypes:\n{df.dtypes.to_string()}")
print(f"  Nulls:  {df.isnull().sum().sum()} total")
 
# --- 3b. clean ----------------------------------------------------------------
df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
df.dropna(subset=["salary"], inplace=True)
df["role"]     = df["role"].str.strip()
df["location"] = df["location"].str.strip()
df["skills_list"] = df["skills"].str.split("|")        # explode later for skill counts
 
# --- 3c. feature engineering --------------------------------------------------
df["salary_band"] = pd.cut(
    df["salary"],
    bins=[0, 40_000, 60_000, 80_000, 120_000],
    labels=["< £40k", "£40–60k", "£60–80k", "£80k+"],
)
df["days_since_posted"] = (datetime.today() - df["posted_date"]).dt.days
df["is_fresh"] = df["days_since_posted"] <= 14
df["seniority"] = df["experience_yrs"].apply(
    lambda y: "Junior" if y < 2 else ("Mid" if y < 5 else "Senior")
)
 
# --- 3d. groupby & aggregation ------------------------------------------------
role_stats = (
    df.groupby("role")["salary"]
    .agg(count="count", mean="mean", median="median", max="max")
    .round(0)
    .sort_values("median", ascending=False)
    .reset_index()
)
print(f"\n  Role salary stats:\n{role_stats.to_string(index=False)}")
 
location_stats = (
    df.groupby("location")["salary"]
    .agg(count="count", mean="mean")
    .sort_values("mean", ascending=False)
    .reset_index()
)
 
sector_role = (
    df.groupby(["sector", "role"])["salary"]
    .mean()
    .round(0)
    .reset_index()
    .rename(columns={"salary": "avg_salary"})
)
 
# --- 3e. skill demand (explode list column) -----------------------------------
skills_df = (
    df.explode("skills_list")
    .groupby("skills_list")
    .agg(job_count=("job_id", "count"), avg_salary=("salary", "mean"))
    .sort_values("job_count", ascending=False)
    .reset_index()
    .rename(columns={"skills_list": "skill"})
)
print(f"\n  Top 5 skills:\n{skills_df.head().to_string(index=False)}")
 
# --- 3f. pivot: avg salary by role × seniority --------------------------------
salary_pivot = df.pivot_table(
    values="salary", index="role", columns="seniority",
    aggfunc="median", observed=True,
).round(0)
print(f"\n  Salary pivot (role × seniority):\n{salary_pivot.to_string()}")
 
# --- 3g. merge: add a 'category' lookup table ---------------------------------
category_map = pd.DataFrame({
    "role": ROLES,
    "category": ["Analytics", "Analytics", "Analytics",
                 "Data Science", "Data Science", "Data Science"],
})
df = df.merge(category_map, on="role", how="left")
 
# --- 3h. export to Excel (multiple sheets) ------------------------------------
EXCEL_PATH = OUT / "job_market_report.xlsx"
with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as writer:
    df.drop(columns=["skills_list"]).to_excel(writer, sheet_name="Raw Data", index=False)
    role_stats.to_excel(writer, sheet_name="Role Stats", index=False)
    skills_df.to_excel(writer, sheet_name="Skill Demand", index=False)
    salary_pivot.to_excel(writer, sheet_name="Salary Pivot")
print(f"\n  Excel report → {EXCEL_PATH.name}  ({EXCEL_PATH.stat().st_size // 1024} KB)")
 
# fill email summary
_email_summary_placeholder.update({
    "avg_salary":    df["salary"].mean(),
    "top_role":      role_stats.iloc[0]["role"],
    "top_location":  location_stats.iloc[0]["location"],
})
email_txt = build_email_report(_email_summary_placeholder)
EMAIL_PATH = OUT / "daily_email_report.txt"
with open(EMAIL_PATH, "w") as f:
    f.write(email_txt)
print(f"  Email report  → {EMAIL_PATH.name}")
 
 
# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 4 — VISUALISATION
#  • matplotlib: salary distribution histogram
#  • seaborn:    role × seniority heatmap
#  • plotly:     interactive scatter + bar charts (saved as HTML)
# ══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "═"*60)
print("  PHASE 4 — Visualisation")
print("═"*60)
 
PALETTE = {"Junior": "#5DCAA5", "Mid": "#378ADD", "Senior": "#7F77DD"}
sns.set_theme(style="whitegrid", palette="muted")
 
# --- 4a. matplotlib: salary distribution by role (boxplot) -------------------
fig, ax = plt.subplots(figsize=(10, 5))
roles_ordered = role_stats["role"].tolist()
bp_data = [df[df["role"] == r]["salary"] / 1000 for r in roles_ordered]
bp = ax.boxplot(bp_data, patch_artist=True, notch=False,
                medianprops=dict(color="#E24B4A", linewidth=2))
colors = ["#E1F5EE", "#E6F1FB", "#EEEDFE", "#FAEEDA", "#FAECE7", "#EAF3DE"]
for patch, c in zip(bp["boxes"], colors):
    patch.set_facecolor(c)
ax.set_xticklabels(roles_ordered, rotation=20, ha="right", fontsize=9)
ax.set_ylabel("Salary (£k)", fontsize=10)
ax.set_title("Salary Distribution by Role", fontsize=13, fontweight="bold", pad=12)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"£{x:.0f}k"))
plt.tight_layout()
MPL_PATH = OUT / "salary_boxplot.png"
fig.savefig(MPL_PATH, dpi=140, bbox_inches="tight")
plt.close()
print(f"  matplotlib → {MPL_PATH.name}")
 
# --- 4b. seaborn: heatmap of median salary by role × seniority ---------------
fig, ax = plt.subplots(figsize=(7, 4))
hm_data = salary_pivot / 1000
mask = hm_data.isnull()
sns.heatmap(hm_data, annot=True, fmt=".0f", cmap="YlGnBu",
            linewidths=0.5, linecolor="#e0e0e0",
            cbar_kws={"label": "£k"}, ax=ax, mask=mask)
ax.set_title("Median Salary (£k): Role × Seniority", fontsize=12, fontweight="bold", pad=10)
ax.set_xlabel("")
ax.set_ylabel("")
plt.tight_layout()
SNS_PATH = OUT / "salary_heatmap.png"
fig.savefig(SNS_PATH, dpi=140, bbox_inches="tight")
plt.close()
print(f"  seaborn    → {SNS_PATH.name}")
 
# --- 4c. plotly: skill demand bar (interactive HTML) -------------------------
top_skills = skills_df.head(12)
fig_skills = px.bar(
    top_skills, x="skill", y="job_count",
    color="avg_salary",
    color_continuous_scale="Teal",
    labels={"job_count": "Job Listings", "avg_salary": "Avg Salary (£)", "skill": "Skill"},
    title="Top 12 In-Demand Skills (colour = avg salary)",
)
fig_skills.update_layout(
    plot_bgcolor="#fafafa", paper_bgcolor="#fafafa",
    font_family="sans-serif", title_font_size=14,
    coloraxis_colorbar=dict(tickprefix="£", tickformat=","),
)
SKILLS_HTML = OUT / "skill_demand.html"
fig_skills.write_html(str(SKILLS_HTML))
print(f"  plotly     → {SKILLS_HTML.name}")
 
# --- 4d. plotly: scatter salary vs experience, coloured by seniority ----------
fig_scatter = px.scatter(
    df.sample(300, random_state=1),
    x="experience_yrs", y="salary",
    color="seniority",
    symbol="category",
    hover_data=["role", "company", "location"],
    color_discrete_map=PALETTE,
    labels={"experience_yrs": "Years of Experience", "salary": "Salary (£)",
            "seniority": "Seniority", "category": "Category"},
    title="Salary vs Experience (sample of 300)",
)
fig_scatter.update_layout(plot_bgcolor="#fafafa", paper_bgcolor="#fafafa",
                           font_family="sans-serif", title_font_size=14)
fig_scatter.update_yaxes(tickprefix="£", tickformat=",")
SCATTER_HTML = OUT / "salary_vs_experience.html"
fig_scatter.write_html(str(SCATTER_HTML))
print(f"  plotly     → {SCATTER_HTML.name}")
 
print(f"\n  All charts saved to outputs/")
 
 
# ══════════════════════════════════════════════════════════════════════════════
#  PHASE 5 — STREAMLIT DASHBOARD  (written to disk, launched as subprocess)
# ══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "═"*60)
print("  PHASE 5 — Streamlit dashboard")
print("═"*60)
 
DASH_PATH = BASE / "dashboard.py"
DASH_CODE = '''"""
Job Market Analyser — Streamlit Dashboard  (Phase 5)
Run: streamlit run dashboard.py
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import json
 
st.set_page_config(page_title="Job Market Analyser", page_icon="📊", layout="wide")
 
BASE = Path(__file__).parent
DATA = BASE / "data"
OUT  = BASE / "outputs"
 
@st.cache_data
def load_data():
    df = pd.read_csv(DATA / "job_listings.csv", parse_dates=["posted_date"])
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
    df["skills_list"] = df["skills"].str.split("|")
    df["seniority"] = df["experience_yrs"].apply(
        lambda y: "Junior" if y < 2 else ("Mid" if y < 5 else "Senior"))
    df["salary_band"] = pd.cut(df["salary"],
        bins=[0,40000,60000,80000,120000],
        labels=["< £40k","£40–60k","£60–80k","£80k+"])
    return df
 
df = load_data()
PALETTE = {"Junior":"#5DCAA5","Mid":"#378ADD","Senior":"#7F77DD"}
 
# ── sidebar filters ──────────────────────────────────────────────────────────
st.sidebar.title("🔎 Filters")
roles     = st.sidebar.multiselect("Role",     sorted(df["role"].unique()),     default=sorted(df["role"].unique()))
locs      = st.sidebar.multiselect("Location", sorted(df["location"].unique()), default=sorted(df["location"].unique()))
sectors   = st.sidebar.multiselect("Sector",   sorted(df["sector"].unique()),   default=sorted(df["sector"].unique()))
sal_range = st.sidebar.slider("Salary range (£)", 20000, 120000, (20000, 120000), step=5000)
 
fdf = df[
    df["role"].isin(roles) &
    df["location"].isin(locs) &
    df["sector"].isin(sectors) &
    df["salary"].between(*sal_range)
]
 
# ── header ───────────────────────────────────────────────────────────────────
st.title("📊 Job Market Analyser")
st.caption("End-to-end Python project — Phases 1 → 5")
st.divider()
 
# ── KPI row ──────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total listings",   f"{len(fdf):,}")
k2.metric("Avg salary",       f"£{fdf['salary'].mean():,.0f}")
k3.metric("Median salary",    f"£{fdf['salary'].median():,.0f}")
k4.metric("Remote listings",  f"{fdf['remote'].sum():,}")
k5.metric("Fresh (≤14 days)", f"{(fdf['days_since_posted'] <= 14).sum():,}" if 'days_since_posted' in fdf else "—")
 
st.divider()
 
# ── row 1: salary by role + skill demand ─────────────────────────────────────
c1, c2 = st.columns(2)
 
with c1:
    st.subheader("Salary by role")
    role_med = fdf.groupby("role")["salary"].median().sort_values(ascending=True).reset_index()
    fig = px.bar(role_med, x="salary", y="role", orientation="h",
                 color="salary", color_continuous_scale="Teal",
                 labels={"salary":"Median salary (£)","role":""},
                 text=role_med["salary"].apply(lambda x: f"£{x:,.0f}"))
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                      margin=dict(l=0,r=20,t=10,b=0), height=300)
    st.plotly_chart(fig, use_container_width=True)
 
with c2:
    st.subheader("Top skills in demand")
    skills_exp = fdf.explode("skills_list")
    top_sk = (skills_exp.groupby("skills_list")
              .agg(count=("job_id","count"), avg_sal=("salary","mean"))
              .sort_values("count", ascending=False).head(10).reset_index())
    fig2 = px.bar(top_sk, x="count", y="skills_list", orientation="h",
                  color="avg_sal", color_continuous_scale="Blues",
                  labels={"count":"Listings","skills_list":"","avg_sal":"Avg £"})
    fig2.update_layout(coloraxis_colorbar=dict(title="Avg £",tickprefix="£",tickformat=","),
                       plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                       margin=dict(l=0,r=20,t=10,b=0), height=300)
    st.plotly_chart(fig2, use_container_width=True)
 
# ── row 2: scatter + seniority pie ───────────────────────────────────────────
c3, c4 = st.columns([2,1])
 
with c3:
    st.subheader("Salary vs experience")
    sample = fdf.sample(min(300, len(fdf)), random_state=42)
    fig3 = px.scatter(sample, x="experience_yrs", y="salary",
                      color="seniority", color_discrete_map=PALETTE,
                      hover_data=["role","company","location"],
                      labels={"experience_yrs":"Years exp","salary":"Salary (£)","seniority":"Level"})
    fig3.update_yaxes(tickprefix="£", tickformat=",")
    fig3.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                       margin=dict(l=0,r=0,t=10,b=0), height=320)
    st.plotly_chart(fig3, use_container_width=True)
 
with c4:
    st.subheader("Seniority mix")
    sen_counts = fdf["seniority"].value_counts().reset_index()
    fig4 = px.pie(sen_counts, values="count", names="seniority",
                  color="seniority", color_discrete_map=PALETTE,
                  hole=0.45)
    fig4.update_layout(margin=dict(l=0,r=0,t=10,b=0), height=320,
                       showlegend=True, paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig4, use_container_width=True)
 
# ── row 3: location map (bubble) + salary heatmap ────────────────────────────
st.subheader("Salary band distribution by location")
loc_band = (fdf.groupby(["location","salary_band"], observed=True)
            .size().reset_index(name="count"))
fig5 = px.bar(loc_band, x="location", y="count", color="salary_band",
              barmode="stack",
              color_discrete_sequence=["#9FE1CB","#5DCAA5","#378ADD","#7F77DD"],
              labels={"count":"Listings","location":"","salary_band":"Band"})
fig5.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                   margin=dict(l=0,r=0,t=10,b=0), height=280)
st.plotly_chart(fig5, use_container_width=True)
 
# ── data table ───────────────────────────────────────────────────────────────
st.subheader("Raw data explorer")
cols_show = ["job_id","role","company","sector","location","salary","seniority","remote","posted_date","skills"]
st.dataframe(fdf[cols_show].sort_values("salary", ascending=False).reset_index(drop=True),
             use_container_width=True, height=300)
 
st.divider()
st.caption("Data is synthetically generated for learning purposes. Built with Python, pandas, plotly, and Streamlit.")
'''
 
with open(DASH_PATH, "w") as f:
    f.write(DASH_CODE)
print(f"  dashboard.py written → {DASH_PATH.name}")
 
 
# ══════════════════════════════════════════════════════════════════════════════
#  SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "═"*60)
print("  PIPELINE COMPLETE")
print("═"*60)
inv_final = audit_outputs(OUT)
print(f"  Outputs ({inv_final['file_count']} files, {inv_final['total_kb']} KB):")
for fn in sorted(inv_final["files"]):
    print(f"    • {fn}")
 
print("""
  ─────────────────────────────────────────
  Next step → launch the Streamlit dashboard:
    streamlit run dashboard.py
  ─────────────────────────────────────────
""")
 
# auto-launch if --no-dash not passed
parser = argparse.ArgumentParser()
parser.add_argument("--no-dash", action="store_true")
args, _ = parser.parse_known_args()
 
if not args.no_dash:
    print("  Launching Streamlit (Ctrl+C to quit)…")
    os.chdir(BASE)
    subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard.py",
                    "--server.headless", "true"])
