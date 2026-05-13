"""
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
