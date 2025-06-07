# 🧪 Embedded ELT Demo

This project demonstrates a modern **Embedded ELT** architecture powered by:

* **DLT** – Pulls GitHub data using the `github_reactions` source

* **Sling** – Syncs data from a simulated B2B Postgres SaaS application

* **Dagster** – Provides orchestration, lineage, and observability

* **DuckDB** – Serves as the local analytical warehouse

Built to showcase composable ingestion pipelines and declarative transformations using asset-driven orchestration.

---

## 🚀 Quickstart

### 1. Set up your environment

git clone &lt;repo>
cd embedded_elt_demo
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt


### 2. Set your GitHub access token

DLT requires a GitHub token to authenticate API access.

export SOURCES__GITHUB__ACCESS_TOKEN="ghp_your_token_here"


To persist this:

**Zsh**

echo 'export SOURCES__GITHUB__ACCESS_TOKEN="ghp_your_token_here"' >> ~/.zshrc
source ~/.zshrc


**Bash**

echo 'export SOURCES__GITHUB__ACCESS_TOKEN="ghp_your_token_here"' >> ~/.bashrc
source ~/.bashrc


---

## 🧠 Project Overview

### Pipelines

| **Source** | **Tool** | **Location** | **Destination** |
|---|---|---|---|
| GitHub | DLT | `dlt_pipelines.py` | DuckDB |
| Postgres SaaS | Sling | `assets/sling_assets.py` | DuckDB |

All assets are:

* Grouped into Raw, Pipelines, and Analytics

* Tagged using:

  * `dagster/kind/github`

  * `dagster/kind/postgres`

  * `dagster/kind/duckdb`

  * Functional labels: `raw`, `marketing`, `ingestion`

---

## 🧮 Derived Analytics Assets

Located in `assets/derived_assets.py`, these assets generate materialized tables in DuckDB, such as:

* `top_comment_authors`

* `avg_comment_length`

* `user_core_activity_summary`

* `team_performance_summary`

* `nps_by_subscription_month`

Each is defined with rich metadata and upstream dependencies.

---

## 🐘 Data Sources

### DLT GitHub Source

* Uses `dlt_sources.github.github_reactions`

* Tracks issues, pull requests, comments, and reactions

### Sling Postgres Source

* Pulls sample SaaS data from `sample-data.popsql.io`

* Tables include:

  * `users`

  * `tickets`

  * `teams`

  * `events`

  * `nps_responses`

* Sling asset keys follow the format: `target/public/<table_name>`

---

## 🧭 Running the Project

Start Dagster:

dagster dev


Navigate to `http://localhost:3000` to:

* Visualize global lineage

* Track materialization history

* Explore asset metadata and logs

---

## 📝 Notes

* **DLT** uses a `.dlt/` folder to track pipeline state.
  If you change directories or encounter file not found errors, delete the `.dlt/` folder and retry.

* **Sling** simulates Postgres and runs entirely locally.
  No cloud resources or database setup needed.

---

## 🎯 Key Concepts Demonstrated

✅ Embedded ELT architecture
✅ GitHub and Postgres ingestion using two tools
✅ Asset materialization with lineage and metadata
✅ Grouping and tagging for observability
✅ Local, production-style pipeline orchestration

---

## ✅ Status

✅ Pipelines working

✅ Analytics assets materialized

✅ Tags, metadata, and grouping enabled

✅ Dagster lineage view connected end-to-en
