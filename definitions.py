from dagster import Definitions
from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource
import assets.sling_assets
from dlt_pipelines import dagster_github_assets, dlt_source_assets
from assets.derived_assets import derived_analytics_assets
from dagster import (
    Definitions,
    load_assets_from_modules,
)

from dagster_sling import SlingResource, SlingConnectionResource

duckdb_database = "data/github_data.duckdb"

sling_resource = SlingResource(
    connections=[
        SlingConnectionResource(
            name="postgres",
            type="postgres",
            host="sample-data.popsql.io",
            user="demo",
            database="marker",
            password="demo",
        ),
        SlingConnectionResource(
            name="duckdb",
            type="duckdb",
            instance=duckdb_database,
        ),
    ]
)

import assets

sling_assets = load_assets_from_modules([assets.sling_assets])

defs = Definitions(
    assets=[
        *dlt_source_assets,
        dagster_github_assets,
        *sling_assets,
        *derived_analytics_assets, 
    ],
    resources={
        "dlt": DagsterDltResource(),
        "sling": sling_resource,
        "duckdb": DuckDBResource(database=duckdb_database),
    }
)

