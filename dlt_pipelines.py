from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt_sources.github import github_reactions

from dagster import AssetKey, SourceAsset, AutoMaterializePolicy, AutomationCondition, AssetSpec
from typing import Any, Iterable, Mapping
from dagster_dlt import DagsterDltTranslator

#this is used just to add some additional metadata - not required
class CustomDltTranslator(DagsterDltTranslator):
    def __init__(self, cron_schedule: str = "*/5 * * * *", shard_name: str = "main"):
        super().__init__()
        self.cron_schedule = cron_schedule
        self.shard_name = shard_name

    def get_tags(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
        "dagster/kind/duckdb": "",
        "marketing": "",
        "ingestion": "",
    }

    def get_description(self, stream_definition):
        return "Dlt pipeline to ingest github data"

    def get_metadata(self, stream_definition: Mapping[str, Any]) -> Mapping[str, Any]:
        key: AssetKey = self.get_asset_key(stream_definition)
        return {
            **super().get_metadata(stream_definition),
            "dagster/table_name": ".".join(key.path),
        }

    def get_auto_materialize_policy(
        self, stream_definition: Mapping[str, Any]
    ) -> AutoMaterializePolicy | None:
        return (
            AutomationCondition.cron_tick_passed(self.cron_schedule)
            & ~AutomationCondition.in_progress()
        ).as_auto_materialize_policy()

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = next(iter(super().get_deps_asset_key(stream_definition)))
        return AssetKey([self.shard_name, *stream_asset_key.path])

@dlt_assets(
    dlt_source=github_reactions(
        "dagster-io", "dagster", max_items=250
    ),
    dlt_pipeline=pipeline(
        pipeline_name="github_issues",
        dataset_name="gh_data",
        destination="duckdb",
        progress="log",
    ),
    name="github",
    group_name="Pipelines",
    dagster_dlt_translator=CustomDltTranslator()

)
def dagster_github_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)

def get_tags_for_key(key: AssetKey):
    path_str = "_".join(key.path).lower()

    if "pull_requests" in path_str:
        return {"dagster/kind/github": "", "pull_requests": "", "external":""}
    elif "issues" in path_str:
        return {"dagster/kind/github": "", "issues": "", "external":""}
    else:
        return {"dagster/kind/github": "", "other": "", "external":""}

#this is used just to change the group name of the source asset - not required
dlt_source_assets = [
    *[
        SourceAsset(key, group_name="Raw", tags=get_tags_for_key(key), description=f"GitHub data for {key.path[-1]}")
        for key in dagster_github_assets.dependency_keys
    ]
]
