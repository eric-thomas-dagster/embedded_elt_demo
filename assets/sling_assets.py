from dagster_sling import sling_assets, SlingResource
from dagster import AssetKey, SourceAsset, AutoMaterializePolicy, AutomationCondition, AssetSpec
from typing import Any, Iterable, Mapping
from dagster_sling.dagster_sling_translator import DagsterSlingTranslator


#this is used just to add some additional metadata - not required
class CustomSlingTranslator(DagsterSlingTranslator):
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

    def get_group_name(self, stream_definition):
        return "Pipelines"
    
    def get_description(self, stream_definition):
        return "Sling pipeline to ingest fake B2B SaaS company data"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = next(iter(super().get_deps_asset_key(stream_definition)))
        return AssetKey([self.shard_name, *stream_asset_key.path])

replication_config = {
    "source": "postgres",
    "target": "duckdb",
    "defaults": {
        "mode": "full-refresh",
        "object": "{stream_schema}_{stream_table}"
    },
    "streams": {
        "public.tickets": None,
        "public.events": None,
        "public.teams": None,
        "public.users": None,
        "public.nps_responses": None
    }
}

@sling_assets(replication_config=replication_config,dagster_sling_translator=CustomSlingTranslator())
def sling_sync_assets(context, sling: SlingResource)-> Iterable[Any]:
    yield from sling.replicate(context=context)
    for row in sling.stream_raw_logs():
        context.log.info(row)

def get_tags_for_sling_key(key: AssetKey):
    path_str = "_".join(key.path).lower()

    if "tickets" in path_str:
        return {"dagster/kind/postgres": "", "tickets": "", "raw": "", "external":""}
    elif "users" in path_str:
        return {"dagster/kind/postgres": "", "users": "", "raw": "", "external":""}
    elif "teams" in path_str:
        return {"dagster/kind/postgres": "", "teams": "", "raw": "", "external":""}
    elif "events" in path_str:
        return {"dagster/kind/postgres": "", "events": "", "raw": "", "external":""}
    elif "nps" in path_str:
        return {"dagster/kind/postgres": "", "nps": "", "raw": "", "external":""}
    else:
        return {"dagster/kind/postgres": "", "raw": "", "external":""}

sling_source_assets = [
    SourceAsset(
        key,
        group_name="Raw",
        tags=get_tags_for_sling_key(key),
        description=f"fake B2B SaaS company data for {key.path[-1]}"
    )
    for key in sling_sync_assets.dependency_keys
]