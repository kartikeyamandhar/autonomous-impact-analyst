"""Asset jobs: named selections of the pipeline runnable on demand or schedule."""

from dagster import define_asset_job

full_monitoring_cycle = define_asset_job(
    name="full_monitoring_cycle",
    selection=[
        "raw_data_sync", "dbt_build", "dbt_artifacts",
        "neo4j_graph", "anomaly_events", "impact_reports", "executed_actions",
    ],
)

graph_refresh = define_asset_job(
    name="graph_refresh",
    selection=["dbt_artifacts", "neo4j_graph"],
)

detection_only = define_asset_job(
    name="detection_only",
    selection=["anomaly_events", "impact_reports", "executed_actions"],
)
