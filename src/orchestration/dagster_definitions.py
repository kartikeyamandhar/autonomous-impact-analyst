"""Top-level Dagster Definitions: assets + jobs + schedule.

Load with: dagster dev -m src.orchestration.dagster_definitions
"""

from dagster import Definitions

from src.orchestration.assets import (
    anomaly_events,
    dbt_artifacts,
    dbt_build,
    executed_actions,
    impact_reports,
    neo4j_graph,
    raw_data_sync,
)
from src.orchestration.jobs import detection_only, full_monitoring_cycle, graph_refresh
from src.orchestration.schedules import monitoring_schedule

defs = Definitions(
    assets=[
        raw_data_sync, dbt_build, dbt_artifacts,
        neo4j_graph, anomaly_events, impact_reports, executed_actions,
    ],
    jobs=[full_monitoring_cycle, graph_refresh, detection_only],
    schedules=[monitoring_schedule],
)
