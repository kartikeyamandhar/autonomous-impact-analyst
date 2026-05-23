"""Run the full monitoring cycle every 15 minutes."""

from dagster import ScheduleDefinition

from src.orchestration.jobs import full_monitoring_cycle

monitoring_schedule = ScheduleDefinition(
    name="monitoring_schedule",
    job=full_monitoring_cycle,
    cron_schedule="*/15 * * * *",
)
