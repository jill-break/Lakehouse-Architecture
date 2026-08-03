"""
Structured JSON logging and CloudWatch custom metrics for the Glue ETL jobs.

Every line emitted is a single JSON object so CloudWatch Logs Insights can
filter on severity and query individual fields, e.g.

    fields @timestamp, job, event, rows
    | filter level = "ERROR"
    | filter job = "orders"
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

METRIC_NAMESPACE = "Lakehouse/ETL"


class JsonFormatter(logging.Formatter):
    """Render a log record as one JSON object, merging any structured extras."""

    RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
        "message",
        "asctime",
        "taskName",
    }

    def __init__(self, job: str, run_id: str) -> None:
        super().__init__()
        self.job = job
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "job": self.job,
            "run_id": self.run_id,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in self.RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(job: str, run_id: str = None) -> logging.Logger:
    """Return a logger that writes single-line JSON to stdout (CloudWatch)."""
    run_id = run_id or os.environ.get("JOB_RUN_ID", "local")
    logger = logging.getLogger(f"lakehouse.{job}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(job=job, run_id=run_id))
    logger.addHandler(handler)
    return logger


def resolve_run_id(argv: list) -> str:
    """
    Pull the Glue run id out of sys.argv without requiring it in the job's
    getResolvedOptions list. Correlating every log line with a run id is the
    difference between a searchable log group and a wall of text.
    """
    if "--JOB_RUN_ID" in argv:
        index = argv.index("--JOB_RUN_ID") + 1
        if index < len(argv):
            return argv[index]
    return os.environ.get("JOB_RUN_ID", "local")


def emit_metric(job: str, metric_name: str, value: float, unit: str = "Count") -> None:
    """
    Publish a custom CloudWatch metric so row counts can be graphed and alarmed
    on. Metric failures must never fail the ETL, so errors are logged and
    swallowed.
    """
    try:
        import boto3

        boto3.client("cloudwatch").put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "Dataset", "Value": job}],
                    "Value": float(value),
                    "Unit": unit,
                }
            ],
        )
    except Exception as exc:  # noqa: BLE001 - metrics are best effort
        logging.getLogger(f"lakehouse.{job}").warning(
            "metric_publish_failed", extra={"metric": metric_name, "error": str(exc)}
        )
