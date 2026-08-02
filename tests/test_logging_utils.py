"""
Structured logging and CloudWatch metrics (MD-9, MD-10).

Bare print() gave CloudWatch Logs Insights nothing to filter or query on. Each
line is now a JSON object carrying the job name, run id and any structured
fields the caller attached.
"""

import json
import logging

import boto3
import pytest
from moto import mock_aws

from common.logging_utils import emit_metric, get_logger, resolve_run_id


def read_json_lines(capsys):
    captured = capsys.readouterr().out.strip().splitlines()
    return [json.loads(line) for line in captured if line.startswith("{")]


def test_log_line_is_json_with_job_and_run_id(capsys):
    log = get_logger("orders", "jr_abc123")
    log.info("validation_complete")

    record = read_json_lines(capsys)[0]
    assert record["job"] == "orders"
    assert record["run_id"] == "jr_abc123"
    assert record["event"] == "validation_complete"
    assert record["level"] == "INFO"
    assert "timestamp" in record


def test_structured_extras_become_queryable_fields(capsys):
    log = get_logger("orders", "jr_1")
    log.info("validation_complete", extra={"total_rows": 500, "rejected_rows": 12})

    record = read_json_lines(capsys)[0]
    assert record["total_rows"] == 500
    assert record["rejected_rows"] == 12


def test_levels_are_distinguishable(capsys):
    log = get_logger("orders", "jr_1")
    log.info("fine")
    log.warning("suspicious")
    log.error("broken")

    levels = [record["level"] for record in read_json_lines(capsys)]
    assert levels == ["INFO", "WARNING", "ERROR"]


def test_repeated_get_logger_does_not_duplicate_handlers(capsys):
    get_logger("orders", "jr_1")
    log = get_logger("orders", "jr_2")
    log.info("once")

    assert len(read_json_lines(capsys)) == 1


def test_exceptions_are_serialised(capsys):
    log = get_logger("orders", "jr_1")
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("job_failed")

    record = read_json_lines(capsys)[0]
    assert "ValueError: boom" in record["exception"]


@pytest.mark.parametrize(
    "argv,expected",
    [
        (["script.py", "--JOB_RUN_ID", "jr_9f3", "--S3_BUCKET", "b"], "jr_9f3"),
        (["script.py", "--S3_BUCKET", "b"], "local"),
        (["script.py", "--JOB_RUN_ID"], "local"),
    ],
)
def test_resolve_run_id(argv, expected, monkeypatch):
    monkeypatch.delenv("JOB_RUN_ID", raising=False)
    assert resolve_run_id(argv) == expected


def test_emit_metric_publishes_to_cloudwatch(aws_credentials):
    with mock_aws():
        emit_metric("orders", "RowsIngested", 493)

        response = boto3.client("cloudwatch", region_name="us-east-1").list_metrics(
            Namespace="Lakehouse/ETL"
        )
        names = [metric["MetricName"] for metric in response["Metrics"]]
        assert "RowsIngested" in names


def test_emit_metric_never_fails_the_job(monkeypatch, caplog):
    """A metrics outage must not take the ETL down with it."""

    def explode(*args, **kwargs):
        raise RuntimeError("cloudwatch unavailable")

    monkeypatch.setattr(boto3, "client", explode)

    with caplog.at_level(logging.WARNING):
        emit_metric("orders", "RowsIngested", 1)

    assert "metric_publish_failed" in caplog.text
