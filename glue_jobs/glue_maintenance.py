"""
Glue job: Delta Lake table maintenance.

Every pipeline run appends files to each Delta table — with day-grain
partitions and a few hundred rows per batch that means a steady drip of tiny
files, which inflates query planning and S3 request costs until someone
notices. This job compacts them, Z-ORDERs on the column each table is actually
filtered by, and vacuums tombstoned files.

It also carries the symlink-manifest generation that the pipeline used to run
as a separate job. Athena engine v3 reads native Delta tables registered by the
crawler (delta_target with write_manifest = false), so manifests are off by
default; set --GENERATE_MANIFESTS true if a catalog ends up needing them.

Job parameters:
  --S3_BUCKET, --JOB_NAME
  --VACUUM_RETENTION_HOURS  default 168 (7 days, Delta's safety floor)
  --GENERATE_MANIFESTS      default "false"
"""

from delta.tables import DeltaTable

# table name -> the column worth clustering on
ZORDER_COLUMNS = {
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_id",
}


def maintain_table(spark, path: str, zorder_col: str, retention_hours: int, log) -> str:
    """Compact, cluster and vacuum one Delta table. Returns a status string."""
    if not DeltaTable.isDeltaTable(spark, path):
        log.warning("table_missing", extra={"path": path})
        return "missing"

    delta_table = DeltaTable.forPath(spark, path)

    log.info("optimizing", extra={"path": path, "zorder_col": zorder_col})
    delta_table.optimize().executeZOrderBy(zorder_col)

    log.info("vacuuming", extra={"path": path, "retention_hours": retention_hours})
    delta_table.vacuum(retention_hours)

    return "ok"


def main(spark, args: dict, run_id: str = "local") -> dict:
    from common.logging_utils import get_logger

    log = get_logger("maintenance", run_id)
    bucket = args["S3_BUCKET"]
    retention_hours = int(args.get("VACUUM_RETENTION_HOURS", 168))
    generate_manifests = str(args.get("GENERATE_MANIFESTS", "false")).lower() == "true"

    results = {}
    for table, zorder_col in ZORDER_COLUMNS.items():
        path = f"s3://{bucket}/lakehouse-dwh/{table}"
        results[table] = maintain_table(spark, path, zorder_col, retention_hours, log)

        if generate_manifests and results[table] == "ok":
            log.info("generating_manifest", extra={"path": path})
            DeltaTable.forPath(spark, path).generate("symlink_format_manifest")

    log.info("maintenance_complete", extra=results)
    return results


if __name__ == "__main__":
    import sys

    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    from common.logging_utils import resolve_run_id

    job_args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])

    glue_context = GlueContext(SparkContext())
    glue_job = Job(glue_context)
    glue_job.init(job_args["JOB_NAME"], job_args)

    main(glue_context.spark_session, job_args, resolve_run_id(sys.argv))

    glue_job.commit()
