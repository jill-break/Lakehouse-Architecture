resource "aws_athena_workgroup" "lakehouse" {
  name        = "${var.project_name}-${var.environment}"
  description = "Athena workgroup for querying Delta tables in the lakehouse"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
  }
}

# Athena named queries for quick validation after each pipeline run
resource "aws_athena_named_query" "count_products" {
  name      = "lakehouse-count-products"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = var.glue_database_name
  query     = "SELECT COUNT(*) AS product_count FROM products;"
}

resource "aws_athena_named_query" "count_orders" {
  name      = "lakehouse-count-orders"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = var.glue_database_name
  query     = "SELECT COUNT(*) AS order_count, MIN(date) AS earliest, MAX(date) AS latest FROM orders;"
}

resource "aws_athena_named_query" "count_order_items" {
  name      = "lakehouse-count-order-items"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = var.glue_database_name
  query     = "SELECT COUNT(*) AS item_count FROM order_items;"
}

resource "aws_athena_named_query" "revenue_by_dept" {
  name      = "lakehouse-revenue-by-department"
  workgroup = aws_athena_workgroup.lakehouse.id
  database  = var.glue_database_name
  query     = <<-SQL
    SELECT
      p.department,
      COUNT(DISTINCT o.order_id)   AS order_count,
      SUM(o.total_amount)          AS total_revenue,
      AVG(o.total_amount)          AS avg_order_value
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p     ON oi.product_id = p.product_id
    GROUP BY p.department
    ORDER BY total_revenue DESC;
  SQL
}
