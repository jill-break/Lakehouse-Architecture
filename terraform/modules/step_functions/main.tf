locals {
  definition = templatefile("${path.root}/../step_functions/state_machine.json.tpl", {
    glue_products_job_name    = var.glue_products_job_name
    glue_orders_job_name      = var.glue_orders_job_name
    glue_order_items_job_name = var.glue_order_items_job_name
    glue_crawler_name         = var.glue_crawler_name
    sns_topic_arn             = var.sns_topic_arn
    bucket_name               = var.bucket_name
  })
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project_name}-${var.environment}-pipeline"
  role_arn = var.step_functions_role_arn

  definition = local.definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tracing_configuration {
    enabled = true
  }
}

resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/states/${var.project_name}-${var.environment}-pipeline"
  retention_in_days = 14
}
