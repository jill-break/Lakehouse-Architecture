{
  "Comment": "Ecommerce Lakehouse ETL Pipeline — sequential execution: products → orders → order_items → crawler → athena",
  "StartAt": "GlueProducts",
  "States": {

    "GlueProducts": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_products_job_name}" },
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "TimeoutSeconds": 3600,
      "Next": "GlueOrders"
    },

    "GlueOrders": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_orders_job_name}" },
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "TimeoutSeconds": 3600,
      "Next": "GlueOrderItems"
    },

    "GlueOrderItems": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_order_items_job_name}" },
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "TimeoutSeconds": 3600,
      "Next": "RunGlueCrawler"
    },

    "RunGlueCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": { "Name": "${glue_crawler_name}" },
      "Catch": [
        { "ErrorEquals": ["Glue.CrawlerRunningException"], "Next": "WaitForCrawler", "ResultPath": "$.crawlerError" },
        { "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }
      ],
      "Next": "WaitForCrawler"
    },

    "WaitForCrawler": {
      "Type": "Wait",
      "Seconds": 60,
      "Next": "CheckCrawlerStatus"
    },

    "CheckCrawlerStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Parameters": { "Name": "${glue_crawler_name}" },
      "Next": "IsCrawlerDone"
    },

    "IsCrawlerDone": {
      "Type": "Choice",
      "Choices": [{ "Variable": "$.Crawler.State", "StringEquals": "READY", "Next": "AthenaValidation" }],
      "Default": "WaitForCrawler"
    },

    "AthenaValidation": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString": "SELECT (SELECT COUNT(*) FROM ecommerce_lakehouse_dev.products) AS products, (SELECT COUNT(*) FROM ecommerce_lakehouse_dev.orders) AS orders, (SELECT COUNT(*) FROM ecommerce_lakehouse_dev.order_items) AS order_items",
        "WorkGroup": "ecommerce-lakehouse-dev",
        "QueryExecutionContext": { "Database": "ecommerce_lakehouse_dev", "Catalog": "AwsDataCatalog" },
        "ResultConfiguration": { "OutputLocation": "s3://${bucket_name}/athena-results/step-functions/" }
      },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "Next": "PipelineSuccess"
    },

    "PipelineSuccess": { "Type": "Succeed" },

    "JobFailed": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${sns_topic_arn}",
        "Message.$": "States.Format('Lakehouse pipeline FAILED. Error: {}', $.error)",
        "Subject": "Lakehouse Alert: Pipeline Failure"
      },
      "Next": "FailState"
    },

    "FailState": {
      "Type": "Fail",
      "Error": "PipelineFailed",
      "Cause": "One or more ETL steps failed — check CloudWatch logs."
    }
  }
}
