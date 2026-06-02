{
  "Comment": "Ecommerce Lakehouse ETL Pipeline",
  "StartAt": "RunETLJobs",
  "States": {
    "RunETLJobs": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "GlueProducts",
          "States": {
            "GlueProducts": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "${glue_products_job_name}" },
              "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 30, "MaxAttempts": 3, "BackoffRate": 2 }],
              "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "GlueProductsFailed", "ResultPath": "$.error" }],
              "TimeoutSeconds": 3600,
              "End": true
            },
            "GlueProductsFailed": {
              "Type": "Task",
              "Resource": "arn:aws:states:::sns:publish",
              "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Glue job ${glue_products_job_name} FAILED", "Subject": "Lakehouse Alert: Products Job Failed" },
              "End": true
            }
          }
        },
        {
          "StartAt": "GlueOrders",
          "States": {
            "GlueOrders": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "${glue_orders_job_name}" },
              "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 30, "MaxAttempts": 3, "BackoffRate": 2 }],
              "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "GlueOrdersFailed", "ResultPath": "$.error" }],
              "TimeoutSeconds": 3600,
              "End": true
            },
            "GlueOrdersFailed": {
              "Type": "Task",
              "Resource": "arn:aws:states:::sns:publish",
              "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Glue job ${glue_orders_job_name} FAILED", "Subject": "Lakehouse Alert: Orders Job Failed" },
              "End": true
            }
          }
        },
        {
          "StartAt": "GlueOrderItems",
          "States": {
            "GlueOrderItems": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "${glue_order_items_job_name}" },
              "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 30, "MaxAttempts": 3, "BackoffRate": 2 }],
              "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "GlueOrderItemsFailed", "ResultPath": "$.error" }],
              "TimeoutSeconds": 3600,
              "End": true
            },
            "GlueOrderItemsFailed": {
              "Type": "Task",
              "Resource": "arn:aws:states:::sns:publish",
              "Parameters": { "TopicArn": "${sns_topic_arn}", "Message": "Glue job ${glue_order_items_job_name} FAILED", "Subject": "Lakehouse Alert: Order Items Job Failed" },
              "End": true
            }
          }
        }
      ],
      "Next": "RunGlueCrawler",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "PipelineFailed", "ResultPath": "$.error" }]
    },

    "RunGlueCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": { "Name": "${glue_crawler_name}" },
      "Catch": [
        { "ErrorEquals": ["Glue.CrawlerRunningException"], "Next": "WaitForCrawler", "ResultPath": "$.crawlerError" },
        { "ErrorEquals": ["States.ALL"], "Next": "PipelineFailed", "ResultPath": "$.error" }
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
        "QueryString": "SELECT (SELECT COUNT(*) FROM products) AS products, (SELECT COUNT(*) FROM orders) AS orders, (SELECT COUNT(*) FROM order_items) AS order_items",
        "WorkGroup": "ecommerce-lakehouse-dev",
        "ResultConfiguration": { "OutputLocation": "s3://${bucket_name}/athena-results/step-functions/" }
      },
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "PipelineFailed", "ResultPath": "$.error" }],
      "Next": "PipelineSuccess"
    },

    "PipelineSuccess": { "Type": "Succeed" },

    "PipelineFailed": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "${sns_topic_arn}",
        "Message.$": "States.Format('Lakehouse pipeline FAILED. Error: {}', $.error)",
        "Subject": "Lakehouse Alert: Critical Pipeline Failure"
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
