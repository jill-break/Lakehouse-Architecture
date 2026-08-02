{
  "Comment": "Ecommerce Lakehouse ETL pipeline: (products || orders) -> order_items -> maintenance -> crawler -> athena validation",
  "StartAt": "IngestIndependentDatasets",
  "TimeoutSeconds": 5400,
  "States": {

    "IngestIndependentDatasets": {
      "Type": "Parallel",
      "Comment": "products and orders share no dependency, so they run concurrently. order_items depends on BOTH (referential integrity on order_id and product_id) and therefore waits for this state to complete.",
      "Branches": [
        {
          "StartAt": "GlueProducts",
          "States": {
            "GlueProducts": {
              "Type": "Task",
              "Resource": "arn:aws:states:::glue:startJobRun.sync",
              "Parameters": { "JobName": "${glue_products_job_name}" },
              "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
              "TimeoutSeconds": 3600,
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
              "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
              "TimeoutSeconds": 3600,
              "End": true
            }
          }
        }
      ],
      "ResultPath": "$.ingest",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "Next": "GlueOrderItems"
    },

    "GlueOrderItems": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_order_items_job_name}" },
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "TimeoutSeconds": 3600,
      "ResultPath": "$.orderItems",
      "Next": "DeltaMaintenance"
    },

    "DeltaMaintenance": {
      "Type": "Task",
      "Comment": "OPTIMIZE + Z-ORDER + VACUUM so each batch's small files are compacted before anyone queries them.",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": { "JobName": "${glue_maintenance_job_name}" },
      "Retry": [{ "ErrorEquals": ["Glue.ConcurrentRunsExceededException"], "IntervalSeconds": 60, "MaxAttempts": 3, "BackoffRate": 2 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "TimeoutSeconds": 1800,
      "ResultPath": "$.maintenance",
      "Next": "InitCrawlerPolling"
    },

    "InitCrawlerPolling": {
      "Type": "Pass",
      "Result": { "count": 0 },
      "ResultPath": "$.crawlerAttempts",
      "Next": "RunGlueCrawler"
    },

    "RunGlueCrawler": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler",
      "Parameters": { "Name": "${glue_crawler_name}" },
      "ResultPath": "$.crawlerStart",
      "Catch": [
        { "ErrorEquals": ["Glue.CrawlerRunningException"], "Next": "WaitForCrawler", "ResultPath": "$.crawlerError" },
        { "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }
      ],
      "Next": "WaitForCrawler"
    },

    "WaitForCrawler": {
      "Type": "Wait",
      "Seconds": 30,
      "Next": "CheckCrawlerStatus"
    },

    "CheckCrawlerStatus": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler",
      "Parameters": { "Name": "${glue_crawler_name}" },
      "ResultPath": "$.crawlerStatus",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "Next": "IsCrawlerDone"
    },

    "IsCrawlerDone": {
      "Type": "Choice",
      "Comment": "A crawler returns to READY whether it succeeded or failed, so the outcome must be read from LastCrawl.Status. The attempt cap stops a crawler wedged in STOPPING from looping (and billing) forever.",
      "Choices": [
        {
          "And": [
            { "Variable": "$.crawlerStatus.Crawler.State", "StringEquals": "READY" },
            { "Variable": "$.crawlerStatus.Crawler.LastCrawl", "IsPresent": true },
            { "Variable": "$.crawlerStatus.Crawler.LastCrawl.Status", "StringEquals": "SUCCEEDED" }
          ],
          "Next": "AthenaValidation"
        },
        {
          "And": [
            { "Variable": "$.crawlerStatus.Crawler.State", "StringEquals": "READY" },
            { "Variable": "$.crawlerStatus.Crawler.LastCrawl", "IsPresent": true },
            { "Or": [
              { "Variable": "$.crawlerStatus.Crawler.LastCrawl.Status", "StringEquals": "FAILED" },
              { "Variable": "$.crawlerStatus.Crawler.LastCrawl.Status", "StringEquals": "CANCELLED" }
            ]}
          ],
          "Next": "CrawlerFailed"
        },
        {
          "Variable": "$.crawlerAttempts.count",
          "NumericGreaterThanEquals": 40,
          "Next": "CrawlerTimedOut"
        }
      ],
      "Default": "IncrementCrawlerAttempts"
    },

    "IncrementCrawlerAttempts": {
      "Type": "Pass",
      "Parameters": { "count.$": "States.MathAdd($.crawlerAttempts.count, 1)" },
      "ResultPath": "$.crawlerAttempts",
      "Next": "WaitForCrawler"
    },

    "CrawlerFailed": {
      "Type": "Pass",
      "Parameters": {
        "Error": "CrawlerFailed",
        "Cause.$": "States.Format('Glue crawler ${glue_crawler_name} finished with status {}', $.crawlerStatus.Crawler.LastCrawl.Status)"
      },
      "ResultPath": "$.error",
      "Next": "JobFailed"
    },

    "CrawlerTimedOut": {
      "Type": "Pass",
      "Parameters": {
        "Error": "CrawlerPollTimeout",
        "Cause": "Glue crawler did not reach a terminal state within the polling budget (40 x 30s)."
      },
      "ResultPath": "$.error",
      "Next": "JobFailed"
    },

    "AthenaValidation": {
      "Type": "Task",
      "Comment": "Result location comes from the workgroup, which sets enforce_workgroup_configuration = true.",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString": "SELECT CAST((SELECT COUNT(*) FROM ${glue_database_name}.products) + (SELECT COUNT(*) FROM ${glue_database_name}.orders) + (SELECT COUNT(*) FROM ${glue_database_name}.order_items) AS VARCHAR) AS total_rows",
        "WorkGroup": "${athena_workgroup_name}",
        "QueryExecutionContext": { "Database": "${glue_database_name}", "Catalog": "AwsDataCatalog" }
      },
      "ResultPath": "$.athena",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "Next": "GetValidationResults"
    },

    "GetValidationResults": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:athena:getQueryResults",
      "Parameters": { "QueryExecutionId.$": "$.athena.QueryExecution.QueryExecutionId" },
      "ResultPath": "$.validation",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "JobFailed", "ResultPath": "$.error" }],
      "Next": "IsWarehousePopulated"
    },

    "IsWarehousePopulated": {
      "Type": "Choice",
      "Comment": "A validation query that counts rows but asserts nothing lets an empty warehouse pass as success.",
      "Choices": [
        {
          "And": [
            { "Variable": "$.validation.ResultSet.Rows[1].Data[0].VarCharValue", "IsPresent": true },
            { "Variable": "$.validation.ResultSet.Rows[1].Data[0].VarCharValue", "StringEquals": "0" }
          ],
          "Next": "EmptyWarehouse"
        }
      ],
      "Default": "PipelineSuccess"
    },

    "EmptyWarehouse": {
      "Type": "Pass",
      "Parameters": {
        "Error": "EmptyWarehouse",
        "Cause": "Athena validation returned 0 total rows across products, orders and order_items."
      },
      "ResultPath": "$.error",
      "Next": "JobFailed"
    },

    "PipelineSuccess": { "Type": "Succeed" },

    "JobFailed": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Comment": "$.error is an object ({Error, Cause}); States.Format only interpolates strings, numbers and booleans, so it must be serialised first or the publish itself raises States.Runtime and no alert is ever delivered.",
      "Parameters": {
        "TopicArn": "${sns_topic_arn}",
        "Message.$": "States.Format('Lakehouse pipeline FAILED.\n\nExecution: {}\nError: {}', $$.Execution.Name, States.JsonToString($.error))",
        "Subject": "Lakehouse Alert: Pipeline Failure"
      },
      "ResultPath": "$.snsResult",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "FailState", "ResultPath": "$.snsError" }],
      "Next": "FailState"
    },

    "FailState": {
      "Type": "Fail",
      "Error": "PipelineFailed",
      "Cause": "One or more ETL steps failed — check CloudWatch logs and the SNS alert."
    }
  }
}
