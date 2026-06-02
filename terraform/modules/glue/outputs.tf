output "products_job_name" {
  value = aws_glue_job.products.name
}

output "orders_job_name" {
  value = aws_glue_job.orders.name
}

output "order_items_job_name" {
  value = aws_glue_job.order_items.name
}

output "generate_manifests_job_name" {
  value = aws_glue_job.generate_manifests.name
}

output "crawler_name" {
  value = aws_glue_crawler.lakehouse.name
}

output "database_name" {
  value = aws_glue_catalog_database.lakehouse.name
}
