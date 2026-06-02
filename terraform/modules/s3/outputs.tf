output "bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.lakehouse.arn
}
