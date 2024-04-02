# Generates an archive of the source code compressed as a .zip file.
data "archive_file" "source" {
    type        = "zip"
    source_dir  = "./src"
    output_path = "./tmp/function.zip"
}

# Add source code zip to the Cloud Function's bucket
resource "google_storage_bucket_object" "zip" {
    source       = data.archive_file.source.output_path
    content_type = "application/zip"

    # Append to the MD5 checksum of the files's content
    # to force the zip to be updated as soon as a change occurs
    name         = "src-${data.archive_file.source.output_md5}.zip"
    bucket       = google_storage_bucket.function_bucket.name

    # Dependencies are automatically inferred so these lines can be deleted
    depends_on   = [
        google_storage_bucket.function_bucket,  # declared in `storage.tf`
        data.archive_file.source
    ]
}

# Create the Cloud function triggered by a `Finalize` event on the bucket
# resource "google_cloudfunctions_function" "function" {
#     name                  = "batch-load-crime-data"
#     runtime               = "python310"  # of course changeable

#     # Get the source code of the cloud function as a Zip compression
#     source_archive_bucket = google_storage_bucket.function_bucket.name
#     source_archive_object = google_storage_bucket_object.zip.name

#     # Must match the function name in the cloud function `main.py` source code
#     entry_point           = "run_batch_load"
#     timeout = 540

#     #
#     event_trigger {
#         #pubsub
#         event_type = "google.storage.object.finalize"
#         resource   = "${var.project_id}-input"
#     }

#     # Dependencies are automatically inferred so these lines can be deleted
#     depends_on            = [
#         google_storage_bucket.function_bucket,  # declared in `storage.tf`
#         google_storage_bucket_object.zip
#     ]
# }







resource "google_cloudfunctions2_function" "batch-load-crime-data" {
  provider = google
  project = var.project_id
  name = "batch-load-crime-data-fn"
  location = "us-central1"
  description = "Batch load cloud function"

  build_config {
    runtime = "python310"
    entry_point = "run_batch_load"  # Set the entry point

    # environment_variables = var.env_vars
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.zip.name
      }
    }
  }

  service_config {
    max_instance_count  = 3
    min_instance_count = 1
    available_memory    = "4Gi"
    timeout_seconds     = 540
    available_cpu = "4"
    # ingress_settings = "ALLOW_INTERNAL_ONLY"
    # all_traffic_on_latest_revision = true
    # service_account_email = google_service_account.account.email
  }

  event_trigger {
    trigger_region = "us-central1"
    event_type = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic = google_pubsub_topic.run_cloud_fn_topic.id
    # retry_policy = "RETRY_POLICY_RETRY"
    # service_account_email = google_service_account.account.email
  }
}