resource "google_pubsub_topic" "run_cloud_fn_topic" {
  name = "run-cloud-fun-topic"
}

resource "google_cloud_scheduler_job" "job" {
  name        = "run-cloud-fun-job"
  description = "test job"
  schedule    = "0 0 1 * *"

  pubsub_target {
    # topic.id is the topic's full resource name.
    topic_name = google_pubsub_topic.run_cloud_fn_topic.id
    data       = base64encode("run")
  }
}
