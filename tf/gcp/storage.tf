resource "google_storage_bucket" "function_bucket" {
    name     = "${var.project_id}-function"
    location = var.region
}

resource "google_storage_bucket" "raw_bucket" {
    name     = "${var.project_id}-raw"
    location = var.region
}

resource "google_storage_bucket" "curated_bucket" {
    name     = "${var.project_id}-curated"
    location = var.region
}