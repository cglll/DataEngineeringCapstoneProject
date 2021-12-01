resource "random_id" "server" {
  keepers = {
    first = "${timestamp()}"
  } 
  byte_length = 2
}

resource "google_storage_bucket" "your_bucket"{
    name=replace(format("%s/%s",var.bucket,random_id.server.hex),"/","_")
    force_destroy = true
    location=var.location
    project=var.project_id
    storage_class = "STANDARD"
}

resource "google_storage_bucket_iam_member" "bucket_creator"{
    bucket="${google_storage_bucket.your_bucket.name}"
    role="roles/storage.objectAdmin"
    member="serviceAccount:${var.service_account}"
}