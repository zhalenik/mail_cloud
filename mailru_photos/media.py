"""Limits shared by discovery, queue selection, and upload policy."""

# Cloud Mail Android RemoteParams: unlimited_upload_max_file_size default.
# Keep the exact binary limit so files that still qualify are never billed just
# because a rounded decimal interpretation of "2 GB" was used by the client.
UNLIMITED_MAX_BYTES = 2 ** 31
