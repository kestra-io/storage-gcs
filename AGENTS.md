# Kestra GCS Storage

## What

- Implements the storage backend under `io.kestra.storage.gcs`.
- Includes classes such as `GcsClientFactory`, `GcsConfig`, `GcsFileAttributes`, `GcsStorage`.

## Why

- This repository implements a Kestra storage backend for storage plugin for Google Cloud Storage (GCS).
- It stores namespace files and internal execution artifacts outside local disk.

## How

### Architecture

Single-module plugin.

### Project Structure

```
storage-gcs/
├── src/main/java/io/kestra/storage/gcs/
├── src/test/java/io/kestra/storage/gcs/
├── build.gradle
└── README.md
```

## Local rules

- Keep the scope on Kestra internal storage behavior, not workflow task semantics.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
