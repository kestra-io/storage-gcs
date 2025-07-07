package io.kestra.storage.gcs;

import com.google.cloud.storage.BlobInfo;
import io.kestra.core.storages.FileAttributes;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;

@Value
@Builder
public class GcsFileAttributes implements FileAttributes {

    String fileName;
    BlobInfo blobInfo;
    boolean isDirectory;

    @Override
    public long getLastModifiedTime() {
        return Optional.ofNullable(blobInfo.getUpdateTimeOffsetDateTime())
            .map(OffsetDateTime::toInstant)
            .map(Instant::toEpochMilli)
            .orElse(0L);
    }

    @Override
    public long getCreationTime() {
        return Optional.ofNullable(blobInfo.getCreateTimeOffsetDateTime())
            .map(OffsetDateTime::toInstant)
            .map(Instant::toEpochMilli)
            .orElse(0L);
    }

    @Override
    public FileType getType() {
        if (isDirectory || fileName.endsWith("/") || blobInfo.getContentType().equals("application/x-directory")) {
            return FileType.Directory;
        }
        return FileType.File;
    }

    @Override
    public long getSize() {
        return blobInfo.getSize();
    }

    @Override
    public Map<String, String> getMetadata() {
        return blobInfo.getMetadata();
    }
}