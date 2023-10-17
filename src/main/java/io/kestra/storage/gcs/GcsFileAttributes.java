package io.kestra.storage.gcs;

import com.google.cloud.storage.BlobInfo;
import io.kestra.core.storages.FileAttributes;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class GcsFileAttributes implements FileAttributes {

    String fileName;
    BlobInfo blobInfo;
    boolean isDirectory;

    @Override
    public long getLastModifiedTime() {
        return blobInfo.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();
    }

    @Override
    public long getCreationTime() {
        return blobInfo.getCreateTimeOffsetDateTime().toInstant().toEpochMilli();
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
}
