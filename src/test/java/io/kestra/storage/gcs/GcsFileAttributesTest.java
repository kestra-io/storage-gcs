package io.kestra.storage.gcs;

import org.junit.jupiter.api.Test;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;

import io.kestra.core.storages.FileAttributes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class GcsFileAttributesTest {

    @Test
    void shouldBeAFileWhenTheObjectWasUploadedWithoutContentType() {
        var attributes = GcsFileAttributes.builder()
            .fileName("presigned.txt")
            .blobInfo(BlobInfo.newBuilder(BlobId.of("bucket", "presigned.txt")).build())
            .build();

        assertThat(attributes.getType(), is(FileAttributes.FileType.File));
    }
}
