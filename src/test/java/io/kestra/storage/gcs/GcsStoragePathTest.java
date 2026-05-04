package io.kestra.storage.gcs;

import java.net.URI;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class GcsStoragePathTest {

    @Test
    void getPathWithPrefix() {
        GcsStorage storage = GcsStorage.builder()
            .bucket("test-bucket")
            .path("instance-a")
            .build();

        String result = storage.getPath("main", URI.create("/folder1/folder2"));
        assertThat(result, is("instance-a/main/folder1/folder2"));
    }

    @Test
    void getPathWithPrefixTrailingSlash() {
        GcsStorage storage = GcsStorage.builder()
            .bucket("test-bucket")
            .path("instance-a/")
            .build();

        String result = storage.getPath("main", URI.create("/folder1/folder2"));
        assertThat(result, is("instance-a/main/folder1/folder2"));
    }

    @Test
    void getPathWithoutPrefix() {
        GcsStorage storage = GcsStorage.builder()
            .bucket("test-bucket")
            .build();

        String result = storage.getPath("main", URI.create("/folder1/folder2"));
        assertThat(result, is("main/folder1/folder2"));
    }

    @Test
    void getPathInstanceResourceNotAffectedByPrefix() {
        GcsStorage storage = GcsStorage.builder()
            .bucket("test-bucket")
            .path("instance-a")
            .build();

        String result = storage.getPath(URI.create("/folder1/folder2"));
        assertThat(result, is("folder1/folder2"));
    }
}
