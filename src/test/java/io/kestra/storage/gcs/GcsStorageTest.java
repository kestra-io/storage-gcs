package io.kestra.storage.gcs;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.PresignableStorage;
import io.kestra.core.storages.PresignedRequest;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsStorageTest extends StorageTestSuite {

    @Test
    void shouldReadAndWriteThroughPresignedRequestsWithoutCredentials() throws Exception {
        var tenantId = IdUtils.create();
        var directory = "/" + IdUtils.create() + "/a";
        var uri = new URI(directory + "/presigned.txt");
        var presignable = storageInterface.as(PresignableStorage.class).orElseThrow();

        try (HttpClient client = HttpClient.newHttpClient()) {
            var put = presignable.presign(tenantId, null, uri, PresignedRequest.Method.PUT, Map.of("expirationDate", "2030-01-01T00:00:00Z"), Duration.ofMinutes(5)).orElseThrow();
            var upload = HttpRequest.newBuilder(put.url()).PUT(HttpRequest.BodyPublishers.ofString("hello"));
            put.headers().forEach(upload::header);
            assertThat(client.send(upload.build(), HttpResponse.BodyHandlers.discarding()).statusCode(), is(200));

            var stored = storageInterface.getWithMetadata(tenantId, null, uri);
            try (var content = stored.inputStream()) {
                assertThat(new String(content.readAllBytes(), StandardCharsets.UTF_8), is("hello"));
            }
            assertThat(stored.metadata(), hasEntry("expirationDate", "2030-01-01T00:00:00Z"));
            assertThat(storageInterface.getAttributes(tenantId, null, new URI(directory)).getType(), is(FileAttributes.FileType.Directory));

            var get = presignable.presign(tenantId, null, uri, PresignedRequest.Method.GET, Map.of(), Duration.ofMinutes(5)).orElseThrow();
            var download = client.send(HttpRequest.newBuilder(get.url()).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(download.body(), is("hello"));
        }
    }

    @Test
    void directoryMarkerRecreatedAfterDeleteByPrefix() throws Exception {
        var tenantId = IdUtils.create();
        var prefix = IdUtils.create();
        var dirPath = "/" + prefix + "/a/b";
        var firstFile = dirPath + "/c.txt";
        var secondFile = dirPath + "/d.txt";
        var content = "hello".getBytes();

        // 1. put a file → mkdirs creates the "a/b/" marker
        storageInterface.put(tenantId, null, new URI(firstFile), new ByteArrayInputStream(content));

        // 2. delete everything under a/b/ → marker blob is removed from GCS
        storageInterface.deleteByPrefix(tenantId, null, new URI(dirPath + "/"));

        // 3. put another file under the same directory → mkdirs must recreate the marker
        storageInterface.put(tenantId, null, new URI(secondFile), new ByteArrayInputStream(content));

        // 4. getAttributes must not throw FileNotFoundException — the directory marker was recreated by mkdirs
        var attrs = storageInterface.getAttributes(tenantId, null, new URI(dirPath));
        assertThat(attrs.getType(), is(FileAttributes.FileType.Directory));
        // the newly-put file must be reachable
        assertTrue(storageInterface.exists(tenantId, null, new URI(secondFile)));
    }
}
