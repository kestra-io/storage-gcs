package io.kestra.storage.gcs;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
class GcsStoragePurgeTest {

    private static final String TENANT = "purge-test-tenant";
    private static final String NAMESPACE = "io.kestra.test";
    private static final byte[] CONTENT = "test-content".getBytes();

    @Inject
    StorageInterface storageInterface;

    private String testPrefix;
    private final List<URI> createdUris = new ArrayList<>();

    @BeforeEach
    void setUp() {
        testPrefix = "/purge/" + UUID.randomUUID();
        createdUris.clear();
    }

    @AfterEach
    void tearDown() throws IOException {
        // Clean up any objects that were not deleted by the test
        for (var uri : createdUris) {
            try {
                storageInterface.delete(TENANT, NAMESPACE, uri);
            } catch (Exception ignored) {
            }
        }
    }

    private URI putFile(String path) throws IOException {
        var uri = URI.create("kestra://" + testPrefix + "/" + path);
        storageInterface.put(TENANT, NAMESPACE, uri, new StorageObject(null, new ByteArrayInputStream(CONTENT)));
        createdUris.add(uri);
        return uri;
    }

    @Test
    void inWindowObjectsAreDeletedAndUrisReturned() throws IOException, InterruptedException {
        var before = Instant.now().minusSeconds(2);
        var uri1 = putFile("file1.txt");
        var uri2 = putFile("file2.txt");
        var after = Instant.now().plusSeconds(2);

        var deleted = storageInterface.purgeByLastModified(TENANT, NAMESPACE, URI.create("kestra://" + testPrefix), before, after, false);

        assertThat(deleted, hasSize(2));
        assertThat(deleted, containsInAnyOrder(uri1, uri2));
        // Verify files are actually deleted
        assertThat(storageInterface.exists(TENANT, NAMESPACE, uri1), is(false));
        assertThat(storageInterface.exists(TENANT, NAMESPACE, uri2), is(false));
        createdUris.removeAll(deleted);
    }

    @Test
    void outOfWindowObjectsArePreserved() throws IOException {
        var uri = putFile("preserved.txt");
        var future = Instant.now().plusSeconds(3600);

        var deleted = storageInterface.purgeByLastModified(
            TENANT, NAMESPACE,
            URI.create("kestra://" + testPrefix),
            future, null, false
        );

        assertThat(deleted, is(empty()));
        assertThat(storageInterface.exists(TENANT, NAMESPACE, uri), is(true));
    }

    @Test
    void dryRunReturnMatchedUrisWithoutDeleting() throws IOException {
        var before = Instant.now().minusSeconds(2);
        var uri = putFile("dry-run.txt");
        var after = Instant.now().plusSeconds(2);

        var matched = storageInterface.purgeByLastModified(TENANT, NAMESPACE, URI.create("kestra://" + testPrefix), before, after, true);

        assertThat(matched, hasSize(1));
        assertThat(matched, contains(uri));
        // File must still exist after dry run
        assertThat(storageInterface.exists(TENANT, NAMESPACE, uri), is(true));
    }

    @Test
    void missingPrefixReturnsEmptyList() throws IOException {
        var nonExistent = URI.create("kestra:///purge/non-existent-" + UUID.randomUUID());
        var result = storageInterface.purgeByLastModified(
            TENANT, NAMESPACE, nonExistent,
            Instant.now().minusSeconds(60), Instant.now().plusSeconds(60),
            false
        );

        assertThat(result, is(empty()));
    }

    @Test
    void nullStartDateMeansUnboundedLowerBound() throws IOException {
        var uri = putFile("unbounded-lower.txt");
        var after = Instant.now().plusSeconds(2);

        var deleted = storageInterface.purgeByLastModified(TENANT, NAMESPACE, URI.create("kestra://" + testPrefix), null, after, false);

        assertThat(deleted, hasSize(1));
        assertThat(deleted, contains(uri));
        createdUris.removeAll(deleted);
    }

    @Test
    void nullEndDateMeansUnboundedUpperBound() throws IOException {
        var uri = putFile("unbounded-upper.txt");
        var before = Instant.now().minusSeconds(2);

        var deleted = storageInterface.purgeByLastModified(TENANT, NAMESPACE, URI.create("kestra://" + testPrefix), before, null, false);

        assertThat(deleted, hasSize(1));
        assertThat(deleted, contains(uri));
        createdUris.removeAll(deleted);
    }

    @Test
    void nullBothBoundsMatchesEverythingUnderPrefix() throws IOException {
        var uri1 = putFile("all1.txt");
        var uri2 = putFile("all2.txt");

        var deleted = storageInterface.purgeByLastModified(TENANT, NAMESPACE, URI.create("kestra://" + testPrefix), null, null, false);

        assertThat(deleted, hasSize(2));
        assertThat(deleted, containsInAnyOrder(uri1, uri2));
        createdUris.removeAll(deleted);
    }

    @Test
    void moreThan100ObjectsArePurgedViaBatchChunking() throws IOException {
        var before = Instant.now().minusSeconds(2);
        int count = 110;
        var uris = new ArrayList<URI>();
        for (int i = 0; i < count; i++) {
            uris.add(putFile("batch/file-" + i + ".txt"));
        }
        var after = Instant.now().plusSeconds(10);

        var deleted = storageInterface.purgeByLastModified(TENANT, NAMESPACE, URI.create("kestra://" + testPrefix), before, after, false);

        assertThat(deleted, hasSize(count));
        for (var uri : uris) {
            assertThat(storageInterface.exists(TENANT, NAMESPACE, uri), is(false));
        }
        createdUris.removeAll(deleted);
    }
}
