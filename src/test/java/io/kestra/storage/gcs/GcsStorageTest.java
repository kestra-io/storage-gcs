package io.kestra.storage.gcs;

import java.io.ByteArrayInputStream;
import java.net.URI;

import org.junit.jupiter.api.Test;

import io.kestra.core.storage.StorageTestSuite;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsStorageTest extends StorageTestSuite {

    /**
     * Regression test for Bug 2: directory markers removed by deleteByPrefix must not be
     * served from the stale in-memory cache on the next put, causing getAttributes to report
     * the directory as absent even though objects exist underneath.
     */
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
