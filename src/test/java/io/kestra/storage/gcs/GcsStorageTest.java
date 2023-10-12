package io.kestra.storage.gcs;

import com.google.common.io.CharStreams;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;
import io.kestra.core.storages.StorageInterface;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
class GcsStorageTest {
    @Inject
    StorageInterface storageInterface;

    private URI putFile(String tenantID, URL resource, String path) throws Exception {
        return storageInterface.put(
            tenantID,
            new URI(path),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );
    }

    @Test
    void get() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        this.putFile(tenantId, resource, "/" + prefix + "/storage/get.yml");

        URI item = new URI("/" + prefix + "/storage/get.yml");
        InputStream get = storageInterface.get(tenantId,item);
        assertThat(CharStreams.toString(new InputStreamReader(get)), is(content));
        assertTrue(storageInterface.exists(tenantId,item));
        assertThat(storageInterface.size(tenantId, item), is((long) content.length()));
        assertThat(storageInterface.lastModifiedTime(tenantId, item), notNullValue());

        InputStream getScheme = storageInterface.get(tenantId, new URI("kestra:///" + prefix + "/storage/get.yml"));
        assertThat(CharStreams.toString(new InputStreamReader(getScheme)), is(content));
    }

    @Test
    void missing() {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(tenantId, new URI("/" + prefix + "/storage/missing.yml"));
        });
    }

    @Test
    void put() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");
        URI put = this.putFile(tenantId, resource, "/" + prefix + "/storage/put.yml");
        InputStream get = storageInterface.get(tenantId, new URI("/" + prefix + "/storage/put.yml"));

        assertThat(put.toString(), is(new URI("kestra:///" + prefix + "/storage/put.yml").toString()));
        assertThat(
            CharStreams.toString(new InputStreamReader(get)),
            is(CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile()))))
        );

        assertThat(storageInterface.size(tenantId, new URI("/" + prefix + "/storage/put.yml")), is(76L));

        assertThrows(FileNotFoundException.class, () -> {
            assertThat(storageInterface.size(tenantId, new URI("/" + prefix + "/storage/muissing.yml")), is(76L));
        });

        boolean delete = storageInterface.delete(tenantId, put);
        assertThat(delete, is(true));

        delete = storageInterface.delete(tenantId, put);
        assertThat(delete, is(false));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(tenantId, new URI("/" + prefix + "/storage/put.yml"));
        });
    }

    @Test
    void deleteByPrefix() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");

        List<String> path = Arrays.asList(
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/1.yml",
            "/" + prefix + "/storage/level1/level2/1.yml"
        );

        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        List<URI> deleted = storageInterface.deleteByPrefix(tenantId, new URI("/" + prefix + "/storage/"));

        assertThat(deleted, containsInAnyOrder(path.stream().map(s -> URI.create("kestra://" + s)).toArray()));

        assertThrows(FileNotFoundException.class, () -> {
            storageInterface.get(tenantId, new URI("/" + prefix + "/storage/"));
        });

        path
            .forEach(s -> {
                assertThrows(FileNotFoundException.class, () -> {
                    storageInterface.get(tenantId, new URI(s));
                });
            });
    }

    @Test
    void deleteByPrefixNoResult() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        List<URI> deleted = storageInterface.deleteByPrefix(tenantId, new URI("/" + prefix + "/storage/"));
        assertThat(deleted.size(), is(0));
    }
}
