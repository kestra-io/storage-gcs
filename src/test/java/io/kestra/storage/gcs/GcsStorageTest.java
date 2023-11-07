package io.kestra.storage.gcs;

import com.google.common.io.CharStreams;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import io.kestra.core.storages.StorageInterface;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
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

        get(tenantId, prefix);
    }

    @Test
    void getNoTenant() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = null;

        get(tenantId, prefix);
    }

    private void get(String tenantId, String prefix) throws Exception {
        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        this.putFile(tenantId, resource, "/" + prefix + "/storage/get.yml");
        this.putFile(tenantId, resource, "/" + prefix + "/storage/level2/2.yml");

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
    void getNoTraversal() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        this.putFile(tenantId, resource, "/" + prefix + "/storage/get.yml");
        this.putFile(tenantId, resource, "/" + prefix + "/storage/level2/2.yml");
        // Assert that '..' in path cannot be used as gcs do not use directory listing and traversal.
        assertThrows(FileNotFoundException.class, () -> {storageInterface.get(tenantId, new URI("kestra:///" + prefix + "/storage/level2/../get.yml")); });
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

        deleteByPrefix(prefix, tenantId);
    }

    @Test
    void deleteByPrefixNoTenant() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        deleteByPrefix(prefix, tenantId);
    }

    private void deleteByPrefix(String prefix, String tenantId) throws Exception {
        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");

        List<String> path = Arrays.asList(
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/1.yml",
            "/" + prefix + "/storage/level1/level2/1.yml"
        );

        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        List<URI> deleted = storageInterface.deleteByPrefix(tenantId, new URI("/" + prefix + "/storage/"));

        List<String> res = Arrays.asList(
            "/" + prefix + "/storage/",
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/",
            "/" + prefix + "/storage/level1/1.yml",
            "/" + prefix + "/storage/level1/level2/",
            "/" + prefix + "/storage/level1/level2/1.yml"
        );

        assertThat(deleted, containsInAnyOrder(res.stream().map(s -> URI.create("kestra://" + s)).toArray()));

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

    @Test
    void list() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();
        URL resource = GcsFileAttributes.class.getClassLoader().getResource("application.yml");

        List<String> path = Arrays.asList(
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/1.yml",
            "/" + prefix + "/storage/level1/level2/1.yml",
            "/" + prefix + "/storage/another/1.yml"
        );
        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        List<FileAttributes> list = storageInterface.list(tenantId, new URI("/" + prefix + "/storage"));

        assertThat(list.stream().map(FileAttributes::getFileName).toList(), containsInAnyOrder("root.yml", "level1", "another"));

        // Assert that null modification & creation date don't break getters (and so the serialization) since they can be null for directory
        Assertions.assertDoesNotThrow(() -> {
            list.forEach(fileAttr -> {
                fileAttr.getLastModifiedTime();
                fileAttr.getCreationTime();
            });
        });
        // Assert that '..' in path cannot be used as gcs do not use directory listing and traversal.
        assertThrows(FileNotFoundException.class, () -> {storageInterface.get(tenantId, new URI("/" + prefix + "/storage/level2/..")); });

    }

    @Test
    void getAttributes() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();
        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource).getFile())));

        List<String> path = Arrays.asList(
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/1.yml"
        );
        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        FileAttributes attr = storageInterface.getAttributes(tenantId, new URI("/" + prefix + "/storage/root.yml"));
        assertThat(attr.getFileName(), is("root.yml"));
        assertThat(attr.getType(), is(FileAttributes.FileType.File));
        assertThat(attr.getSize(), is((long) content.length()));
        assertThat(attr.getLastModifiedTime(), notNullValue());

        attr = storageInterface.getAttributes(tenantId, new URI("/" + prefix + "/storage/level1"));
        assertThat(attr.getFileName(), is("level1"));
        assertThat(attr.getType(), is(FileAttributes.FileType.Directory));
        assertThat(attr.getSize(), is(0L));
        assertThat(attr.getLastModifiedTime(), notNullValue());
    }

    @Test
    void delete() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();
        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");

        List<String> path = Arrays.asList(
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/1.yml",
            "/" + prefix + "/storage/level1/level2/1.yml",
            "/" + prefix + "/storage/another/1.yml"
        );
        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        boolean deleted = storageInterface.delete(tenantId, new URI("/" + prefix + "/storage/level1"));
        assertThat(deleted, is(true));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/root.yml")), is(true));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/another/1.yml")), is(true));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/level1")), is(false));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/level1/1.yml")), is(false));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/level1/level2/1.yml")), is(false));
        deleted = storageInterface.delete(tenantId, new URI("/" + prefix + "/storage/root.yml"));
        assertThat(deleted, is(true));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/root.yml")), is(false));
    }

    @Test
    void createDirectory() throws URISyntaxException, IOException {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();

        storageInterface.createDirectory(tenantId, new URI("/" + prefix + "/storage/level1"));
        FileAttributes attr = storageInterface.getAttributes(tenantId, new URI("/" + prefix + "/storage/level1"));
        assertThat(attr.getFileName(), is("level1"));
        assertThat(attr.getType(), is(FileAttributes.FileType.Directory));
        assertThat(attr.getSize(), is(0L));
        assertThat(attr.getLastModifiedTime(), notNullValue());
    }

    @Test
    void move() throws Exception {
        String prefix = IdUtils.create();
        String tenantId = IdUtils.create();
        URL resource = GcsStorageTest.class.getClassLoader().getResource("application.yml");

        List<String> path = Arrays.asList(
            "/" + prefix + "/storage/root.yml",
            "/" + prefix + "/storage/level1/1.yml",
            "/" + prefix + "/storage/level1/level2/2.yml",
            "/" + prefix + "/storage/another/1.yml"
        );
        path.forEach(throwConsumer(s -> this.putFile(tenantId, resource, s)));

        storageInterface.move(tenantId, new URI("/" + prefix + "/storage/level1"), new URI("/" + prefix + "/storage/moved"));

        List<FileAttributes> list = storageInterface.list(tenantId, new URI("/" + prefix + "/storage/moved"));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/level1")), is(false));
        assertThat(list.stream().map(FileAttributes::getFileName).toList(), containsInAnyOrder("level2", "1.yml"));

        list = storageInterface.list(tenantId, new URI("/" + prefix + "/storage/moved/level2"));
        assertThat(list.stream().map(FileAttributes::getFileName).toList(), containsInAnyOrder("2.yml"));

        storageInterface.move(tenantId, new URI("/" + prefix + "/storage/root.yml"), new URI("/" + prefix + "/storage/root-moved.yml"));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/root.yml")), is(false));
        assertThat(storageInterface.exists(tenantId, new URI("/" + prefix + "/storage/root-moved.yml")), is(true));
    }
}
