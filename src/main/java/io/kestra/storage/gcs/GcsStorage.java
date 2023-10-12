package io.kestra.storage.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import io.micronaut.core.annotation.Introspected;
import io.kestra.core.storages.StorageInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@GcsStorageEnabled
@Introspected
public class GcsStorage implements StorageInterface {
    @Inject
    GcsClientFactory factory;

    @Inject
    GcsConfig config;

    private Storage client() {
        return factory.of(config);
    }

    private BlobId blob(String tenantId, URI uri) {
        return BlobId.of(this.config.getBucket(), tenantId + uri.getPath());
    }

    @Override
    public InputStream get(String tenantId, URI uri) throws IOException {
        try {
            Blob blob = this.client().get(this.blob(tenantId, URI.create(uri.getPath())));

            if (blob == null || !blob.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            ReadableByteChannel reader = blob.reader();
            return Channels.newInputStream(reader);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean exists(String tenantId, URI uri) {
        try {
            Blob blob = this.client().get(this.blob(tenantId, URI.create(uri.getPath())));
            return blob != null && blob.exists();
        } catch (StorageException e) {
            return false;
        }
    }

    @Override
    public Long size(String tenantId,URI uri) throws IOException {
        try {
            Blob blob = this.client().get(this.blob(tenantId, URI.create(uri.getPath())));

            if (blob == null || !blob.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blob.getSize();
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Long lastModifiedTime(String tenantId,URI uri) throws IOException {
        try {
            Blob blob = this.client().get(this.blob(tenantId, URI.create(uri.getPath())));

            if (blob == null || !blob.exists()) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            return blob.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli();
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public URI put(String tenantId, URI uri, InputStream data) throws IOException {
        try {
            BlobInfo blobInfo = BlobInfo
                .newBuilder(this.blob(tenantId, uri))
                .build();

            try (WriteChannel writer = this.client().writer(blobInfo)) {
                byte[] buffer = new byte[10_240];

                int limit;
                while ((limit = data.read(buffer)) >= 0) {
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }

            data.close();

            return URI.create("kestra://" + uri.getPath());
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    public boolean delete(String tenantId, URI uri) throws IOException {
        try {
            return this.client().delete(this.blob(tenantId, uri));
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        try {
            StorageBatch batch = this.client().batch();
            Map<URI, StorageBatchResult<Boolean>> results = new HashMap<>();

            Page<Blob> blobs = this.client()
                .list(this.config.getBucket(),
                    Storage.BlobListOption.prefix(tenantId + storagePrefix.getPath())
                );

            for (Blob blob : blobs.iterateAll()) {
                results.put(URI.create("kestra:///" + blob.getBlobId().getName().replace(tenantId + "/", "")), batch.delete(blob.getBlobId()));
            }

            if (results.size() == 0) {
                return List.of();
            }

            batch.submit();

            if (!results.entrySet().stream().allMatch(r -> r.getValue() != null && r.getValue().get())) {
                throw new IOException("Unable to delete all files, failed on [" +
                    results
                        .entrySet()
                        .stream()
                        .filter(r -> r.getValue() == null || !r.getValue().get())
                        .map(r -> r.getKey().getPath())
                        .collect(Collectors.joining(", ")) +
                    "]");
            }

            return new ArrayList<>(results.keySet());
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
