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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

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

    private BlobId blob(URI uri) {
        return BlobId.of(this.config.getBucket(), uri.getPath().substring(1));
    }

    @Override
    public InputStream get(URI uri) throws FileNotFoundException  {
        Blob blob = this.client().get(this.blob(URI.create(uri.getPath())));

        if (blob == null || !blob.exists()) {
            throw new FileNotFoundException(uri.toString() + " (File not found)");
        }

        ReadableByteChannel reader = blob.reader();
        return Channels.newInputStream(reader);
    }

    @Override
    public Long size(URI uri) throws IOException {
        return this.client().get(this.blob(URI.create(uri.getPath()))).getSize();
    }

    @Override
    public URI put(URI uri, InputStream data) throws IOException {
        BlobInfo blobInfo = BlobInfo
            .newBuilder(this.blob(uri))
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
    }

    public boolean delete(URI uri) {
        return this.client().delete(this.blob(uri));
    }

    @Override
    public List<URI> deleteByPrefix(URI storagePrefix) throws IOException {
        StorageBatch batch = this.client().batch();
        Map<URI, StorageBatchResult<Boolean>> results = new HashMap<>();

        Page<Blob> blobs = this.client()
            .list(this.config.getBucket(),
                Storage.BlobListOption.prefix(storagePrefix.getPath().substring(1))
            );

        for (Blob blob : blobs.iterateAll()) {
            results.put(URI.create("kestra:///" + blob.getBlobId().getName()), batch.delete(blob.getBlobId()));
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
    }
}
