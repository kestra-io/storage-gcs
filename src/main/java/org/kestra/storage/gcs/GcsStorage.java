package org.kestra.storage.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.micronaut.core.annotation.Introspected;
import org.kestra.core.storages.StorageInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
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
}
