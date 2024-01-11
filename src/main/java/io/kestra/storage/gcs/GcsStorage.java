package io.kestra.storage.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import io.kestra.core.storages.FileAttributes;
import io.micronaut.core.annotation.Introspected;
import io.kestra.core.storages.StorageInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@Singleton
@GcsStorageEnabled
@Introspected
public class GcsStorage implements StorageInterface {
    @Inject
    Storage storage;

    @Inject
    GcsConfig config;


    private BlobId blob(String tenantId, URI uri) {
        String path = getPath(tenantId, uri);
        return blob(path);
    }

    @NotNull
    private BlobId blob(String path) {
        return BlobId.of(this.config.getBucket(), path);
    }

    @NotNull
    private String getPath(String tenantId, URI uri) {
        if (uri == null) {
            uri = URI.create("/");
        }

        parentTraversalGuard(uri);
        String path = uri.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        if (tenantId == null) {
            return path;
        }
        return "/" + tenantId + path;
    }

    // Traversal does not work with gcs but it just return empty objects so throwing is more explicit
    private void parentTraversalGuard(URI uri) {
        if (uri.toString().contains("..")) {
            throw new IllegalArgumentException("File should be accessed with their full path and not using relative '..' path.");
        }
    }

    @Override
    public InputStream get(String tenantId, URI uri) throws IOException {
        try {
            Blob blob = this.storage.get(this.blob(tenantId, URI.create(uri.getPath())));

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
    public List<URI> allByPrefix(String tenantId, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        return blobsForPrefix(path, true, includeDirectories)
            .map(BlobInfo::getName)
            .map(blobPath -> URI.create("kestra://" + prefix.getPath() + blobPath.substring(path.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        String prefix = (path.endsWith("/")) ? path : path + "/";

        List<FileAttributes> list = blobsForPrefix(prefix, false, true)
            .map(throwFunction(this::getGcsFileAttributes))
            .toList();
        if(list.isEmpty()) {
            // this will throw FileNotFound if there is no directory
            this.getAttributes(tenantId, uri);
        }
        return list;
    }

    private Stream<Blob> blobsForPrefix(String prefix, boolean recursive, boolean includeDirectories) {
        Storage.BlobListOption[] blobListOptions = Stream.concat(
            Stream.of(Storage.BlobListOption.prefix(prefix)),
            recursive ? Stream.empty() : Stream.of(Storage.BlobListOption.currentDirectory())
        ).toArray(Storage.BlobListOption[]::new);
        Page<Blob> blobs = this.storage.list(config.bucket, blobListOptions);
        return blobs.streamAll()
            .filter(blob -> {
                String key = blob.getName().substring(prefix.length());
                // Remove recursive result and requested dir
                return !key.isEmpty()
                    && !Objects.equals(key, prefix)
                    && !key.equals("/")
                    && (recursive || Path.of(key).getParent() == null)
                    && (includeDirectories || !key.endsWith("/"));
            });
    }

    @Override
    public boolean exists(String tenantId, URI uri) {
        try {
            Blob blob = this.storage.get(this.blob(tenantId, URI.create(uri.getPath())));
            return blob != null && blob.exists();
        } catch (StorageException e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!exists(tenantId, uri)) {
            path = path + "/";
        }
        Blob blob = this.storage.get(this.blob(path));
        if (blob == null) {
            throw new FileNotFoundException("%s not found.".formatted(uri));
        }
        return getGcsFileAttributes(blob);
    }

    private FileAttributes getGcsFileAttributes(Blob blob) {
        GcsFileAttributes.GcsFileAttributesBuilder builder = GcsFileAttributes.builder()
            .fileName(new File(blob.getName()).getName())
            .blobInfo(blob.asBlobInfo());
        if (blob.getName().endsWith("/")) {
            builder.isDirectory(true);
        }
        return builder.build();
    }

    @Override
    public URI put(String tenantId, URI uri, InputStream data) throws IOException {
        try {
            String path = getPath(tenantId, uri);
            mkdirs(path);

            BlobInfo blobInfo = BlobInfo
                .newBuilder(this.blob(tenantId, uri))
                .build();

            try (WriteChannel writer = this.storage.writer(blobInfo)) {
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

    private void mkdirs(String path) {
        path = path.replaceAll("^/*", "");
        String[] directories = path.split("/");
        StringBuilder aggregatedPath = new StringBuilder("/");
        // perform 1 put request per parent directory in the path
        for (int i = 0; i <= directories.length - (path.endsWith("/") ? 1 : 2); i++) {
            aggregatedPath.append(directories[i]).append("/");
            BlobInfo blobInfo = BlobInfo
                .newBuilder(this.blob(aggregatedPath.toString()))
                .build();
            this.storage.create(blobInfo);
        }
    }

    @Override
    public boolean delete(String tenantId, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, uri);
        } catch (FileNotFoundException e) {
            return false;
        }


        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            return !this.deleteByPrefix(
                tenantId,
                uri.getPath().endsWith("/") ? uri : URI.create(uri.getPath() + "/")
            ).isEmpty();
        }

        return this.storage.delete(this.blob(tenantId, uri));
    }

    @Override
    public URI createDirectory(String tenantId, URI uri) {
        String path = getPath(tenantId, uri);
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        mkdirs(path);
        return createUri(uri.getPath());
    }

    @Override
    public URI move(String tenantId, URI from, URI to) throws IOException {
        String path = getPath(tenantId, from);
        StorageBatch batch = this.storage.batch();

        if (getAttributes(tenantId, from).getType() == FileAttributes.FileType.File) {
            // move just a file
            BlobId source = blob(path);
            BlobId target = blob(tenantId, to);
            moveFile(source, target, batch);
        } else {
            // move directories
            String prefix = (!path.endsWith("/")) ? path + "/" : path;

            Page<Blob> list = this.storage.list(config.bucket, Storage.BlobListOption.prefix(prefix));
            list.streamAll().forEach(blob -> {
                BlobId target = blob(getPath(tenantId, to) + "/" + blob.getName().substring(prefix.length()));
                moveFile(blob.getBlobId(), target, batch);
            });
        }
        batch.submit();
        return createUri(to.getPath());
    }

    private void moveFile(BlobId source, BlobId target, StorageBatch batch) {
        this.storage.copy(Storage.CopyRequest.newBuilder().setSource(source).setTarget(target).build());
        batch.delete(source);
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, URI storagePrefix) throws IOException {
        try {
            StorageBatch batch = this.storage.batch();
            Map<URI, StorageBatchResult<Boolean>> results = new HashMap<>();

            String prefix = getPath(tenantId, storagePrefix);

            Page<Blob> blobs = this.storage
                .list(this.config.getBucket(),
                    Storage.BlobListOption.prefix(prefix)
                );

            for (Blob blob : blobs.iterateAll()) {
                results.put(URI.create("kestra://" + blob.getBlobId().getName().replace(tenantId + "/", "").replaceAll("/$", "")), batch.delete(blob.getBlobId()));
            }

            if (results.isEmpty()) {
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
    private static URI createUri(String key) {
        return URI.create("kestra://%s".formatted(key));
    }
}
