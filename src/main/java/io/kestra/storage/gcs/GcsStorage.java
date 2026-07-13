package io.kestra.storage.gcs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;

import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.storages.FileAttributes;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.storages.StorageObject;

import jakarta.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.jackson.Jacksonized;

import static io.kestra.core.utils.Rethrow.throwFunction;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Jacksonized
@Getter
@Plugin
@Plugin.Id("gcs")
public class GcsStorage implements StorageInterface, GcsConfig {
    private static final Logger log = LoggerFactory.getLogger(GcsStorage.class);

    // GCS batch requests are limited to 100 operations per submit call.
    private static final int BATCH_DELETE_LIMIT = 100;

    // The GCS JSON batch endpoint intermittently returns transient "server(s) are not
    // responding" errors that the client does not retry on its own. Re-submit a fresh
    // batch with a short backoff to absorb those before failing.
    private static final int BATCH_SUBMIT_MAX_ATTEMPTS = 3;
    private static final long BATCH_SUBMIT_RETRY_BACKOFF_MS = 500;

    private String bucket;

    private String path;

    @PluginProperty(secret = true)
    private String serviceAccount;

    private String projectId;

    @Getter(AccessLevel.PRIVATE)
    private Storage storage;

    /** {@inheritDoc} **/
    @Override
    public void init() {
        this.storage = GcsClientFactory.of(this);
    }

    /** {@inheritDoc} **/
    @Override
    public void close() {
        if (this.storage != null) {
            try {
                this.storage.close();
            } catch (Exception e) {
                log.warn("Failed to close GcsStorage", e);
            }
        }
    }

    @Override
    public String getPath(String tenantId, URI uri) {
        String basePath = StorageInterface.super.getPath(tenantId, uri);
        if (path == null) {
            return basePath;
        }
        return path + (path.endsWith("/") ? basePath : "/" + basePath);
    }

    private BlobId blob(String tenantId, URI uri) {
        String path = getPath(tenantId, uri);
        return blob(path);
    }

    private BlobId blob(URI uri) {
        return blob(getPath(uri));
    }

    private BlobId blob(String path) {
        return BlobId.of(bucket, path);
    }

    @Override
    public InputStream get(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        return getWithMetadata(tenantId, namespace, uri).inputStream();
    }

    @Override
    public InputStream getInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        return getFromBlobId(uri, blob(uri)).inputStream();
    }

    @Override
    public StorageObject getWithMetadata(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        BlobId blobId = this.blob(tenantId, URI.create(uri.getPath()));
        return getFromBlobId(uri, blobId);
    }

    private StorageObject getFromBlobId(URI uri, BlobId blobId) throws IOException {
        try {
            Blob blob = this.storage.get(blobId);

            if (blob == null) {
                throw new FileNotFoundException(uri + " (File not found)");
            }

            ReadableByteChannel reader = blob.reader();
            return new StorageObject(blob.getMetadata(), Channels.newInputStream(reader));
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public List<URI> allByPrefix(String tenantId, @Nullable String namespace, URI prefix, boolean includeDirectories) {
        String path = getPath(tenantId, prefix);
        return blobsForPrefix(path, true, includeDirectories)
            .map(BlobInfo::getName)
            .map(blobPath -> URI.create("kestra://" + prefix.getPath() + blobPath.substring(path.length())))
            .toList();
    }

    @Override
    public List<FileAttributes> list(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        String prefix = (path.endsWith("/")) ? path : path + "/";

        List<FileAttributes> list = blobsForPrefix(prefix, false, true)
            .map(throwFunction(this::getGcsFileAttributes))
            .toList();
        if (list.isEmpty()) {
            // this will throw FileNotFound if there is no directory
            this.getAttributes(tenantId, namespace, uri);
        }
        return list;
    }

    @Override
    public List<FileAttributes> listInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        String path = getPath(uri);
        String prefix = (path.endsWith("/")) ? path : path + "/";
        //in case uri is null, we need to search in the root ("")
        prefix = prefix.equals("/") ? "" : prefix;

        List<FileAttributes> list = blobsForPrefix(prefix, false, true)
            .map(throwFunction(this::getGcsFileAttributes))
            .toList();
        if (list.isEmpty()) {
            // this will throw FileNotFound if there is no directory
            this.getAttributes(uri, path);
        }
        return list;
    }

    private Stream<Blob> blobsForPrefix(String prefix, boolean recursive, boolean includeDirectories) {
        Storage.BlobListOption[] blobListOptions = Stream.concat(
            Stream.of(Storage.BlobListOption.prefix(prefix)),
            recursive ? Stream.empty() : Stream.of(Storage.BlobListOption.currentDirectory())
        ).toArray(Storage.BlobListOption[]::new);
        Page<Blob> blobs = this.storage.list(bucket, blobListOptions);
        return blobs.streamAll()
            .filter(blob ->
            {
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
    public boolean exists(String tenantId, @Nullable String namespace, URI uri) {
        BlobId blobId = this.blob(tenantId, URI.create(uri.getPath()));
        return exists(blobId);
    }

    @Override
    public boolean existsInstanceResource(@Nullable String namespace, URI uri) {
        BlobId blobId = this.blob(URI.create(uri.getPath()));
        return exists(blobId);
    }

    private boolean exists(BlobId blobId) {
        try {
            Blob blob = this.storage.get(blobId);
            return blob != null && blob.exists();
        } catch (StorageException e) {
            return false;
        }
    }

    @Override
    public FileAttributes getAttributes(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        String path = getPath(tenantId, uri);
        if (!exists(tenantId, namespace, uri)) {
            path = path + "/";
        }
        return getAttributes(uri, path);
    }

    @Override
    public FileAttributes getInstanceAttributes(@Nullable String namespace, URI uri) throws IOException {
        String path = getPath(uri);
        if (!exists(this.blob(uri))) {
            path = path + "/";
        }
        return getAttributes(uri, path);
    }

    private FileAttributes getAttributes(URI uri, String path) throws FileNotFoundException {
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
    public URI put(String tenantId, @Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        String path = getPath(tenantId, uri);
        BlobInfo blobInfo = BlobInfo
            .newBuilder(this.blob(tenantId, uri))
            .setMetadata(storageObject.metadata())
            .build();
        return put(uri, storageObject, path, blobInfo);
    }

    @Override
    public URI putInstanceResource(@Nullable String namespace, URI uri, StorageObject storageObject) throws IOException {
        String path = getPath(uri);
        BlobInfo blobInfo = BlobInfo
            .newBuilder(this.blob(uri))
            .setMetadata(storageObject.metadata())
            .build();
        return put(uri, storageObject, path, blobInfo);
    }

    private URI put(URI uri, StorageObject storageObject, String path, BlobInfo blobInfo)
        throws IOException {
        try {
            mkdirs(path);
            try (
                WriteChannel writer = this.storage.writer(blobInfo);
                InputStream data = storageObject.inputStream()
            ) {
                byte[] buffer = new byte[10_240];

                int limit;
                while ((limit = data.read(buffer)) >= 0) {
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }

            return URI.create("kestra://" + uri.getPath());
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private void mkdirs(String path) {
        if (path == null || path.isEmpty())
            return;

        String dirPath = path.endsWith("/") ? path : path.substring(0, path.lastIndexOf('/') + 1);

        String[] parts = dirPath.split("/");
        StringBuilder currentPath = new StringBuilder();

        for (String part : parts) {
            if (!part.isEmpty()) {
                currentPath.append(part).append("/");
                String dir = currentPath.toString();

                if (!exists(blob(dir))) {
                    try {
                        BlobInfo blobInfo = BlobInfo.newBuilder(blob(dir)).build();
                        storage.create(blobInfo);
                    } catch (StorageException e) {
                        log.warn("Failed to create directory: {}", dir, e);
                    }
                }
            }
        }
    }

    @Override
    public boolean delete(String tenantId, @Nullable String namespace, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getAttributes(tenantId, namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        }

        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            return !this.deleteByPrefix(
                tenantId,
                namespace,
                uri.getPath().endsWith("/") ? uri : URI.create(uri.getPath() + "/")
            ).isEmpty();
        }

        return this.storage.delete(this.blob(tenantId, uri));
    }

    @Override
    public boolean deleteInstanceResource(@Nullable String namespace, URI uri) throws IOException {
        FileAttributes fileAttributes;
        try {
            fileAttributes = getInstanceAttributes(namespace, uri);
        } catch (FileNotFoundException e) {
            return false;
        }

        if (fileAttributes.getType() == FileAttributes.FileType.Directory) {
            return !this.deleteByPrefix(
                uri.getPath().endsWith("/") ? uri : URI.create(uri.getPath() + "/")
            ).isEmpty();
        }

        return this.storage.delete(this.blob(uri));
    }

    @Override
    public URI createDirectory(String tenantId, @Nullable String namespace, URI uri) {
        String path = getPath(tenantId, uri);
        return createDirectory(uri, path);
    }

    @Override
    public URI createInstanceDirectory(String namespace, URI uri) {
        return createDirectory(uri, getPath(uri));
    }

    private URI createDirectory(URI uri, String path) {
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        mkdirs(path);
        return createUri(uri.getPath());
    }

    @Override
    public URI move(String tenantId, @Nullable String namespace, URI from, URI to) throws IOException {
        String path = getPath(tenantId, from);
        Map<URI, BlobId> toDelete = new LinkedHashMap<>();

        if (getAttributes(tenantId, namespace, from).getType() == FileAttributes.FileType.File) {
            // move just a file
            BlobId source = blob(path);
            BlobId target = blob(tenantId, to);
            copyForMove(source, target, toDelete);
        } else {
            // move directories
            String prefix = (!path.endsWith("/")) ? path + "/" : path;

            Page<Blob> list = this.storage.list(bucket, Storage.BlobListOption.prefix(prefix));
            list.streamAll().forEach(blob ->
            {
                BlobId target = blob(getPath(tenantId, to) + "/" + blob.getName().substring(prefix.length()));
                copyForMove(blob.getBlobId(), target, toDelete);
            });
        }
        batchDeleteWithRetry(toDelete);
        return createUri(to.getPath());
    }

    private void copyForMove(BlobId source, BlobId target, Map<URI, BlobId> toDelete) {
        this.storage.copy(Storage.CopyRequest.newBuilder().setSource(source).setTarget(target).build());
        toDelete.put(URI.create("kestra://" + source.getName()), source);
    }

    @Override
    public List<URI> purgeByLastModified(
        String tenantId,
        @Nullable String namespace,
        URI prefix,
        @Nullable Instant startDate,
        @Nullable Instant endDate,
        boolean dryRun
    ) throws IOException {
        try {
            var path = getPath(tenantId, prefix);
            Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix(path));

            var matched = new ArrayList<URI>();
            var chunk = new ArrayList<BlobId>(BATCH_DELETE_LIMIT);

            for (var blob : page.iterateAll()) {
                if (blob.getName().endsWith("/")) {
                    continue;
                }
                var updateTime = blob.getUpdateTimeOffsetDateTime();
                if (isInWindow(updateTime, startDate, endDate)) {
                    matched.add(URI.create("kestra://" + prefix.getPath() + blob.getName().substring(path.length())));
                    if (!dryRun) {
                        chunk.add(blob.getBlobId());
                        if (chunk.size() == BATCH_DELETE_LIMIT) {
                            batchDelete(chunk);
                            chunk.clear();
                        }
                    }
                }
            }

            if (!chunk.isEmpty()) {
                batchDelete(chunk);
            }

            return matched;
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private static boolean isInWindow(OffsetDateTime updateTime, Instant startDate, Instant endDate) {
        if (updateTime == null) {
            return false;
        }
        var instant = updateTime.toInstant();
        if (startDate != null && instant.isBefore(startDate)) {
            return false;
        }
        if (endDate != null && instant.isAfter(endDate)) {
            return false;
        }
        return true;
    }

    private void batchDelete(List<BlobId> blobIds) throws IOException {
        var toDelete = new LinkedHashMap<URI, BlobId>();
        for (var blobId : blobIds) {
            toDelete.put(URI.create("kestra://" + blobId.getName()), blobId);
        }
        bulkDelete(toDelete);
    }

    @Override
    public List<URI> deleteByPrefix(String tenantId, @Nullable String namespace, URI storagePrefix) throws IOException {
        try {
            Map<URI, BlobId> toDelete = new LinkedHashMap<>();

            String prefix = getPath(tenantId, storagePrefix);

            Page<Blob> blobs = this.storage
                .list(bucket, Storage.BlobListOption.prefix(prefix));

            for (Blob blob : blobs.iterateAll()) {
                BlobId blobId = blob.getBlobId();
                toDelete.put(URI.create("kestra://" + blobId.getName().replaceFirst(tenantId, "").replaceAll("/$", "")), blobId);
            }

            return bulkDelete(toDelete);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private List<URI> deleteByPrefix(URI storagePrefix) throws IOException {
        try {
            Map<URI, BlobId> toDelete = new LinkedHashMap<>();

            String prefix = getPath(storagePrefix);

            Page<Blob> blobs = this.storage.list(bucket, Storage.BlobListOption.prefix(prefix));

            for (Blob blob : blobs.iterateAll()) {
                BlobId blobId = blob.getBlobId();
                toDelete.put(URI.create("kestra://" + blobId.getName().replaceAll("/$", "")), blobId);
            }

            return bulkDelete(toDelete);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    private List<URI> bulkDelete(Map<URI, BlobId> toDelete) throws IOException {
        if (toDelete.isEmpty()) {
            return List.of();
        }

        Map<URI, Boolean> results = batchDeleteWithRetry(toDelete);

        List<URI> failed = results.entrySet().stream()
            .filter(e -> !Boolean.TRUE.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .toList();

        if (!failed.isEmpty()) {
            throw new IOException(
                "Unable to delete all files, failed on [" +
                    failed.stream().map(URI::getPath).collect(Collectors.joining(", ")) +
                    "]"
            );
        }

        return new ArrayList<>(toDelete.keySet());
    }

    /**
     * Submits the given deletes as a GCS batch, rebuilding and re-submitting a fresh batch on
     * transient {@link StorageException}s. A {@link StorageBatch} is single-use, so the batch is
     * recreated on every attempt rather than re-submitting the same instance.
     *
     * @return per-URI deletion outcome ({@code true} when the object was deleted)
     */
    private Map<URI, Boolean> batchDeleteWithRetry(Map<URI, BlobId> toDelete) {
        StorageException last = null;
        for (int attempt = 1; attempt <= BATCH_SUBMIT_MAX_ATTEMPTS; attempt++) {
            StorageBatch batch = this.storage.batch();
            Map<URI, StorageBatchResult<Boolean>> results = new LinkedHashMap<>();
            toDelete.forEach((uri, blobId) -> results.put(uri, batch.delete(blobId)));

            try {
                batch.submit();
            } catch (StorageException e) {
                last = e;
                if (attempt < BATCH_SUBMIT_MAX_ATTEMPTS) {
                    log.warn("GCS batch submit failed (attempt {}/{}), retrying: {}",
                        attempt, BATCH_SUBMIT_MAX_ATTEMPTS, e.getMessage());
                    sleepBackoff(attempt);
                    continue;
                }
                throw e;
            }

            Map<URI, Boolean> outcome = new LinkedHashMap<>();
            results.forEach((uri, result) -> outcome.put(uri, result != null && Boolean.TRUE.equals(result.get())));
            return outcome;
        }
        throw last; // unreachable: the loop either returns or throws
    }

    private static void sleepBackoff(int attempt) {
        try {
            Thread.sleep(BATCH_SUBMIT_RETRY_BACKOFF_MS * attempt);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException(0, "Interrupted while retrying GCS batch submit", e);
        }
    }

    private static URI createUri(String key) {
        return URI.create("kestra://%s".formatted(key));
    }
}
