package io.kestra.storage.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;

public class GcsClientFactory {
    @SneakyThrows
    protected static GoogleCredentials credentials(final GcsConfig config) {
        GoogleCredentials credentials;

        if (config.getServiceAccount() != null) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(config.getServiceAccount().getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);

        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }
        return credentials;
    }

    public static Storage of(final GcsConfig config) {
        return StorageOptions
            .newBuilder()
            .setCredentials(credentials(config))
            .setProjectId(config.getProjectId())
            .build()
            .getService();
    }
}
