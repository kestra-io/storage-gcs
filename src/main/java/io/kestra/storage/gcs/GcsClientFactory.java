package io.kestra.storage.gcs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.micronaut.context.annotation.Factory;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import jakarta.inject.Singleton;

@Factory
@GcsStorageEnabled
public class GcsClientFactory {
    @SneakyThrows
    protected GoogleCredentials credentials(GcsConfig config) {
        GoogleCredentials credentials;

        if (config.getServiceAccount() != null) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(config.getServiceAccount().getBytes());
            credentials = ServiceAccountCredentials.fromStream(byteArrayInputStream);

        } else {
            credentials = GoogleCredentials.getApplicationDefault();
        }

        return credentials;
    }

    @Singleton
    public Storage of(GcsConfig config) {
        return StorageOptions
            .newBuilder()
            .setCredentials(this.credentials(config))
            .setProjectId(config.getProjectId())
            .build()
            .getService();
    }
}
