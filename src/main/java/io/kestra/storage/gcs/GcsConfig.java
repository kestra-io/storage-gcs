package io.kestra.storage.gcs;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Getter;

import jakarta.inject.Singleton;

@Singleton
@Getter
@ConfigurationProperties("kestra.storage.gcs")
public class GcsConfig {
    String bucket;

    String serviceAccount;

    String projectId;
}
