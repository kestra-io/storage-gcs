package io.kestra.storage.gcs;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public interface GcsConfig {

    @Schema(
        title = "The GCS bucket where to store internal objects."
    )
    @PluginProperty
    @NotNull
    @NotBlank
    String getBucket();

    @Schema(
        title = "Object path prefix within the GCS bucket to store data.",
        description = "If set, all objects will be stored under this prefix (e.g. `bucket/path/`)."
    )
    @PluginProperty
    String getPath();

    @Schema(
        title = "The GCS service account key, as a JSON string.",
        description = "If not provided, the default credentials will be used."
    )
    @PluginProperty(secret = true)
    String getServiceAccount();

    @Schema(
        title = "The GCP project ID."
    )
    @PluginProperty
    String getProjectId();
}
