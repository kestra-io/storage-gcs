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
        title = "The GCS service account key, as a JSON string.",
        description = "If not provided, the default credentials will be used."
    )
    @PluginProperty
    String getServiceAccount();

    @Schema(
        title = "The GCP project ID."
    )
    @PluginProperty
    String getProjectId();
}
