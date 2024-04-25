package io.kestra.storage.gcs;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public interface GcsConfig {

    @JsonProperty
    @NotNull
    @NotBlank
    String getBucket();

    @JsonProperty
    String getServiceAccount();

    @JsonProperty
    String getProjectId();
}
