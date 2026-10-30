package io.kestra.storage.gcs;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.docs.JsonSchemaGenerator;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

@KestraTest
class GcsStorageSchemaTest {
    @Inject
    private JsonSchemaGenerator jsonSchemaGenerator;

    @SuppressWarnings("unchecked")
    @Test
    void schema() {
        var schema = jsonSchemaGenerator.properties(StorageInterface.class, GcsStorage.class);

        var properties = (Map<String, Object>) schema.get("properties");
        assertThat(properties, not(hasKey("storage")));
    }
}
