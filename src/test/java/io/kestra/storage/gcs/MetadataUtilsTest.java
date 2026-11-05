package io.kestra.storage.gcs;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MetadataUtilsTest {

    @Test
    void shouldKeepTheCaseOfAKeyWhenTheHeaderCarryingItIsLowercased() {
        Map<String, String> metadata = Map.of("expirationDate", "2026-01-01T00:00:00Z", "description", "a value");

        Map<String, String> lowercasedOnTheWire = new HashMap<>();
        MetadataUtils.toStoredMetadata(metadata).forEach((key, value) -> lowercasedOnTheWire.put(key.toLowerCase(), value));

        assertThat(MetadataUtils.toRetrievedMetadata(lowercasedOnTheWire), is(metadata));
    }

    @Test
    void shouldReadAKeyStoredInCamelCaseUnchanged() {
        Map<String, String> metadata = Map.of("expirationDate", "2026-01-01T00:00:00Z");

        assertThat(MetadataUtils.toRetrievedMetadata(metadata), is(metadata));
    }
}
