package io.kestra.storage.gcs;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MetadataUtilsTest {

    @Test
    void shouldKeepTheCaseOfAKeyWhenTheHeaderCarryingItIsLowercased() {
        var metadata = Map.of("expirationDate", "2026-01-01T00:00:00Z", "description", "a value");

        var lowercasedOnTheWire = new HashMap<String, String>();
        MetadataUtils.toStoredMetadata(metadata).forEach((key, value) -> lowercasedOnTheWire.put(key.toLowerCase(Locale.ROOT), value));

        assertThat(MetadataUtils.toRetrievedMetadata(lowercasedOnTheWire), is(metadata));
    }

    @Test
    void shouldReadAKeyStoredInCamelCaseUnchanged() {
        var metadata = Map.of("expirationDate", "2026-01-01T00:00:00Z");

        assertThat(MetadataUtils.toRetrievedMetadata(metadata), is(metadata));
    }
}
