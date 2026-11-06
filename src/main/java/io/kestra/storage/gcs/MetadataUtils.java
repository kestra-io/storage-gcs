package io.kestra.storage.gcs;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Maps metadata keys to the snake case a presigned upload sends them in, since a header name reaches GCS lowercased
 * and would otherwise lose the case of a key such as {@code expirationDate}.
 */
final class MetadataUtils {

    private static final Pattern UPPERCASE = Pattern.compile("([A-Z])");
    private static final Pattern WORD_SEPARATOR = Pattern.compile("_([a-z])");

    private MetadataUtils() {
    }

    static Map<String, String> toStoredMetadata(Map<String, String> metadata) {
        Map<String, String> stored = new HashMap<>(metadata.size());
        metadata.forEach((key, value) -> stored.put(UPPERCASE.matcher(key).replaceAll("_$1").toLowerCase(), value));
        return stored;
    }

    static Map<String, String> toRetrievedMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return null;
        }
        Map<String, String> retrieved = new HashMap<>(metadata.size());
        metadata.forEach((key, value) -> retrieved.put(WORD_SEPARATOR.matcher(key).replaceAll(match -> match.group(1).toUpperCase()), value));
        return retrieved;
    }
}
