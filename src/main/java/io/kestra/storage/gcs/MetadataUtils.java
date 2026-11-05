package io.kestra.storage.gcs;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Stores metadata keys in snake case on every write, since a presigned upload sends them as header names that reach
 * GCS lowercased and would otherwise lose the case of a key such as {@code expirationDate}.
 */
final class MetadataUtils {

    private static final Pattern UPPERCASE = Pattern.compile("([A-Z])");
    private static final Pattern WORD_SEPARATOR = Pattern.compile("_([a-z])");

    private MetadataUtils() {
    }

    static Map<String, String> toStoredMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return null;
        }
        var stored = new HashMap<String, String>(metadata.size());
        metadata.forEach((key, value) -> stored.put(UPPERCASE.matcher(key).replaceAll("_$1").toLowerCase(Locale.ROOT), value));
        return stored;
    }

    static Map<String, String> toRetrievedMetadata(Map<String, String> metadata) {
        if (metadata == null) {
            return null;
        }
        var retrieved = new HashMap<String, String>(metadata.size());
        metadata.forEach((key, value) -> retrieved.put(WORD_SEPARATOR.matcher(key).replaceAll(match -> match.group(1).toUpperCase(Locale.ROOT)), value));
        return retrieved;
    }
}
