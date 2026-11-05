package io.kestra.storage.gcs;

import java.io.IOException;
import java.net.URI;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Date;

import org.junit.jupiter.api.Test;

import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.storage.StorageOptions;

import io.kestra.core.storages.PresignRequest;
import io.kestra.core.storages.PresignedRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GcsStoragePresignTest {

    @Test
    void shouldCapTtlToSevenDaysWhenTtlIsLonger() throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        var storage = storage(
            ServiceAccountCredentials.newBuilder()
                .setClientEmail("test@test-project.iam.gserviceaccount.com")
                .setPrivateKey(generator.generateKeyPair().getPrivate())
                .build()
        );

        var request = storage.presign(get(Duration.ofDays(30))).orElseThrow();

        assertThat(request.url().getQuery(), containsString("X-Goog-Expires=604800"));
    }

    @Test
    void shouldThrowIllegalStateWhenCredentialsCannotSign() {
        var storage = storage(
            UserCredentials.newBuilder().setClientId("id").setClientSecret("secret").setRefreshToken("token").build()
        );

        var exception = assertThrows(
            IllegalStateException.class,
            () -> storage.presign(get(Duration.ofMinutes(5)))
        );

        assertThat(exception.getMessage(), containsString("iam.serviceAccounts.signBlob"));
    }

    @Test
    void shouldThrowIOExceptionWithGuidanceWhenSigningIsRefused() {
        var storage = storage(new RefusingSignerCredentials());

        var exception = assertThrows(
            IOException.class,
            () -> storage.presign(get(Duration.ofMinutes(5)))
        );

        assertThat(exception.getMessage(), containsString("iam.serviceAccounts.signBlob"));
    }

    private static PresignRequest get(Duration ttl) {
        return PresignRequest.builder().tenantId("main").uri(URI.create("/file.txt")).method(PresignedRequest.Method.GET).ttl(ttl).build();
    }

    private static GcsStorage storage(GoogleCredentials credentials) {
        var storage = StorageOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(credentials)
            .build()
            .getService();
        return GcsStorage.builder().bucket("test-bucket").storage(storage).build();
    }

    private static class RefusingSignerCredentials extends GoogleCredentials implements ServiceAccountSigner {
        @Override
        public String getAccount() {
            return "test@test-project.iam.gserviceaccount.com";
        }

        @Override
        public byte[] sign(byte[] toSign) {
            throw new ServiceAccountSigner.SigningException("Permission denied", new IOException());
        }

        @Override
        public AccessToken refreshAccessToken() {
            return new AccessToken("token", new Date(System.currentTimeMillis() + 3600_000));
        }
    }
}
