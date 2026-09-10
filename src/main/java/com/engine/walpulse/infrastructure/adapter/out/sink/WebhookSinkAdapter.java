package com.engine.walpulse.infrastructure.adapter.out.sink;

import com.engine.walpulse.application.dto.WalPulseProperties;
import com.engine.walpulse.domain.exception.WalStreamException;
import com.engine.walpulse.domain.model.SinkRecord;
import com.engine.walpulse.domain.port.out.EventSinkPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;

/**
 * High-performance HTTP/2 Webhook sink adapter using Java 21 HttpClient.
 */
public class WebhookSinkAdapter implements EventSinkPort {

    private static final Logger log = LoggerFactory.getLogger(WebhookSinkAdapter.class);

    private final WalPulseProperties.WebhookProperties config;
    private final HttpClient httpClient;

    public WebhookSinkAdapter(WalPulseProperties.WebhookProperties config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofMillis(config.getTimeoutMs()))
                .build();
    }

    @Override
    public CompletableFuture<Void> send(SinkRecord record) {
        String signature = computeHmacSha256(record.payloadJson(), config.getSecret());

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(config.getUrl()))
                .timeout(Duration.ofMillis(config.getTimeoutMs()))
                .header("Content-Type", "application/json")
                .header("X-WalPulse-Event-Id", record.id())
                .header("X-WalPulse-LSN", record.lsn().asString())
                .header("X-WalPulse-Operation", record.operation().name())
                .header("X-WalPulse-Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(record.payloadJson()))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenAccept(response -> {
                    if (response.statusCode() >= 200 && response.statusCode() < 300) {
                        log.debug("Webhook delivered successfully for event {}, status: {}", record.id(), response.statusCode());
                    } else {
                        log.warn("Webhook returned non-2xx status code {} for event {}", response.statusCode(), record.id());
                        throw new WalStreamException("Webhook failed with status: " + response.statusCode());
                    }
                });
    }

    @Override
    public String getSinkName() {
        return "webhook";
    }

    private String computeHmacSha256(String data, String secret) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(secretKey);
            byte[] rawHmac = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(rawHmac);
        } catch (Exception e) {
            log.warn("Failed to compute HMAC signature, sending empty signature", e);
            return "";
        }
    }
}
