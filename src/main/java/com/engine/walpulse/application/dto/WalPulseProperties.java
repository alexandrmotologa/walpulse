package com.engine.walpulse.application.dto;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "walpulse")
public class WalPulseProperties {

    private PostgresProperties postgres = new PostgresProperties();
    private TableFilterRule filter = new TableFilterRule();
    private SinkProperties sink = new SinkProperties();

    public PostgresProperties getPostgres() {
        return postgres;
    }

    public void setPostgres(PostgresProperties postgres) {
        this.postgres = postgres;
    }

    public TableFilterRule getFilter() {
        return filter;
    }

    public void setFilter(TableFilterRule filter) {
        this.filter = filter;
    }

    public SinkProperties getSink() {
        return sink;
    }

    public void setSink(SinkProperties sink) {
        this.sink = sink;
    }

    public static class PostgresProperties {
        private String host = "localhost";
        private int port = 5432;
        private String database = "postgres";
        private String username = "postgres";
        private String password = "postgres";
        private String slotName = "walpulse_slot";
        private String publicationName = "walpulse_pub";
        private boolean createSlotIfMissing = true;
        private int statusIntervalMs = 5000;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getSlotName() { return slotName; }
        public void setSlotName(String slotName) { this.slotName = slotName; }
        public String getPublicationName() { return publicationName; }
        public void setPublicationName(String publicationName) { this.publicationName = publicationName; }
        public boolean isCreateSlotIfMissing() { return createSlotIfMissing; }
        public void setCreateSlotIfMissing(boolean createSlotIfMissing) { this.createSlotIfMissing = createSlotIfMissing; }
        public int getStatusIntervalMs() { return statusIntervalMs; }
        public void setStatusIntervalMs(int statusIntervalMs) { this.statusIntervalMs = statusIntervalMs; }
    }

    public static class SinkProperties {
        private String type = "logging";
        private KafkaProperties kafka = new KafkaProperties();
        private WebhookProperties webhook = new WebhookProperties();

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public KafkaProperties getKafka() { return kafka; }
        public void setKafka(KafkaProperties kafka) { this.kafka = kafka; }
        public WebhookProperties getWebhook() { return webhook; }
        public void setWebhook(WebhookProperties webhook) { this.webhook = webhook; }
    }

    public static class KafkaProperties {
        private String bootstrapServers = "localhost:9092";
        private String topicPrefix = "cdc.";
        private String acks = "all";
        private int retries = 3;

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public String getTopicPrefix() { return topicPrefix; }
        public void setTopicPrefix(String topicPrefix) { this.topicPrefix = topicPrefix; }
        public String getAcks() { return acks; }
        public void setAcks(String acks) { this.acks = acks; }
        public int getRetries() { return retries; }
        public void setRetries(int retries) { this.retries = retries; }
    }

    public static class WebhookProperties {
        private String url = "http://localhost:9000/webhook";
        private String secret = "walpulse-secret";
        private int timeoutMs = 5000;
        private int maxRetries = 3;

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
        public String getSecret() { return secret; }
        public void setSecret(String secret) { this.secret = secret; }
        public int getTimeoutMs() { return timeoutMs; }
        public void setTimeoutMs(int timeoutMs) { this.timeoutMs = timeoutMs; }
        public int getMaxRetries() { return maxRetries; }
        public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    }
}
