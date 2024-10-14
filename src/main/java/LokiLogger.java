import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LokiLogger {
    private static final Logger logger = LoggerFactory.getLogger(LokiLogger.class);
    private KafkaConsumer<String, String> consumer;


    public LokiLogger(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "loki-logger-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void consumeAndSendToLoki(String lokiUrl) {
        while (true) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
                sendLogToLoki(lokiUrl, record.value());
            }
        }
    }

    private void sendLogToLoki(String lokiUrl, String log) {
        String jsonPayload = "{"
                + "\"streams\": ["
                + "{"
                + "\"stream\": {"
                + "\"job\": \"keycloak-logs\""
                + "},"
                + "\"values\": [["
                + String.valueOf(System.currentTimeMillis() * 1000000) // Tiempo en nanosegundos
                + ", \"" + log + "\""
                + "]]"
                + "}"
                + "]"
                + "}";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(lokiUrl);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(jsonPayload));

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                String responseString = EntityUtils.toString(response.getEntity());
                logger.info("Response from Loki: {}", responseString);
            }
        } catch (Exception e) {
            logger.error("Error sending log to Loki: ", e);
        }
    }
}

