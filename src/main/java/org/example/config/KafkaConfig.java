package org.example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    private static final String APPLICATION_ID = "text-filter-processor";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.topics.messages:messages}")
    private String messagesTopic;
    @Value("${kafka.topics.blocked-users:blocked_users}")
    private String blockedUsersTopic;
    @Value("${kafka.topics.banned-words:banned_words}")
    private String bannedWordsTopic;
    @Value("${kafka.topics.filtered-messages:filtered_messages}")
    private String filteredMessagesTopic;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public NewTopic messagesTopic() {
        return TopicBuilder.name(messagesTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic filteredMessagesTopic() {
        return TopicBuilder.name(filteredMessagesTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic blockedUsersTopic() {
        return TopicBuilder.name(blockedUsersTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic bannedWordsTopic() {
        return TopicBuilder.name(bannedWordsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}