package org.example.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ProcessTopologyBuilder {

    // названия хранилищ
    public static final String BANNED_WORDS_STORE_NAME = "banned-words-store";
    public static final String BLOCK_STORE_NAME = "block-store";

    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.messages:messages}")
    private String messagesTopic;
    @Value("${kafka.topics.filtered-messages:filtered_messages}")
    private String filteredMessagesTopic;
    @Value("${kafka.topics.blocked-users:blocked_users}")
    private String blockedUsersTopic;
    @Value("${kafka.topics.banned-words:banned_words}")
    private String bannedWordsTopic;

    // строю топологию с помощью streambuilder'а, добавляя правила десериализации через Serdes,
    // а также распредляя процессоры по логике
    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, String>> blockStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(BLOCK_STORE_NAME),
                        Serdes.String(),
                        Serdes.String()
                );
        StoreBuilder<KeyValueStore<String, String>> bannedWordsStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(BANNED_WORDS_STORE_NAME),
                        Serdes.String(),
                        Serdes.String()
                );

        builder.addStateStore(blockStoreBuilder);
        builder.addStateStore(bannedWordsStoreBuilder);

        builder.<String, String>stream(blockedUsersTopic)
                .process(() -> new BlockEventProcessor(objectMapper), BLOCK_STORE_NAME);

        builder.<String, String>stream(bannedWordsTopic)
                .process(() -> new BannedWordProcessor(objectMapper), BANNED_WORDS_STORE_NAME);

        builder.<String, String>stream(messagesTopic)
                .process(() -> new MessageFilteringProcessor(objectMapper), BLOCK_STORE_NAME, BANNED_WORDS_STORE_NAME)
                .to(filteredMessagesTopic);
    }
}