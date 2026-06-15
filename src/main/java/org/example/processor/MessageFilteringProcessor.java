package org.example.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.dto.ChatMessage;

import java.util.regex.Pattern;

import static org.example.processor.ProcessTopologyBuilder.BANNED_WORDS_STORE_NAME;
import static org.example.processor.ProcessTopologyBuilder.BLOCK_STORE_NAME;

@Slf4j
public class MessageFilteringProcessor implements Processor<String, String, String, String> {

    // правила для идентификации сообщений и их маскирования при совпадении
    private static final String WORD_BOUNDARY = "\\b";
    private static final String REMOVE_CASE = "(?i)";
    private static final String MASKING_PATTERN = "***";

    private final ObjectMapper objectMapper;

    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> blockStore;
    private KeyValueStore<String, String> bannedWordsStore;

    public MessageFilteringProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // добавили контекст процессора и сторы, где хранятся данные о заблокированных пользователях и словах
    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = context;
        this.blockStore = context.getStateStore(BLOCK_STORE_NAME);
        this.bannedWordsStore = context.getStateStore(BANNED_WORDS_STORE_NAME);
    }

    @Override
    public void process(Record<String, String> kafkaRecord) {
        try {
            // считываем сообщение из кафки
            ChatMessage message = objectMapper.readValue(kafkaRecord.value(), ChatMessage.class);

            // приводим к виду, в котором хранится blockStore, для сравнения пары blocker:blocked как recipient:sender
            String blockKey = message.recipientId() + ":" + message.senderId();
            // если нашлась пара, то не отправляем сообщение
            if (blockStore.get(blockKey) != null) {
                return;
            }

            // привели к маскированному виду
            String maskedText = maskBannedWords(message.text());

            // отправляем сообщение в корректном формате
            context.forward(kafkaRecord.withValue(
                    objectMapper.writeValueAsString(
                            ChatMessage.builder()
                                    .senderId(message.senderId())
                                    .recipientId(message.recipientId())
                                    .text(maskedText)
                                    .build()
                    )
            ));
        } catch (Exception e) {
            log.error("Не удалось обработать текст сообщения", e);
        }
    }

    // маскируем слова из контекста сообщения, приводя в формат ***
    private String maskBannedWords(String content) {
        if (content == null) return null;
        try (KeyValueIterator<String, String> iter = bannedWordsStore.all()) {
            while (iter.hasNext()) {
                String word = iter.next().key;
                content = content.replaceAll(
                        REMOVE_CASE + WORD_BOUNDARY + Pattern.quote(word) + WORD_BOUNDARY,
                        MASKING_PATTERN
                );
            }
        }
        return content;
    }
}