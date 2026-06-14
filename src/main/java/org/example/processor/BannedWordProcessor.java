package org.example.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.dto.BannedWordEvent;

import static org.example.processor.ProcessTopologyBuilder.BANNED_WORDS_STORE_NAME;

@Slf4j
public class BannedWordProcessor implements Processor<String, String, Void, Void> {

    private final ObjectMapper objectMapper;

    private KeyValueStore<String, String> bannedWordsStore;

    public BannedWordProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.bannedWordsStore = context.getStateStore(BANNED_WORDS_STORE_NAME);
    }

    @Override
    public void process(Record<String, String> kafkaRecord) {
        try {
            BannedWordEvent event = objectMapper.readValue(kafkaRecord.value(), BannedWordEvent.class);
            String word = event.word().toLowerCase().trim();
            log.info("Слово заблокировано: {}, поместили его в топик: {}", word, BANNED_WORDS_STORE_NAME);
            bannedWordsStore.put(word, "BANNED");
        } catch (Exception e) {
            log.error("Не удалось обработать процесс блокирования слова", e);
        }
    }
}