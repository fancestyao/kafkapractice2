package org.example.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.dto.UserBlockEvent;

import static org.example.processor.ProcessTopologyBuilder.BLOCK_STORE_NAME;

@Slf4j
public class BlockEventProcessor implements Processor<String, String, Void, Void> {

    private final ObjectMapper objectMapper;

    private KeyValueStore<String, String> blockStore;

    public BlockEventProcessor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.blockStore = context.getStateStore(BLOCK_STORE_NAME);
    }

    // процесс, в котором добавляем блокировку пользователем
    @Override
    public void process(Record<String, String> kafkaRecord) {
        try {
            UserBlockEvent event = objectMapper.readValue(kafkaRecord.value(), UserBlockEvent.class);
            String key = event.blockerId() + ":" + event.blockedId();
            log.info("Добавляем блокировку пользователем: {}", key);
            blockStore.put(key, "BLOCKED");
        } catch (Exception e) {
            log.error("Не удалось обработать процесс блокирования пользователя", e);
        }
    }
}