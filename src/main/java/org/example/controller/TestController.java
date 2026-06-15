package org.example.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.dto.BannedWordEvent;
import org.example.dto.UserBlockEvent;
import org.example.dto.ChatMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping
@RequiredArgsConstructor
@Slf4j
public class TestController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.messages:messages}")
    private String messagesTopic;
    @Value("${kafka.topics.blocked-users:blocked_users}")
    private String blockedUsersTopic;
    @Value("${kafka.topics.banned-words:banned_words}")
    private String bannedWordsTopic;

    // основная логика с отправкой сообщения через постман (в моем случае)
    @PostMapping("/send-text")
    public void sendMessage(@RequestBody ChatMessage message) throws Exception {
        String json = objectMapper.writeValueAsString(message);
        kafkaTemplate.send(messagesTopic, message.senderId(), json);
        log.info("Пользователем: {} отправлено сообщение: {} пользователю: {}",
                message.senderId(), message.text(), message.recipientId());
    }

    // блокировка пользователя
    @PostMapping("/block-user")
    public void blockUser(@RequestBody UserBlockEvent event) throws Exception {
        String key = event.blockerId() + ":" + event.blockedId();
        String json = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(blockedUsersTopic, key, json);
        log.info("Для пользователя: {} пользователь: {} успешно заблокирован",
                event.blockerId(), event.blockedId());
    }

    // добавление заблокированных слов
    @PostMapping("/add-banned-word")
    public void addBannedWord(@RequestBody BannedWordEvent event) throws Exception {
        String key = event.word().toLowerCase();
        String json = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(bannedWordsTopic, key, json);
        log.info("Успешно добавлено новое запрещенное слово: {}", key);
    }
}
