package org.example.dto;

import lombok.Builder;

@Builder
public record ChatMessage(
        // идентификатор отправителя
        String senderId,
        // идентификатор получателя
        String recipientId,
        // текст сообщения
        String text
) {}
