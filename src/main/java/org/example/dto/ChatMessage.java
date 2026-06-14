package org.example.dto;

import lombok.Builder;

@Builder
public record ChatMessage(
        String senderId,
        String recipientId,
        String text
) {}
