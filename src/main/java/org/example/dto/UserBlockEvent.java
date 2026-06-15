package org.example.dto;

import lombok.Builder;

@Builder
public record UserBlockEvent(
        // идентификатор блокирующего пользователя
        String blockerId,
        // идентификатор заблокированного пользователя
        String blockedId
) {}
