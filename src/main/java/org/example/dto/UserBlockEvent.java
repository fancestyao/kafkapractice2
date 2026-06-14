package org.example.dto;

import lombok.Builder;

@Builder
public record UserBlockEvent(
        String blockerId,
        String blockedId
) {}
