package org.example.dto;

import lombok.Builder;

@Builder
public record BannedWordEvent(
        String word
) {}
