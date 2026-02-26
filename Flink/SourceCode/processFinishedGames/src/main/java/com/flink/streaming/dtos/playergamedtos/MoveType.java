package com.flink.streaming.dtos.playergamedtos;

public enum MoveType {
        NORMAL,
        CAPTURE,
        CASTLING_SHORT,
        CASTLING_LONG,
        PROMOTION,
        EN_PASSANT,
        CHECK,
        CHECKMATE;

    public static MoveType fromString(String moveType) {
        if (moveType == null) {
            throw new IllegalArgumentException("moveType cannot be null");
        }
        try {
            return MoveType.valueOf(moveType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown MoveType: " + moveType, e);
        }
    }
}
