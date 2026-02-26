package com.flink.streaming.dtos;


public class CastlingType {
    private Type castlingType;
    private Period periodPlayed;
    private Result result;
    public CastlingType() {}
    public CastlingType(Type castlingType, Period periodPlayed, Result result) {
        this.castlingType = castlingType;
        this.periodPlayed = periodPlayed;
        this.result = result;
    }
    public Type getCastlingType() {
        return castlingType;
    }
    public Period getPeriodPlayed() {
        return periodPlayed;
    }
    public Result getResult() {
        return result;
    }

    public void setCastlingType(Type castlingType) {
        this.castlingType = castlingType;
    }

    public void setPeriodPlayed(Period periodPlayed) {
        this.periodPlayed = periodPlayed;
    }

    public void setResult(Result result) {
        this.result = result;
    }
}
