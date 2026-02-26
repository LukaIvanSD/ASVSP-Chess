package com.flink.streaming.dtos;

public class CastlingPeriodStatsOutput {
    private int castlingPlayed;
    private double castlingPlayedPercentage;
    private double castlingPeriodWonPercentage;
    private double castlingDrawPercentage;
    private double castlingLostPercentage;
    public CastlingPeriodStatsOutput() {}

    public int getCastlingPlayed() {
        return castlingPlayed;
    }

    public void setCastlingPlayed(int castlingPlayed) {
        this.castlingPlayed = castlingPlayed;
    }

    public double getCastlingPlayedPercentage() {
        return castlingPlayedPercentage;
    }

    public void setCastlingPlayedPercentage(double castlingPlayedPercentage) {
        this.castlingPlayedPercentage = castlingPlayedPercentage;
    }

    public double getCastlingPeriodWonPercentage() {
        return castlingPeriodWonPercentage;
    }

    public void setCastlingPeriodWonPercentage(double castlingPeriodWonPercentage) {
        this.castlingPeriodWonPercentage = castlingPeriodWonPercentage;
    }

    public double getCastlingDrawPercentage() {
        return castlingDrawPercentage;
    }

    public void setCastlingDrawPercentage(double castlingDrawPercentage) {
        this.castlingDrawPercentage = castlingDrawPercentage;
    }

    public double getCastlingLostPercentage() {
        return castlingLostPercentage;
    }

    public void setCastlingLostPercentage(double castlingLostPercentage) {
        this.castlingLostPercentage = castlingLostPercentage;
    }
}
