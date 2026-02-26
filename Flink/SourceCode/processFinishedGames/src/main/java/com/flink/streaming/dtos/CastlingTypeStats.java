package com.flink.streaming.dtos;

public class CastlingTypeStats {
    private int castlingPlayed;
    private double castlingPlayedPercentage;
    private double castlingWonPercentage;
    private double castlingDrawPercentage;
    private double castlingLossPercentage;
    private CastlingPeriodStatsOutput earlyPeriodStats;
    private CastlingPeriodStatsOutput middlePeriodStats;
    private CastlingPeriodStatsOutput latePeriodStats;

    public CastlingTypeStats() {}
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

    public double getCastlingWonPercentage() {
        return castlingWonPercentage;
    }

    public void setCastlingWonPercentage(double castlingWonPercentage) {
        this.castlingWonPercentage = castlingWonPercentage;
    }

    public double getCastlingDrawPercentage() {
        return castlingDrawPercentage;
    }

    public void setCastlingDrawPercentage(double castlingDrawPercentage) {
        this.castlingDrawPercentage = castlingDrawPercentage;
    }

    public double getCastlingLossPercentage() {
        return castlingLossPercentage;
    }

    public void setCastlingLossPercentage(double castlingLossPercentage) {
        this.castlingLossPercentage = castlingLossPercentage;
    }

    public CastlingPeriodStatsOutput getEarlyPeriodStats() {
        return earlyPeriodStats;
    }

    public void setEarlyPeriodStats(CastlingPeriodStatsOutput earlyPeriodStats) {
        this.earlyPeriodStats = earlyPeriodStats;
    }

    public CastlingPeriodStatsOutput getMiddlePeriodStats() {
        return middlePeriodStats;
    }

    public void setMiddlePeriodStats(CastlingPeriodStatsOutput middlePeriodStats) {
        this.middlePeriodStats = middlePeriodStats;
    }

    public CastlingPeriodStatsOutput getLatePeriodStats() {
        return latePeriodStats;
    }

    public void setLatePeriodStats(CastlingPeriodStatsOutput latePeriodStats) {
        this.latePeriodStats = latePeriodStats;
    }
}
