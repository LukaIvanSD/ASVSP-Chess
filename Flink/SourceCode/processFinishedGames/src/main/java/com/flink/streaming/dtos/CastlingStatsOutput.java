package com.flink.streaming.dtos;

public class CastlingStatsOutput {
    private int castlingPlayed;
    private double castlingPlayedPercent;
    private double castlingWinPercentage;
    private double castlingLossPercentage;
    private double castlingDrawPercentage;
    private CastlingTypeStats shortCastlingStats;
    private CastlingTypeStats longCastlingStats;
    public CastlingStatsOutput(){}

    public int getCastlingPlayed() {
        return castlingPlayed;
    }

    public void setCastlingPlayed(int castlingPlayed) {
        this.castlingPlayed = castlingPlayed;
    }

    public double getCastlingPlayedPercent() {
        return castlingPlayedPercent;
    }

    public void setCastlingPlayedPercent(double castlingPlayedPercent) {
        this.castlingPlayedPercent = castlingPlayedPercent;
    }

    public double getCastlingWinPercentage() {
        return castlingWinPercentage;
    }

    public void setCastlingWinPercentage(double castlingWinPercentage) {
        this.castlingWinPercentage = castlingWinPercentage;
    }

    public double getCastlingLossPercentage() {
        return castlingLossPercentage;
    }

    public void setCastlingLossPercentage(double castlingLossPercentage) {
        this.castlingLossPercentage = castlingLossPercentage;
    }

    public double getCastlingDrawPercentage() {
        return castlingDrawPercentage;
    }

    public void setCastlingDrawPercentage(double castlingDrawPercentage) {
        this.castlingDrawPercentage = castlingDrawPercentage;
    }

    public CastlingTypeStats getShortCastlingStats() {
        return shortCastlingStats;
    }

    public void setShortCastlingStats(CastlingTypeStats shortCastlingStats) {
        this.shortCastlingStats = shortCastlingStats;
    }

    public CastlingTypeStats getLongCastlingStats() {
        return longCastlingStats;
    }

    public void setLongCastlingStats(CastlingTypeStats longCastlingStats) {
        this.longCastlingStats = longCastlingStats;
    }
}
