package com.flink.streaming.dtos;

public class PlayerStatsOutput {
    private String playerName;
    private int totalGamesPlayed;
    private int totalWins;
    private double WinPercentage;
    private CastlingStatsOutput castlingStats;
    private StreakStatsOutput streakStats;
    private CheckStatsOutput checkStats;
    private boolean isDefault=false;

    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean aDefault) {
        isDefault = aDefault;
    }

    public PlayerStatsOutput(){}
    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public int getTotalGamesPlayed() {
        return totalGamesPlayed;
    }

    public void setTotalGamesPlayed(int totalGamesPlayed) {
        this.totalGamesPlayed = totalGamesPlayed;
    }

    public int getTotalWins() {
        return totalWins;
    }

    public void setTotalWins(int totalWins) {
        this.totalWins = totalWins;
    }

    public double getWinPercentage() {
        return WinPercentage;
    }

    public void setWinPercentage(double winPercentage) {
        WinPercentage = winPercentage;
    }

    public CastlingStatsOutput getCastlingStats() {
        return castlingStats;
    }

    public void setCastlingStats(CastlingStatsOutput castlingStats) {
        this.castlingStats = castlingStats;
    }

    public StreakStatsOutput getStreakStats() {
        return streakStats;
    }

    public void setStreakStats(StreakStatsOutput streakStats) {
        this.streakStats = streakStats;
    }

    public CheckStatsOutput getCheckStats() {
        return checkStats;
    }

    public void setCheckStats(CheckStatsOutput checkStats) {
        this.checkStats = checkStats;
    }

}
