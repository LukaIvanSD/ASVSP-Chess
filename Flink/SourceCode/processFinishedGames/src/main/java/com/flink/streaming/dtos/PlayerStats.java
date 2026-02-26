package com.flink.streaming.dtos;

import java.util.ArrayList;
import java.util.List;

public class PlayerStats {
    private String playerName;
    private int gamesWon;
    private int gamesPlayed;
    private CastlingStats castlingStats;
    private List<GameCheckStats> checkStats =  new ArrayList<>();
    private StreakStats streakStats;
    private boolean isDefault=false;

    public PlayerStats(){}
    public int getGamesPlayed() {
        return gamesPlayed;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public int getGamesWon() {
        return gamesWon;
    }

    public void setGamesWon(int gamesWon) {
        this.gamesWon = gamesWon;
    }

    public void setGamesPlayed(int gamesPlayed) {
        this.gamesPlayed = gamesPlayed;
    }

    public CastlingStats getCastlingStats() {
        return castlingStats;
    }

    public void setCastlingStats(CastlingStats castlingStats) {
        this.castlingStats = castlingStats;
    }

    public List<GameCheckStats> getCheckStats() {
        return checkStats;
    }

    public void setCheckStats(List<GameCheckStats> checkStats) {
        this.checkStats = checkStats;
    }

    public StreakStats getStreakStats() {
        return streakStats;
    }

    public void setStreakStats(StreakStats streakStats) {
        this.streakStats = streakStats;
    }

    public void setIsDefault(boolean isDefault) {
        this.isDefault=isDefault;
    }
    public boolean getIsDefault() {
        return isDefault;
    }
    public int getNumberOfChecks() {
        int numberOfChecks = 0;
        for (GameCheckStats gameCheckStats : checkStats) {
            numberOfChecks += gameCheckStats.getNumberOfChecks();
        }
        return numberOfChecks;
    }
}
