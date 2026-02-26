package com.flink.streaming.dtos;

public class StreakStat {
    private Streak streak;
    private int numberOfNextGamesWon = 0;
    private int numberOfNextGamesLost = 0;
    private int numberOfNextGamesDraw = 0;
    public StreakStat(Streak streak, int numberOfNextGamesWon, int numberOfNextGamesLost,int numberOfNextGamesDraw) {
        this.streak = streak;
        this.numberOfNextGamesWon = numberOfNextGamesWon;
        this.numberOfNextGamesLost = numberOfNextGamesLost;
        this.numberOfNextGamesDraw = numberOfNextGamesDraw;
    }

    public int getNumberOfNextGamesDraw() {
        return numberOfNextGamesDraw;
    }

    public void setNumberOfNextGamesDraw(int numberOfNextGamesDraw) {
        this.numberOfNextGamesDraw = numberOfNextGamesDraw;
    }

    public StreakStat(){}
    public Streak getStreak() {
        return streak;
    }

    public void setStreak(Streak streak) {
        this.streak = streak;
    }

    public int getNumberOfNextGamesWon() {
        return numberOfNextGamesWon;
    }

    public void setNumberOfNextGamesWon(int numberOfNextGamesWon) {
        this.numberOfNextGamesWon = numberOfNextGamesWon;
    }

    public int getNumberOfNextGamesLost() {
        return numberOfNextGamesLost;
    }

    public void setNumberOfNextGamesLost(int numberOfNextGamesLost) {
        this.numberOfNextGamesLost = numberOfNextGamesLost;
    }
}
