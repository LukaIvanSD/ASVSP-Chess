package com.flink.streaming.dtos;

import java.util.ArrayList;
import java.util.List;

public class StreakStats {
    private Streak currentStreak;
    private List<StreakStat> streakStatList = new ArrayList<>();
    public StreakStats(Streak currentStreak,List<StreakStat> streakStatList) {
        this.currentStreak = currentStreak;
        this.streakStatList = streakStatList;
    }
    public StreakStats(){}
    public Streak getCurrentStreak() {
        return currentStreak;
    }
    public StreakStat getCurrentStreakStat() {
        for(StreakStat streakStat : streakStatList){
            if(streakStat.getStreak().isEqual(currentStreak)){
                return streakStat;
            }
        }
        return null;
    }

    public void setCurrentStreak(Streak currentStreak) {
        this.currentStreak = currentStreak;
    }

    public List<StreakStat> getStreakStatList() {
        return streakStatList;
    }

    public void setStreakStatList(List<StreakStat> streakStatList) {
        this.streakStatList = streakStatList;
    }

    public void addStreakStatList(Streak currentStreak) {
        StreakStat streakStat = new StreakStat(currentStreak,0,0,0);
        streakStatList.add(streakStat);
    }

    public void updateStreakStatList(Streak currentStreak, Streak previousStreak) {
        for(StreakStat streakStat : streakStatList){
            if(streakStat.getStreak().isEqual(previousStreak)){
                if(currentStreak.getStreakResult() == Result.WON){
                    streakStat.setNumberOfNextGamesWon(streakStat.getNumberOfNextGamesWon()+1);
                }
                else if (currentStreak.getStreakResult() == Result.LOST){
                    streakStat.setNumberOfNextGamesLost(streakStat.getNumberOfNextGamesLost()+1);
                }
                else {
                    streakStat.setNumberOfNextGamesDraw(streakStat.getNumberOfNextGamesDraw()+1);
                }
            }
        }
        for(StreakStat streakStat : streakStatList){
            if(streakStat.getStreak().isEqual(currentStreak)){
                return;
            }
        }
        addStreakStatList(currentStreak);
    }
}
