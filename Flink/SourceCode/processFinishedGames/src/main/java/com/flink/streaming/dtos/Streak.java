package com.flink.streaming.dtos;

import java.util.ArrayList;
import java.util.List;

public class Streak {
    private int streakNumber = 0;
    private Result streakResult = Result.LOST;
    private long lastGameTimestampFromLastDifferentStreak;
    private List<Long> gameTimeStamps = new ArrayList<>();

    public Streak() {
    }
    public Streak(Streak streak) {
        this.streakNumber = streak.streakNumber;
        this.streakResult = streak.streakResult;
        this.lastGameTimestampFromLastDifferentStreak = streak.lastGameTimestampFromLastDifferentStreak;
        this.gameTimeStamps = new ArrayList<>(streak.gameTimeStamps);
    }
    public Streak(int streakNumber, Result streakResult) {
        this.streakNumber = streakNumber;
        this.streakResult = streakResult;
    }

    public int getStreakNumber() {
        return streakNumber;
    }

    public void setStreakNumber(int streakNumber) {
        this.streakNumber = streakNumber;
    }

    public Result getStreakResult() {
        return streakResult;
    }

    public long getLastGameTimestampFromLastDifferentStreak() {
        return lastGameTimestampFromLastDifferentStreak;
    }

    public void setLastGameTimestampFromLastDifferentStreak(long lastGameTimestampFromLastDifferentStreak) {
        this.lastGameTimestampFromLastDifferentStreak = lastGameTimestampFromLastDifferentStreak;
    }

    public List<Long> getGameTimeStamps() {
        return gameTimeStamps;
    }

    public void setGameTimeStamps(List<Long> gameTimeStamps) {
        this.gameTimeStamps = gameTimeStamps;
    }

    public void setStreakResult(Result streakResult) {
        this.streakResult = streakResult;
    }

    public void updateStreak(Result gameResult,long gameTimeStamp) {
        if (this.streakResult != gameResult){
            this.streakResult = gameResult;
            this.setStreakNumber(1);
            this.lastGameTimestampFromLastDifferentStreak = getLastTimeStamp();
            this.gameTimeStamps.clear();
        }
        else{
            this.setStreakNumber(getStreakNumber()+1);
        }
        this.gameTimeStamps.add(gameTimeStamp);

    }

    public long getLastTimeStamp() {
        long maxTimestamp = 0;
        for (Long timeStamp : gameTimeStamps) {
            if (timeStamp > maxTimestamp) {
                maxTimestamp = timeStamp;
            }
        }
        return maxTimestamp;
    }
    public boolean isEqual(Streak streak) {
        return streakNumber == streak.streakNumber && streakResult == streak.streakResult;
    }
}
