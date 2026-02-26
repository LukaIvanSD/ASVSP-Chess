package com.flink.streaming.dtos;

public class StreakOutput {
    private int streakNumber;
    private Result streakResult;
    private double nextGameWinPercentage;
    private double nextGameLosePercentage;
    private double nextGameDrawPercentage;
    public StreakOutput() {}

    public StreakOutput(StreakOutput streakOutput) {
        this.streakNumber = streakOutput.streakNumber;
        this.streakResult = streakOutput.streakResult;
        this.nextGameWinPercentage = streakOutput.nextGameWinPercentage;
        this.nextGameLosePercentage = streakOutput.nextGameLosePercentage;
        this.nextGameDrawPercentage = streakOutput.nextGameDrawPercentage;
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

    public void setStreakResult(Result streakResult) {
        this.streakResult = streakResult;
    }

    public double getNextGameWinPercentage() {
        return nextGameWinPercentage;
    }

    public void setNextGameWinPercentage(double nextGameWinPercentage) {
        this.nextGameWinPercentage = nextGameWinPercentage;
    }

    public double getNextGameLosePercentage() {
        return nextGameLosePercentage;
    }

    public void setNextGameLosePercentage(double nextGameLosePercentage) {
        this.nextGameLosePercentage = nextGameLosePercentage;
    }

    public double getNextGameDrawPercentage() {
        return nextGameDrawPercentage;
    }

    public void setNextGameDrawPercentage(double nextGameDrawPercentage) {
        this.nextGameDrawPercentage = nextGameDrawPercentage;
    }
}
