package com.flink.streaming.dtos;

public class StreakStatsOutput {

    private StreakOutput currentStreak;
    private StreakOutput streakWithHighestWinProbability;
    private StreakOutput streakWithHighestLoseProbability;
    private StreakOutput streakWithHighestDrawProbability;

    public StreakStatsOutput() {}
    public StreakOutput getCurrentStreak() {
        return currentStreak;
    }

    public void setCurrentStreak(StreakOutput currentStreak) {
        this.currentStreak = new StreakOutput(currentStreak);
    }

    public StreakOutput getStreakWithHighestWinProbability() {
        return streakWithHighestWinProbability;
    }

    public void setStreakWithHighestWinProbability(StreakOutput streakWithHighestWinProbability) {
        this.streakWithHighestWinProbability =new StreakOutput(streakWithHighestWinProbability);
    }

    public StreakOutput getStreakWithHighestLoseProbability() {
        return streakWithHighestLoseProbability;
    }

    public void setStreakWithHighestLoseProbability(StreakOutput streakWithHighestLoseProbability) {
        this.streakWithHighestLoseProbability = new StreakOutput(streakWithHighestLoseProbability);
    }

    public StreakOutput getStreakWithHighestDrawProbability() {
        return streakWithHighestDrawProbability;
    }

    public void setStreakWithHighestDrawProbability(StreakOutput streakWithHighestDrawProbability) {
        this.streakWithHighestDrawProbability = new StreakOutput(streakWithHighestDrawProbability);
    }
}
