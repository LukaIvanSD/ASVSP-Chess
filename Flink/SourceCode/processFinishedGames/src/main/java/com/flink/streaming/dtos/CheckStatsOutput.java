package com.flink.streaming.dtos;

public class CheckStatsOutput {
    private int numberOfChecks;
    private double checksPerGame;
    private int numberOfChecksWithMostWins;
    private int numberOfChecksWithMostLoses;

    public double numberOfChecksWinPercentage;
    public double numberOfChecksLosePercentage;
    private FigureCheckStatOutput bestFigureCheckStat;
    private PeriodCheckStatOutput bestPeriodCheckStat;

    public CheckStatsOutput(){}
    public int getNumberOfChecks() {
        return numberOfChecks;
    }
    public double getNumberOfChecksWinPercentage() {
        return numberOfChecksWinPercentage;
    }

    public void setNumberOfChecksWinPercentage(double numberOfChecksWinPercentage) {
        this.numberOfChecksWinPercentage = numberOfChecksWinPercentage;
    }

    public double getNumberOfChecksLosePercentage() {
        return numberOfChecksLosePercentage;
    }

    public void setNumberOfChecksLosePercentage(double numberOfChecksLosePercentage) {
        this.numberOfChecksLosePercentage = numberOfChecksLosePercentage;
    }


    public void setNumberOfChecks(int numberOfChecks) {
        this.numberOfChecks = numberOfChecks;
    }

    public double getChecksPerGame() {
        return checksPerGame;
    }

    public void setChecksPerGame(double checksPerGame) {
        this.checksPerGame = checksPerGame;
    }

    public int getNumberOfChecksWithMostWins() {
        return numberOfChecksWithMostWins;
    }

    public void setNumberOfChecksWithMostWins(int numberOfChecksWithMostWins) {
        this.numberOfChecksWithMostWins = numberOfChecksWithMostWins;
    }

    public int getNumberOfChecksWithMostLoses() {
        return numberOfChecksWithMostLoses;
    }

    public void setNumberOfChecksWithMostLoses(int numberOfChecksWithMostLoses) {
        this.numberOfChecksWithMostLoses = numberOfChecksWithMostLoses;
    }

    public FigureCheckStatOutput getBestFigureCheckStat() {
        return bestFigureCheckStat;
    }

    public void setBestFigureCheckStat(FigureCheckStatOutput bestFigureCheckStat) {
        this.bestFigureCheckStat = bestFigureCheckStat;
    }

    public PeriodCheckStatOutput getBestPeriodCheckStat() {
        return bestPeriodCheckStat;
    }

    public void setBestPeriodCheckStat(PeriodCheckStatOutput bestPeriodCheckStat) {
        this.bestPeriodCheckStat = bestPeriodCheckStat;
    }
}
