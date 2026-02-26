package com.flink.streaming.dtos;

public class PeriodCheckStatOutput {
    private Period period;
    private int numberOfChecks;
    private double checkPercentage;
    private int numberOfWins;
    private double winPercentage;
    public PeriodCheckStatOutput(){}

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    public int getNumberOfChecks() {
        return numberOfChecks;
    }

    public void setNumberOfChecks(int numberOfChecks) {
        this.numberOfChecks = numberOfChecks;
    }

    public double getCheckPercentage() {
        return checkPercentage;
    }

    public void setCheckPercentage(double checkPercentage) {
        this.checkPercentage = checkPercentage;
    }

    public int getNumberOfWins() {
        return numberOfWins;
    }

    public void setNumberOfWins(int numberOfWins) {
        this.numberOfWins = numberOfWins;
    }

    public double getWinPercentage() {
        return winPercentage;
    }

    public void setWinPercentage(double winPercentage) {
        this.winPercentage = winPercentage;
    }
}
