package com.flink.streaming.dtos;

public class FigureCheckStatOutput {
    private Figure figure;
    private int numberOfChecks;
    private double score;
    private double checkPercentage;
    private int numberOfWins;
    private double winPercentage;

    public FigureCheckStatOutput() {}
    public Figure getFigure() {
        return figure;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public void setFigure(Figure figure) {
        this.figure = figure;
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
