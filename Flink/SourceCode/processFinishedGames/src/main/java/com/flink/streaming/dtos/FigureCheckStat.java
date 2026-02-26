package com.flink.streaming.dtos;

public class FigureCheckStat {
    private Figure figure;
    private int numberOfChecks;

    public FigureCheckStat(Figure figure, int numberOfChecks) {
        this.figure = figure;
        this.numberOfChecks = numberOfChecks;
    }
    public FigureCheckStat(){

    }
    public Figure getFigure() {
        return figure;
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
}
