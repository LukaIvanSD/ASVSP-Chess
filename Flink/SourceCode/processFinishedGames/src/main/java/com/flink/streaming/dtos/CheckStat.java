package com.flink.streaming.dtos;

import java.util.ArrayList;
import java.util.List;

public class CheckStat {
    private Period checkPeriod;
    private int numberOfChecks;
    private List<FigureCheckStat> figureCheckStatList =  new ArrayList<>();
    public CheckStat(Period checkPeriod, List<FigureCheckStat> figureCheckStatList) {
        this.checkPeriod = checkPeriod;
        this.figureCheckStatList = figureCheckStatList;
        this.numberOfChecks = figureCheckStatList.size();
    }
    public CheckStat(){}

    public Period getCheckPeriod() {
        return checkPeriod;
    }

    public void setCheckPeriod(Period checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

    public int getNumberOfChecks() {
        return numberOfChecks;
    }

    public void setNumberOfChecks(int numberOfChecks) {
        this.numberOfChecks = numberOfChecks;
    }

    public List<FigureCheckStat> getFigureCheckStatList() {
        return figureCheckStatList;
    }

    public void setFigureCheckStatList(List<FigureCheckStat> figureCheckStatList) {
        this.figureCheckStatList = figureCheckStatList;
    }

    public void addFigureCheckStat(Figure figure) {
        for (FigureCheckStat figureCheckStat : figureCheckStatList) {
            if (figureCheckStat.getFigure().equals(figure)) {
                figureCheckStat.setNumberOfChecks(figureCheckStat.getNumberOfChecks() + 1);
                return;
            }
        }
        figureCheckStatList.add(new FigureCheckStat(figure, 1));
    }
}
