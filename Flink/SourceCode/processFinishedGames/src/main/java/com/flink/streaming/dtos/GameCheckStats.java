package com.flink.streaming.dtos;

import java.util.ArrayList;
import java.util.List;

public class GameCheckStats {
    private Result result;
    private int numberOfChecks;
    private List<CheckStat> checkStatList =  new ArrayList<>();
    public GameCheckStats() {}

    public GameCheckStats(Result result, int numberOfChecks, List<CheckStat> checkStatList) {
        this.result = result;
        this.numberOfChecks = numberOfChecks;
        this.checkStatList = checkStatList;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public int getNumberOfChecks() {
        return numberOfChecks;
    }

    public void setNumberOfChecks(int numberOfChecks) {
        this.numberOfChecks = numberOfChecks;
    }

    public List<CheckStat> getCheckStatList() {
        return checkStatList;
    }

    public void setCheckStatList(List<CheckStat> checkStatList) {
        this.checkStatList = checkStatList;
    }
    public void addCheckStat(Period period, Figure figure){
        this.setNumberOfChecks(this.getNumberOfChecks() + 1);
        for (CheckStat cS : checkStatList) {
            if (cS.getCheckPeriod() == period) {
                cS.setNumberOfChecks(cS.getNumberOfChecks() + 1);
                cS.addFigureCheckStat(figure);
                return;
            }
        }
        CheckStat checkStat = new CheckStat();
        checkStat.setNumberOfChecks(1);
        checkStat.setCheckPeriod(period);
        checkStat.addFigureCheckStat(figure);
        checkStatList.add(checkStat);
    }
}
