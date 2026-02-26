package com.flink.streaming.models;

import java.io.Serializable;

public class Move  {
    private String figure;
    private String moveType;
    private String field;
    private int secondsLeft;
    private String pgnNotation;
    private boolean check;

    public Move(){}

    public String getFigure() {
        return figure;
    }

    public void setFigure(String figure) {
        this.figure = figure;
    }

    public String getMoveType() {
        return moveType;
    }

    public void setMoveType(String moveType) {
        this.moveType = moveType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public int getSecondsLeft() {
        return secondsLeft;
    }

    public void setSecondsLeft(int secondsLeft) {
        this.secondsLeft = secondsLeft;
    }

    public String getPgnNotation() {
        return pgnNotation;
    }

    public void setPgnNotation(String pgnNotation) {
        this.pgnNotation = pgnNotation;
    }

    public boolean isCheck() {
        return check;
    }

    public void setCheck(boolean check) {
        this.check = check;
    }

}
