package com.flink.streaming.dtos.playergamedtos;
import com.flink.streaming.dtos.Figure;
import java.io.Serializable;

public class Move  {
    private int moveNumber;
    private Figure figure;
    private MoveType moveType;
    private String field;
    private int secondsLeft;
    private String pgnNotation;
    private boolean check;

    public Move(){}

    public int getMoveNumber() {
        return moveNumber;
    }

    public void setMoveNumber(int moveNumber) {
        this.moveNumber = moveNumber;
    }

    public Figure getFigure() {
        return figure;
    }

    public void setFigure(Figure figure) {
        this.figure = figure;
    }

    public MoveType getMoveType() {
        return moveType;
    }

    public void setMoveType(MoveType moveType) {
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
