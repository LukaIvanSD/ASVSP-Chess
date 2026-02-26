package com.flink.streaming.models;

import java.io.Serializable;

public class Moves {
    private int moveNumber;
    private Move white;
    private Move black;

    public Moves(){}
    public int getMoveNumber() {
        return moveNumber;
    }

    public void setMoveNumber(int moveNumber) {
        this.moveNumber = moveNumber;
    }

    public Move getWhite() {
        return white;
    }

    public void setWhite(Move white) {
        this.white = white;
    }

    public Move getBlack() {
        return black;
    }

    public void setBlack(Move black) {
        this.black = black;
    }
}
