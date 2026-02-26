package com.flink.streaming.models;

import java.io.Serializable;

public class TimeControl{
    private int initial;
    private int increment;
    public TimeControl() {}
    public TimeControl(int initial, int increment) {
        this.initial = initial;
        this.increment = increment;
    }
    public int getInitial() {
        return initial;
    }
    public void setInitial(int initial) {
        this.initial = initial;
    }
    public int getIncrement() {
        return increment;
    }
    public void setIncrement(int increment) {
        this.increment = increment;
    }
}
