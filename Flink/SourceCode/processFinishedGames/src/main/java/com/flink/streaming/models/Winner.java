package com.flink.streaming.models;

import java.io.Serializable;
public class Winner {
    private String name;
    private String color;
    public Winner() {}
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

}
