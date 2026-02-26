package com.flink.streaming.models;

import java.io.Serializable;

public class Player {
    private String name;
    private int rating;
    public Player() {}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }
}
