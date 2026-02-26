package com.flink.streaming.dtos;

import java.util.ArrayList;
import java.util.List;

public class CastlingStats {
    private int castlingPlayed;
    private List<CastlingType> castlingTypeList =  new ArrayList<>();
    public CastlingStats() {}
    public CastlingStats(int castlingPlayed, List<CastlingType> castlingTypeList) {
        this.castlingPlayed = castlingPlayed;
        this.castlingTypeList = castlingTypeList;
    }
    public int getCastlingPlayed() {
        return castlingPlayed;
    }

    public void setCastlingPlayed(int castlingPlayed) {
        this.castlingPlayed = castlingPlayed;
    }

    public List<CastlingType> getCastlingTypeList() {
        return castlingTypeList;
    }

    public void setCastlingTypeList(List<CastlingType> castlingTypeList) {
        this.castlingTypeList = castlingTypeList;
    }
}