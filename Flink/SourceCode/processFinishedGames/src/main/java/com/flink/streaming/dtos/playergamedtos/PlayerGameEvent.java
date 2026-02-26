package com.flink.streaming.dtos.playergamedtos;

import com.flink.streaming.dtos.Color;
import com.flink.streaming.dtos.Result;
import java.io.Serializable;
import java.util.List;

public class PlayerGameEvent {
  private String playerName;
  private Color color;
  private String gameId;
  private Date startDate;
  private Result result;
  private String terminationType;
  private List<Move> moves;
  private long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public PlayerGameEvent() {
  }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    public String getGameId() {
        return gameId;
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }


    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public List<Move> getMoves() {
        return moves;
    }

    public void setMoves(List<Move> moves) {
        this.moves = moves;
    }
    public  String getTerminationType() {
        return terminationType;
    }
    public void setTerminationType(String terminationType) {
      this.terminationType = terminationType;
    }

}
