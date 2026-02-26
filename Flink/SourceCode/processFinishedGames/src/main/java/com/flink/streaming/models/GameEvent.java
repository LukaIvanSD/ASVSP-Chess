package com.flink.streaming.models;

import java.io.Serializable;
import java.util.List;

public class GameEvent {

  private String gameType;

  private String gameId;
  private boolean rated;
  private Date startDate;
  private Player whitePlayer;
  private Player blackPlayer;
  private Winner winner;
  private String terminationType;
  private long timestamp;

  private TimeControl timeControl;
  private List<Moves> moves;


    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getGameType() {
    return gameType;
  }
  public void setGameType(String gameType) {
    this.gameType = gameType;
  }
  public GameEvent() {
  }

    public String getGameId() {
        return gameId;
    }

    public void setGameId(String gameId) {
        this.gameId = gameId;
    }

    public boolean isRated() {
        return rated;
    }

    public void setRated(boolean rated) {
        this.rated = rated;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Player getWhitePlayer() {
        return whitePlayer;
    }

    public void setWhitePlayer(Player whitePlayer) {
        this.whitePlayer = whitePlayer;
    }

    public Player getBlackPlayer() {
        return blackPlayer;
    }

    public void setBlackPlayer(Player blackPlayer) {
        this.blackPlayer = blackPlayer;
    }

    public Winner getWinner() {
        return winner;
    }

    public void setWinner(Winner winner) {
        this.winner = winner;
    }

    public TimeControl getTimeControl() {
        return timeControl;
    }

    public void setTimeControl(TimeControl timeControl) {
        this.timeControl = timeControl;
    }

    public List<Moves> getMoves() {
        return moves;
    }

    public void setMoves(List<Moves> moves) {
        this.moves = moves;
    }
    public  String getTerminationType() {
        return terminationType;
    }
    public void setTerminationType(String terminationType) {
      this.terminationType = terminationType;
    }
}
