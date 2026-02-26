package com.flink.streaming.functions;
import com.flink.streaming.dtos.Color;
import com.flink.streaming.dtos.Figure;
import com.flink.streaming.dtos.Result;
import com.flink.streaming.dtos.playergamedtos.Move;
import com.flink.streaming.dtos.playergamedtos.MoveType;
import com.flink.streaming.dtos.playergamedtos.PlayerGameEvent;
import com.flink.streaming.models.GameEvent;
import com.flink.streaming.models.Moves;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class PlayerGameEventExtractor implements FlatMapFunction<GameEvent, PlayerGameEvent> {
    @Override
    public void flatMap(GameEvent gameEvent, Collector<PlayerGameEvent> out) throws Exception {

        PlayerGameEvent whiteEvent = new PlayerGameEvent();
        whiteEvent.setTimestamp(gameEvent.getTimestamp());
        whiteEvent.setPlayerName(gameEvent.getWhitePlayer().getName());
        whiteEvent.setGameId(gameEvent.getGameId());
        whiteEvent.setColor(Color.White);
        whiteEvent.setTerminationType(gameEvent.getTerminationType());
        if (gameEvent.getWinner() != null) {
            whiteEvent.setResult(gameEvent.getWinner().getName().equals(gameEvent.getWhitePlayer().getName()) ? Result.WON : Result.LOST);
        } else {
            whiteEvent.setResult(Result.DRAW);
        }

        List<Move> whiteMoves = new ArrayList<>();
        for(Moves move : gameEvent.getMoves()) {
            Move whiteMove = new Move();
            whiteMove.setMoveNumber(move.getMoveNumber());
            whiteMove.setCheck(move.getWhite().isCheck());
            whiteMove.setPgnNotation(move.getWhite().getPgnNotation());
            whiteMove.setMoveType(MoveType.fromString(move.getWhite().getMoveType()));
            whiteMove.setField(move.getWhite().getField());
            whiteMove.setSecondsLeft(move.getWhite().getSecondsLeft());
            whiteMove.setFigure(Figure.valueOf((move.getWhite().getFigure().toUpperCase())));
            whiteMoves.add(whiteMove);
        }

        whiteEvent.setMoves(whiteMoves);
        out.collect(whiteEvent);

        PlayerGameEvent blackEvent = new PlayerGameEvent();
        blackEvent.setTimestamp(gameEvent.getTimestamp());
        blackEvent.setPlayerName(gameEvent.getBlackPlayer().getName());
        blackEvent.setGameId(gameEvent.getGameId());
        blackEvent.setColor(Color.Black);
        blackEvent.setTerminationType(gameEvent.getTerminationType());
        if (gameEvent.getWinner() != null) {
            blackEvent.setResult(gameEvent.getWinner().getName().equals(gameEvent.getBlackPlayer().getName()) ? Result.WON : Result.LOST);
        } else {
            blackEvent.setResult(Result.DRAW);
        }
        List<Move> blackMoves = new ArrayList<>();
        for(Moves move : gameEvent.getMoves()) {
            if(move.getBlack()==null){
                continue;
            }
            Move blackMove = new Move();
            blackMove.setMoveNumber(move.getMoveNumber());
            blackMove.setCheck(move.getBlack().isCheck());
            blackMove.setPgnNotation(move.getBlack().getPgnNotation());
            blackMove.setMoveType(MoveType.fromString(move.getBlack().getMoveType()));
            blackMove.setField(move.getBlack().getField());
            blackMove.setSecondsLeft(move.getBlack().getSecondsLeft());
            blackMove.setFigure(Figure.valueOf((move.getBlack().getFigure().toUpperCase())));
            blackMoves.add(blackMove);
        }
        blackEvent.setMoves(blackMoves);
        out.collect(blackEvent);
    }
}