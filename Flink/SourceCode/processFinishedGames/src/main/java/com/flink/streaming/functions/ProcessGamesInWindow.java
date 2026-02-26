package com.flink.streaming.functions;

import com.flink.streaming.dtos.*;
import com.flink.streaming.dtos.playergamedtos.Move;
import com.flink.streaming.dtos.playergamedtos.MoveType;
import com.flink.streaming.dtos.playergamedtos.PlayerGameEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ProcessGamesInWindow
        extends KeyedProcessFunction<String, PlayerGameEvent, PlayerStats> {

    private transient MapState<Long, PlayerGameEvent> last7Days;
    private transient ValueState<Boolean> firstTimerRegistered;
    @Override
    public void open(Configuration parameters) {

        MapStateDescriptor<Long, PlayerGameEvent> mapStateDescriptor =
                new MapStateDescriptor<>(
                        "last7DaysState",
                        Long.class,
                        PlayerGameEvent.class
                );

        last7Days = getRuntimeContext().getMapState(mapStateDescriptor);
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("timerRegistered", Boolean.class);

        firstTimerRegistered = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            PlayerGameEvent event,
            Context ctx,
            Collector<PlayerStats> out) throws Exception {

        long eventTimestamp = event.getTimestamp();
        long currentProcessingTime = ctx.timerService().currentProcessingTime();

        last7Days.put(eventTimestamp, event);

        long cutoff = currentProcessingTime - Duration.ofDays(7).toMillis();

        List<Long> toRemove = new ArrayList<>();
        for (Long ts : last7Days.keys()) {
            if (ts < cutoff) {
                toRemove.add(ts);
            }
        }
        for (Long ts : toRemove) {
            last7Days.remove(ts);
        }
        Boolean registered = firstTimerRegistered.value();
        if (registered == null || !registered) {
            long next = ctx.timerService().currentProcessingTime()
                    + Duration.ofMinutes(1).toMillis();

            ctx.timerService().registerProcessingTimeTimer(next);

            firstTimerRegistered.update(true);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<PlayerStats> out) throws Exception {

        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        long cutoff = currentProcessingTime - Duration.ofDays(7).toMillis();
        List<PlayerGameEvent> gamesBeforeCutoff = new ArrayList<>();
        List<PlayerGameEvent> games = new ArrayList<>();

        List<Long> toRemove = new ArrayList<>();

        for (Long ts : last7Days.keys()) {
            PlayerGameEvent event = last7Days.get(ts);

            if (ts >= cutoff) {
                games.add(event);
            } else {
                toRemove.add(ts);
            }
            gamesBeforeCutoff.add(event);
        }

        for (Long ts : toRemove) {
            last7Days.remove(ts);
        }

        if (!games.isEmpty()) {
            List<PlayerGameEvent> sortedGames = new ArrayList<>();
            games.stream()
                    .sorted((g1, g2) -> Long.compare(g1.getTimestamp(), g2.getTimestamp()))
                    .forEach(sortedGames::add);

            out.collect(generatePlayerStats(sortedGames));
        }else if(!gamesBeforeCutoff.isEmpty()) {
            PlayerStats playerStats = new PlayerStats();
            playerStats.setIsDefault(true);
            playerStats.setPlayerName(gamesBeforeCutoff.get(0).getPlayerName());
            out.collect(playerStats);
        }

        long next = currentProcessingTime + Duration.ofMinutes(1).toMillis();
        ctx.timerService().registerProcessingTimeTimer(next);
    }

    private PlayerStats generatePlayerStats(List<PlayerGameEvent> games) {

        PlayerStats stats = new PlayerStats();

        int gamesWon = 0;
        int gamesPlayed = 0;
        int castlingPlayed = 0;

        List<CastlingType> castlingTypeList = new ArrayList<>();
        List<GameCheckStats> checkStatsList = new ArrayList<>();

        CastlingStats castlingStats = new CastlingStats();
        StreakStats streakStats = new StreakStats();

        Streak currentStreak = new Streak();
        Streak previousStreak = null;

        for (PlayerGameEvent game : games) {

            gamesPlayed++;

            Result result = game.getResult();
            if (result == Result.WON) {
                gamesWon++;
            }

            GameCheckStats gameCheckStats = new GameCheckStats();
            gameCheckStats.setResult(result);

            currentStreak.updateStreak(result, game.getTimestamp());

            if (previousStreak == null) {
                streakStats.addStreakStatList(currentStreak);
            } else {
                streakStats.updateStreakStatList(currentStreak, previousStreak);
            }

            for (Move move : game.getMoves()) {

                Move lastMove = getLastMove(game.getMoves());

                Period period = lastMove != null
                        ? getMovePeriod(move.getMoveNumber(),
                        (double) lastMove.getMoveNumber())
                        : Period.EARLY;

                if (move.getMoveType() == MoveType.CASTLING_LONG) {

                    CastlingType type = new CastlingType();
                    type.setCastlingType(Type.CASTLING_LONG);
                    type.setResult(result);
                    type.setPeriodPlayed(period);

                    castlingTypeList.add(type);
                    castlingPlayed++;

                } else if (move.getMoveType() == MoveType.CASTLING_SHORT) {

                    CastlingType type = new CastlingType();
                    type.setCastlingType(Type.CASTLING_SHORT);
                    type.setResult(result);
                    type.setPeriodPlayed(period);

                    castlingTypeList.add(type);
                    castlingPlayed++;
                }

                if (move.isCheck()) {
                    gameCheckStats.addCheckStat(period, move.getFigure());
                }
            }

            checkStatsList.add(gameCheckStats);
            previousStreak = new Streak(currentStreak);
        }

        streakStats.setCurrentStreak(currentStreak);

        castlingStats.setCastlingPlayed(castlingPlayed);
        castlingStats.setCastlingTypeList(castlingTypeList);

        stats.setPlayerName(games.get(0).getPlayerName());
        stats.setGamesPlayed(gamesPlayed);
        stats.setGamesWon(gamesWon);
        stats.setCastlingStats(castlingStats);
        stats.setCheckStats(checkStatsList);
        stats.setStreakStats(streakStats);

        return stats;
    }

    private Move getLastMove(List<Move> moves) {
        return moves.isEmpty() ? null : moves.get(moves.size() - 1);
    }

    private Period getMovePeriod(int currentMoveNumber, double totalMoves) {
        double ratio = currentMoveNumber / totalMoves;

        if (ratio < 0.33) return Period.EARLY;
        if (ratio < 0.66) return Period.MIDDLE;
        return Period.LATE;
    }
}