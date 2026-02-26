package com.flink.streaming.functions;
import com.flink.streaming.dtos.*;
import org.apache.commons.collections.KeyValue;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PlayerStatsReducer implements ReduceFunction<PlayerStats> {
    @Override
    public PlayerStats reduce(PlayerStats s1, PlayerStats s2) throws Exception {
        if (s1.getIsDefault() && s2.getIsDefault()) {
            return s1;
        }

        if (s1.getIsDefault()) return s2;
        if (s2.getIsDefault()) return s1;
        PlayerStats reducedPlayerStats = new PlayerStats();
        reducedPlayerStats.setPlayerName(s1.getPlayerName());
        reducedPlayerStats.setGamesWon(s1.getGamesWon()+ s2.getGamesWon());
        reducedPlayerStats.setGamesPlayed(s1.getGamesPlayed() + s2.getGamesPlayed());
        List<CastlingType> combinedTypes = new ArrayList<>();
        CastlingStats cs1 = s1.getCastlingStats();
        CastlingStats cs2 = s2.getCastlingStats();

        CastlingStats reducedCastling = new CastlingStats();

        int castlingPlayed = s1.getCastlingStats().getCastlingPlayed() + s2.getCastlingStats().getCastlingPlayed();

        reducedCastling.setCastlingPlayed(castlingPlayed);
        if (cs1 != null && cs1.getCastlingTypeList() != null) {
            combinedTypes.addAll(cs1.getCastlingTypeList());
        }
        if (cs2 != null && cs2.getCastlingTypeList() != null) {
            combinedTypes.addAll(cs2.getCastlingTypeList());
        }

        reducedCastling.setCastlingTypeList(combinedTypes);
        reducedPlayerStats.setCastlingStats(reducedCastling);

        List<GameCheckStats> combinedChecks = new ArrayList<>();

        if (s1.getCheckStats() != null) {
            combinedChecks.addAll(s1.getCheckStats());
        }
        if (s2.getCheckStats() != null) {
            combinedChecks.addAll(s2.getCheckStats());
        }
        reducedPlayerStats.setCheckStats(combinedChecks);
       reducedPlayerStats.setStreakStats(reduceStreakStats(s1,s2));
       return reducedPlayerStats;
    }
    private StreakStats  reduceStreakStats(PlayerStats s1, PlayerStats s2) {
        StreakStats reducedStreakStats = new StreakStats();
        Streak st1 = s1.getStreakStats().getCurrentStreak();
        Streak st2 = s2.getStreakStats().getCurrentStreak();

        if (st1 == null) return s2.getStreakStats();
        if (st2 == null) return s1.getStreakStats();

        Streak newer = st1.getLastTimeStamp() >= st2.getLastTimeStamp() ? st1 : st2;
        Streak older = (newer == st1) ? st2 : st1;

        long latestStreakChange = Math.max(st1.getLastGameTimestampFromLastDifferentStreak(), st2.getLastGameTimestampFromLastDifferentStreak());

        Streak combined = new Streak();
        if (st1.getStreakResult() == st2.getStreakResult()) {
            combined.setStreakResult(newer.getStreakResult());
            for(Long ts : st1.getGameTimeStamps())
            {
                if(ts> latestStreakChange)
                {
                    combined.getGameTimeStamps().add(ts);
                    combined.setStreakNumber(combined.getStreakNumber() + 1);
                }
            }
            for(Long ts : st2.getGameTimeStamps())
            {
                if(ts> latestStreakChange)
                {
                    combined.getGameTimeStamps().add(ts);
                    combined.setStreakNumber(combined.getStreakNumber() + 1);
                }
            }
            combined.setLastGameTimestampFromLastDifferentStreak(latestStreakChange);
        } else {
            combined.setStreakResult(newer.getStreakResult());
            for(long ts : newer.getGameTimeStamps())
            {
                if (ts> older.getLastTimeStamp()){
                    combined.getGameTimeStamps().add(ts);
                    combined.setStreakNumber(combined.getStreakNumber() + 1);
                }
            }
            combined.setLastGameTimestampFromLastDifferentStreak(older.getLastTimeStamp());
        }

        reducedStreakStats.setCurrentStreak(combined);

        return reducedStreakStats;
    }
}