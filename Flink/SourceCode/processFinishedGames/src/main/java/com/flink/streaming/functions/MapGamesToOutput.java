package com.flink.streaming.functions;

import com.flink.streaming.dtos.*;
import com.flink.streaming.dtos.PlayerStats;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapGamesToOutput implements MapFunction<PlayerStats, PlayerStatsOutput> {

    @Override
    public PlayerStatsOutput map(PlayerStats playerStats) throws Exception {

        PlayerStatsOutput playerStatsOutput = new PlayerStatsOutput();

        if (playerStats.getIsDefault()) {
            playerStatsOutput.setDefault(true);
            return playerStatsOutput;
        }

        playerStatsOutput.setPlayerName(playerStats.getPlayerName());
        playerStatsOutput.setTotalGamesPlayed(playerStats.getGamesPlayed());
        playerStatsOutput.setTotalWins(playerStats.getGamesWon());
        playerStatsOutput.setWinPercentage(playerStats.getGamesPlayed() > 0
                ? (double) playerStats.getGamesWon() / playerStats.getGamesPlayed() * 100
                : 0.0);

        CastlingStatsOutput castlingStatsOutput = mapCastlingStats(playerStats);
        StreakStatsOutput streakStatsOutput = mapStreakStats(playerStats);
        CheckStatsOutput checkStatsOutput = mapCheckStats(playerStats);


        playerStatsOutput.setStreakStats(streakStatsOutput);
        playerStatsOutput.setCheckStats(checkStatsOutput);
        playerStatsOutput.setCastlingStats(castlingStatsOutput);

        return playerStatsOutput;
    }
    private CheckStatsOutput mapCheckStats(PlayerStats playerStats) {
        CheckStatsOutput checkStatsOutput = new CheckStatsOutput();
        checkStatsOutput.setNumberOfChecks(playerStats.getNumberOfChecks());
        checkStatsOutput.setChecksPerGame(playerStats.getGamesPlayed() > 0 ? (double) playerStats.getNumberOfChecks() / playerStats.getGamesPlayed() : 0.0);
        Map<Integer, int[]> statsByChecks = new HashMap<>(); // [wins, losses, draws]

        for (GameCheckStats gcs : playerStats.getCheckStats()) {
            int checks = gcs.getNumberOfChecks();
            statsByChecks.putIfAbsent(checks, new int[3]);
            int[] arr = statsByChecks.get(checks);

            if (gcs.getResult() == Result.WON) {
                arr[0]++;
            } else if (gcs.getResult() == Result.LOST) {
                arr[1]++;
            } else if (gcs.getResult() == Result.DRAW) {
                arr[2]++;
            }
        }
        double maxWinPercentage = -1.0;
        double maxLosePercentage = -1.0;
        int bestChecksWinPerc = 0;
        int bestChecksLosePerc = 0;

        for (Map.Entry<Integer, int[]> entry : statsByChecks.entrySet()) {
            int checks = entry.getKey();
            int[] arr = entry.getValue();
            int total = arr[0] + arr[1] + arr[2];
            if (total == 0) continue;

            double winPerc = (double) arr[0] / total;
            double losePerc = (double) arr[1] / total;

            if (winPerc > maxWinPercentage) {
                maxWinPercentage = winPerc;
                bestChecksWinPerc = checks;
            }

            if (losePerc > maxLosePercentage) {
                maxLosePercentage = losePerc;
                bestChecksLosePerc = checks;
            }
        }

        checkStatsOutput.setNumberOfChecksWithMostWins(bestChecksWinPerc);
        checkStatsOutput.setNumberOfChecksWithMostLoses(bestChecksLosePerc);
        checkStatsOutput.setNumberOfChecksWinPercentage(maxWinPercentage * 100);
        checkStatsOutput.setNumberOfChecksLosePercentage(maxLosePercentage * 100);

        checkStatsOutput.setBestFigureCheckStat(getBestFigure(playerStats.getCheckStats(),checkStatsOutput.getNumberOfChecks()));
        checkStatsOutput.setBestPeriodCheckStat(getBestPeriod(playerStats.getCheckStats(),checkStatsOutput.getNumberOfChecks()));

        return checkStatsOutput;
    }
    private PeriodCheckStatOutput getBestPeriod(List<GameCheckStats> checkStats, int totalNumberOfChecks) {
        PeriodCheckStatOutput bestPeriodCheckStatOutput = new PeriodCheckStatOutput();

        Map<Period, int[]> periodStats = new HashMap<>(); // [totalChecks, wins]

        for (GameCheckStats gcs : checkStats) {
            for (CheckStat cs : gcs.getCheckStatList()) {
                Period period = cs.getCheckPeriod();
                periodStats.putIfAbsent(period, new int[2]);
                int[] stats = periodStats.get(period);

                stats[0] += cs.getNumberOfChecks();
                if (gcs.getResult() == Result.WON) stats[1] += cs.getNumberOfChecks();
            }
        }

        Period bestPeriod = null;
        int bestTotal = 0;
        double bestScore = -1.0;

        for (Map.Entry<Period, int[]> entry : periodStats.entrySet()) {
            int totalChecks = entry.getValue()[0];
            int wins = entry.getValue()[1];

            double score = wins * 0.7 + totalChecks * 0.3;

            if (score > bestScore) {
                bestScore = score;
                bestPeriod = entry.getKey();
                bestTotal = totalChecks;
            }
        }

        if (bestPeriod != null) {
            bestPeriodCheckStatOutput.setPeriod(bestPeriod);
            bestPeriodCheckStatOutput.setNumberOfChecks(bestTotal);
            bestPeriodCheckStatOutput.setCheckPercentage(totalNumberOfChecks > 0 ? (double) bestTotal / totalNumberOfChecks * 100 : 0.0);
            bestPeriodCheckStatOutput.setNumberOfWins(periodStats.get(bestPeriod)[1]);
            bestPeriodCheckStatOutput.setWinPercentage(bestTotal > 0 ? (double) periodStats.get(bestPeriod)[1] / bestTotal * 100 : 0.0);
        }

        return bestPeriodCheckStatOutput;
    }
    private FigureCheckStatOutput getBestFigure(List<GameCheckStats> checkStats,int totalNumberOfChecks) {
        FigureCheckStatOutput  bestFigureCheckStatOutput = new FigureCheckStatOutput();

        Map<Figure, int[]> figureStats = new HashMap<>(); // [totalChecks, wins]
        //NE POSTOJI NI JEDAN FIGURE STAT U MAPI???
        for (GameCheckStats gcs : checkStats) {
            for (CheckStat cs : gcs.getCheckStatList()) {
                for (FigureCheckStat fcs : cs.getFigureCheckStatList()) {
                    figureStats.putIfAbsent(fcs.getFigure(), new int[2]);
                    int[] stats = figureStats.get(fcs.getFigure());
                    stats[0] += fcs.getNumberOfChecks();
                    if (gcs.getResult() == Result.WON) stats[1] += fcs.getNumberOfChecks();
                }
            }
        }

        Figure bestFigure = null;
        int bestTotal = 0;
        double bestScore = -1.0;

        for (Map.Entry<Figure, int[]> entry : figureStats.entrySet()) {
            int totalChecks = entry.getValue()[0];
            int wins = entry.getValue()[1];
            double score = ((double) wins / totalChecks) * totalChecks + 0.3 * totalChecks;
            if (score > bestScore) {
                bestScore = score;
                bestFigure = entry.getKey();
                bestTotal = totalChecks;
            }
        }
        if(bestFigure != null) {
            bestFigureCheckStatOutput.setFigure(bestFigure);
            bestFigureCheckStatOutput.setNumberOfChecks(bestTotal);
            bestFigureCheckStatOutput.setScore(bestScore);
            bestFigureCheckStatOutput.setCheckPercentage(totalNumberOfChecks > 0 ? (double) bestTotal / totalNumberOfChecks  * 100 : 0.0);
            bestFigureCheckStatOutput.setNumberOfWins(figureStats.get(bestFigure)[1]);
            bestFigureCheckStatOutput.setWinPercentage(bestTotal > 0 ? (double) figureStats.get(bestFigure)[1] / bestTotal * 100 : 0.0);
        }

        return bestFigureCheckStatOutput;
    }
    private StreakStatsOutput mapStreakStats(PlayerStats playerStats) {
        StreakStatsOutput streakStatsOutput = new StreakStatsOutput();
        StreakOutput currentStreakOutput = new StreakOutput();
        StreakOutput streakWithHighestWinProbability = new StreakOutput();
        StreakOutput streakWithHighestLoseProbability = new StreakOutput();
        StreakOutput streakWithHighestDrawProbability = new StreakOutput();
        currentStreakOutput.setStreakNumber(playerStats.getStreakStats().getCurrentStreak().getStreakNumber());
        currentStreakOutput.setStreakResult(playerStats.getStreakStats().getCurrentStreak().getStreakResult());
        StreakStat currentStreakStat = playerStats.getStreakStats().getCurrentStreakStat();

        if(currentStreakStat != null){
            int totalNextGames = currentStreakStat.getNumberOfNextGamesWon() + currentStreakStat.getNumberOfNextGamesLost() + currentStreakStat.getNumberOfNextGamesDraw();
            currentStreakOutput.setNextGameWinPercentage(totalNextGames > 0
                    ? (double) currentStreakStat.getNumberOfNextGamesWon() / totalNextGames * 100 : 0.0);
            currentStreakOutput.setNextGameLosePercentage(totalNextGames > 0
                    ? (double) currentStreakStat.getNumberOfNextGamesLost() / totalNextGames * 100 : 0.0);
            currentStreakOutput.setNextGameDrawPercentage(totalNextGames > 0
                    ? (double) currentStreakStat.getNumberOfNextGamesDraw() / totalNextGames * 100 : 0.0);
        }
        streakStatsOutput.setCurrentStreak(currentStreakOutput);

        Streak maxWinStreak = null;
        Streak maxLossStreak = null;
        Streak maxDrawStreak = null;

        double maxWin = -1.0;
        double maxLoss = -1.0;
        double maxDraw = -1.0;

        for (StreakStat ss : playerStats.getStreakStats().getStreakStatList()) {
            int totalNextGames = ss.getNumberOfNextGamesWon() + ss.getNumberOfNextGamesLost() + ss.getNumberOfNextGamesDraw();
            if (totalNextGames == 0) continue;

            double winProb = (double) ss.getNumberOfNextGamesWon() / totalNextGames;
            double lossProb = (double) ss.getNumberOfNextGamesLost() / totalNextGames;
            double drawProb = (double) ss.getNumberOfNextGamesDraw() / totalNextGames;

            if (winProb > maxWin) {
                maxWin = winProb;
                maxWinStreak = ss.getStreak();
            }

            if (lossProb > maxLoss) {
                maxLoss = lossProb;
                maxLossStreak = ss.getStreak();
            }

            if (drawProb > maxDraw) {
                maxDraw = drawProb;
                maxDrawStreak = ss.getStreak();
            }
        }
        streakWithHighestWinProbability.setStreakNumber(maxWinStreak != null ? maxWinStreak.getStreakNumber() : 0);
        streakWithHighestWinProbability.setStreakResult(maxWinStreak != null ? maxWinStreak.getStreakResult() : null);
        streakWithHighestWinProbability.setNextGameWinPercentage(maxWin >= 0 ? maxWin * 100 : 0.0);
        streakStatsOutput.setStreakWithHighestWinProbability(streakWithHighestWinProbability);
        streakWithHighestLoseProbability.setStreakNumber(maxLossStreak != null ? maxLossStreak.getStreakNumber() : 0);
        streakWithHighestLoseProbability.setStreakResult(maxLossStreak != null ? maxLossStreak.getStreakResult() : null);
        streakWithHighestLoseProbability.setNextGameLosePercentage(maxLoss >= 0 ? maxLoss * 100 : 0.0);
        streakStatsOutput.setStreakWithHighestLoseProbability(streakWithHighestLoseProbability);
        streakWithHighestDrawProbability.setStreakNumber(maxDrawStreak != null ? maxDrawStreak.getStreakNumber() : 0);
        streakWithHighestDrawProbability.setStreakResult(maxDrawStreak != null ? maxDrawStreak.getStreakResult() : null);
        streakWithHighestDrawProbability.setNextGameDrawPercentage(maxDraw >= 0 ? maxDraw * 100 : 0.0);
        streakStatsOutput.setStreakWithHighestDrawProbability(streakWithHighestDrawProbability);
        return  streakStatsOutput;
    }
    private CastlingStatsOutput mapCastlingStats(PlayerStats playerStats) throws Exception {
        CastlingStatsOutput castlingStatsOutput = new CastlingStatsOutput();
        CastlingStats castlingStats = playerStats.getCastlingStats();

        List<CastlingType> list = castlingStats != null && castlingStats.getCastlingTypeList() != null
                ? castlingStats.getCastlingTypeList()
                : Collections.emptyList();

        int totalCastling = 0, totalWins = 0, totalLosses = 0, totalDraws = 0;
        int shortPlayed = 0, shortWins = 0, shortLosses = 0, shortDraws = 0;
        int longPlayed = 0, longWins = 0, longLosses = 0, longDraws = 0;
        int shortEarly = 0, shortMiddle = 0, shortLate = 0;
        int shortEarlyWins = 0, shortEarlyLosses = 0, shortEarlyDraws = 0;
        int longEarly = 0, longMiddle = 0, longLate = 0;
        int longEarlyWins = 0, longEarlyLosses = 0, longEarlyDraws = 0;
        int shortMiddleWins = 0, shortMiddleLosses = 0, shortMiddleDraws = 0;
        int shortLateWins = 0, shortLateLosses = 0, shortLateDraws = 0;
        int longMiddleWins = 0, longMiddleLosses = 0, longMiddleDraws = 0;
        int longLateWins = 0, longLateLosses = 0, longLateDraws = 0;

        for (CastlingType ct : list) {

            totalCastling++;

            if(ct.getResult() == Result.DRAW){
                totalDraws++;
            }
            else if (ct.getResult() == Result.WON){
                totalWins++;
            }
            else {
                totalLosses++;
            }

            boolean isShort = ct.getCastlingType() == Type.CASTLING_SHORT;
            boolean isLong = ct.getCastlingType() == Type.CASTLING_LONG;

            boolean isEarly = ct.getPeriodPlayed() == Period.EARLY;
            boolean isMiddle = ct.getPeriodPlayed() == Period.MIDDLE;
            boolean isLate = ct.getPeriodPlayed() == Period.LATE;

            if (isShort) {
                shortPlayed++;
                if (ct.getResult() == Result.DRAW) {
                    shortDraws++;
                } else if (ct.getResult() == Result.WON) {
                    shortWins++;
                } else {
                    shortLosses++;
                }
                if (isEarly) {
                    shortEarly++;
                    if (ct.getResult() == Result.WON) {
                        shortEarlyWins++;
                    } else if (ct.getResult() == Result.LOST) {
                        shortEarlyLosses++;
                    } else {
                        shortEarlyDraws++;
                    }
                } else if (isMiddle) {
                    shortMiddle++;
                    if (ct.getResult() == Result.WON) shortMiddleWins++;
                    else if (ct.getResult() == Result.LOST) shortMiddleLosses++;
                    else shortMiddleDraws++;
                }
                else if (isLate) {
                    shortLate++;
                    if (ct.getResult() == Result.WON) shortLateWins++;
                    else if (ct.getResult() == Result.LOST) shortLateLosses++;
                    else shortLateDraws++;
                }
            }

            if (isLong) {
                longPlayed++;
                if (ct.getResult() == Result.DRAW) {
                    longDraws++;
                } else if (ct.getResult() == Result.WON) {
                    longWins++;
                } else {
                    longLosses++;
                }
                if (isEarly) {
                    longEarly++;
                    if (ct.getResult() == Result.WON) {
                        longEarlyWins++;
                    } else if (ct.getResult() == Result.LOST) {
                        longEarlyLosses++;
                    } else {
                        longEarlyDraws++;
                    }
                } else if (isMiddle) {
                    longMiddle++;
                    if (ct.getResult() == Result.WON) longMiddleWins++;
                    else if (ct.getResult() == Result.LOST) longMiddleLosses++;
                    else longMiddleDraws++;
                }
                else if (isLate) {
                    longLate++;
                    if (ct.getResult() == Result.WON) longLateWins++;
                    else if (ct.getResult() == Result.LOST) longLateLosses++;
                    else longLateDraws++;
                }
            }
        }

        castlingStatsOutput.setCastlingPlayed(totalCastling);
        castlingStatsOutput.setCastlingPlayedPercent(playerStats.getGamesPlayed() > 0
                ? (double) totalCastling / playerStats.getGamesPlayed() * 100 : 0.0);
        castlingStatsOutput.setCastlingWinPercentage(totalCastling > 0
                ? (double) totalWins / totalCastling * 100 : 0.0);
        castlingStatsOutput.setCastlingLossPercentage(totalCastling > 0
                ? (double) totalLosses / totalCastling * 100 : 0.0);
        castlingStatsOutput.setCastlingDrawPercentage(totalCastling > 0
                ? (double) totalDraws / totalCastling * 100 : 0.0);

        CastlingTypeStats shortStats = new CastlingTypeStats();
        shortStats.setCastlingPlayed(shortPlayed);
        shortStats.setCastlingPlayedPercentage(totalCastling > 0
                ? (double) shortPlayed / totalCastling * 100 : 0.0);
        shortStats.setCastlingWonPercentage(shortPlayed > 0
                ? (double) shortWins / shortPlayed * 100 : 0.0);
        shortStats.setCastlingLossPercentage(shortPlayed > 0
                ? (double) shortLosses / shortPlayed * 100 : 0.0);
        shortStats.setCastlingDrawPercentage(shortPlayed > 0
                ? (double) shortDraws / shortPlayed * 100 : 0.0);

        CastlingTypeStats longStats = new CastlingTypeStats();
        longStats.setCastlingPlayed(longPlayed);
        longStats.setCastlingPlayedPercentage(totalCastling > 0
                ? (double) longPlayed / totalCastling * 100 : 0.0);
        longStats.setCastlingWonPercentage(longPlayed > 0
                ? (double) longWins / longPlayed * 100 : 0.0);
        longStats.setCastlingLossPercentage(longPlayed > 0
                ? (double) longLosses / longPlayed * 100 : 0.0);
        longStats.setCastlingDrawPercentage(longPlayed > 0
                ? (double) longDraws / longPlayed * 100 : 0.0);

        CastlingPeriodStatsOutput longEarlyStats = new CastlingPeriodStatsOutput();
        longEarlyStats.setCastlingPlayed(longEarly);
        longEarlyStats.setCastlingPlayedPercentage(longPlayed > 0 ? (double) longEarly / longPlayed * 100 : 0.0);
        longEarlyStats.setCastlingPeriodWonPercentage(longEarly > 0 ? (double) longEarlyWins / longEarly * 100 : 0.0);
        longEarlyStats.setCastlingLostPercentage(longEarly > 0 ? (double) longEarlyLosses / longEarly * 100 : 0.0);
        longEarlyStats.setCastlingDrawPercentage(longEarly > 0 ? (double) longEarlyDraws / longEarly * 100 : 0.0);

        CastlingPeriodStatsOutput longMiddleStats = new CastlingPeriodStatsOutput();
        longMiddleStats.setCastlingPlayed(longMiddle);
        longMiddleStats.setCastlingPlayedPercentage(longPlayed > 0 ? (double) longMiddle / longPlayed * 100 : 0.0);
        longMiddleStats.setCastlingPeriodWonPercentage(longMiddle > 0 ? (double) longMiddleWins / longMiddle * 100 : 0.0);
        longMiddleStats.setCastlingLostPercentage(longMiddle > 0 ? (double) longMiddleLosses / longMiddle * 100 : 0.0);
        longMiddleStats.setCastlingDrawPercentage(longMiddle > 0 ? (double) longMiddleDraws / longMiddle * 100 : 0.0);

        CastlingPeriodStatsOutput longLateStats = new CastlingPeriodStatsOutput();
        longLateStats.setCastlingPlayed(longLate);
        longLateStats.setCastlingPlayedPercentage(longPlayed > 0 ? (double) longLate / longPlayed * 100 : 0.0);
        longLateStats.setCastlingPeriodWonPercentage(longLate > 0 ? (double) longLateWins / longLate * 100 : 0.0);
        longLateStats.setCastlingLostPercentage(longLate > 0 ? (double) longLateLosses / longLate * 100 : 0.0);
        longLateStats.setCastlingDrawPercentage(longLate > 0 ? (double) longLateDraws / longLate * 100 : 0.0);

        longStats.setEarlyPeriodStats(longEarlyStats);
        longStats.setMiddlePeriodStats(longMiddleStats);
        longStats.setLatePeriodStats(longLateStats);

        CastlingPeriodStatsOutput shortEarlyStats = new CastlingPeriodStatsOutput();
        shortEarlyStats.setCastlingPlayed(shortEarly);
        shortEarlyStats.setCastlingPlayedPercentage(shortPlayed > 0 ? (double) shortEarly / shortPlayed * 100 : 0.0);
        shortEarlyStats.setCastlingPeriodWonPercentage(shortEarly > 0 ? (double) shortEarlyWins / shortEarly * 100 : 0.0);
        shortEarlyStats.setCastlingLostPercentage(shortEarly > 0 ? (double) shortEarlyLosses / shortEarly * 100 : 0.0);
        shortEarlyStats.setCastlingDrawPercentage(shortEarly > 0 ? (double) shortEarlyDraws / shortEarly * 100 : 0.0);

        CastlingPeriodStatsOutput shortMiddleStats = new CastlingPeriodStatsOutput();
        shortMiddleStats.setCastlingPlayed(shortMiddle);
        shortMiddleStats.setCastlingPlayedPercentage(shortPlayed > 0 ? (double) shortMiddle / shortPlayed * 100 : 0.0);
        shortMiddleStats.setCastlingPeriodWonPercentage(shortMiddle > 0 ? (double) shortMiddleWins / shortMiddle * 100 : 0.0);
        shortMiddleStats.setCastlingLostPercentage(shortMiddle > 0 ? (double) shortMiddleLosses / shortMiddle * 100 : 0.0);
        shortMiddleStats.setCastlingDrawPercentage(shortMiddle > 0 ? (double) shortMiddleDraws / shortMiddle * 100 : 0.0);

        CastlingPeriodStatsOutput shortLateStats = new CastlingPeriodStatsOutput();
        shortLateStats.setCastlingPlayed(shortLate);
        shortLateStats.setCastlingPlayedPercentage(shortPlayed > 0 ? (double) shortLate / shortPlayed * 100 : 0.0);
        shortLateStats.setCastlingPeriodWonPercentage(shortLate > 0 ? (double) shortLateWins / shortLate * 100 : 0.0);
        shortLateStats.setCastlingLostPercentage(shortLate > 0 ? (double) shortLateLosses / shortLate * 100 : 0.0);
        shortLateStats.setCastlingDrawPercentage(shortLate > 0 ? (double) shortLateDraws / shortLate * 100 : 0.0);

        shortStats.setEarlyPeriodStats(shortEarlyStats);
        shortStats.setMiddlePeriodStats(shortMiddleStats);
        shortStats.setLatePeriodStats(shortLateStats);

        castlingStatsOutput.setShortCastlingStats(shortStats);
        castlingStatsOutput.setLongCastlingStats(longStats);
        return castlingStatsOutput;
    }
}