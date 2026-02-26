package com.flink.streaming;

import com.flink.streaming.dtos.*;
import com.flink.streaming.dtos.playergamedtos.PlayerGameEvent;
import com.flink.streaming.functions.MapGamesToOutput;
import com.flink.streaming.functions.PlayerGameEventExtractor;
import com.flink.streaming.functions.PlayerStatsReducer;
import com.flink.streaming.functions.ProcessGamesInWindow;
import com.flink.streaming.models.GameEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

import org.apache.flink.connector.mongodb.sink.MongoSink;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.bson.Document;
import com.flink.streaming.deserialization.GameEventDeserializationSchema;

public class ProcessFinishedGamesJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. DEFINICIJA KAFKA SOURCE-A
        KafkaSource<GameEvent> blitzSource = KafkaSource.<GameEvent>builder()
                .setBootstrapServers("kafka-broker1-1:9092")
                .setTopics("chess_finished_games_transformed_blitz")
                .setGroupId("flink-processing-finished-games-blitz-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GameEventDeserializationSchema())
                .build();
        KafkaSource<GameEvent> rapidSource = KafkaSource.<GameEvent>builder()
                .setBootstrapServers("kafka-broker1-1:9092")
                .setTopics("chess_finished_games_transformed_rapid")
                .setGroupId("flink-processing-finished-games-rapid-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GameEventDeserializationSchema())
                .build();
        KafkaSource<GameEvent> othersSource = KafkaSource.<GameEvent>builder()
                .setBootstrapServers("kafka-broker1-1:9092")
                .setTopics("chess_finished_games_transformed_others")
                .setGroupId("flink-processing-finished-games-others-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GameEventDeserializationSchema())
                .build();
        KafkaSource<GameEvent> bulletSource = KafkaSource.<GameEvent>builder()
                .setBootstrapServers("kafka-broker1-1:9092")
                .setTopics("chess_finished_games_transformed_bullet")
                .setGroupId("flink-processing-finished-games-bullet-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new GameEventDeserializationSchema())
                .build();

        //Kreiranje stream-a iz Kafka source-a
        DataStream<GameEvent> blitzStream = env.fromSource(blitzSource,
                WatermarkStrategy
                        .<GameEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp()), // koristi timestamp iz event-a
                "Blitz Kafka Source");

        DataStream<GameEvent> rapidStream = env.fromSource(rapidSource,
                WatermarkStrategy
                        .<GameEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                "Rapid Kafka Source");

        DataStream<GameEvent> bulletStream = env.fromSource(bulletSource,
                WatermarkStrategy
                        .<GameEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                "Bullet Kafka Source");

        DataStream<GameEvent> othersStream = env.fromSource(othersSource,
                WatermarkStrategy
                        .<GameEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                        .withTimestampAssigner((event, ts) -> event.getTimestamp()),
                "Others Kafka Source");
        // Mozda spojiti sa istorijskim podacima.
        // Fanout
        DataStream<PlayerGameEvent> blitzPlayerEvents = blitzStream.flatMap(new PlayerGameEventExtractor());
        DataStream<PlayerGameEvent> rapidPlayerEvents = rapidStream.flatMap(new PlayerGameEventExtractor());
        DataStream<PlayerGameEvent> bulletPlayerEvents = bulletStream.flatMap(new PlayerGameEventExtractor());
        DataStream<PlayerGameEvent> othersPlayerEvents = othersStream.flatMap(new PlayerGameEventExtractor());

        DataStream<PlayerStats> blitzPlayerEventsWindowed = blitzPlayerEvents.keyBy(gameEvent ->gameEvent.getPlayerName()).process(new ProcessGamesInWindow());
        DataStream<PlayerStats> rapidPlayerEventsWindowed = rapidPlayerEvents.keyBy(gameEvent ->gameEvent.getPlayerName()).process(new ProcessGamesInWindow());
        DataStream<PlayerStats> bulletPlayerEventsWindowed = bulletPlayerEvents.keyBy(gameEvent ->gameEvent.getPlayerName()).process(new ProcessGamesInWindow());
        DataStream<PlayerStats> othersPlayerEventsWindowed = othersPlayerEvents.keyBy(gameEvent ->gameEvent.getPlayerName()).process(new ProcessGamesInWindow());


        DataStream<PlayerStats> unifiedPlayerStats = blitzPlayerEventsWindowed.union(rapidPlayerEventsWindowed).union(bulletPlayerEventsWindowed).union(othersPlayerEventsWindowed);

        DataStream<PlayerStats> summedPlayerStatsStream = unifiedPlayerStats
                .keyBy(PlayerStats::getPlayerName)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce(new PlayerStatsReducer());

        DataStream<PlayerStatsOutput> blitzPlayerEventsOutput = blitzPlayerEventsWindowed.map(new MapGamesToOutput());
        DataStream<PlayerStatsOutput> rapidPlayerEventsOutput = rapidPlayerEventsWindowed.map(new MapGamesToOutput());
        DataStream<PlayerStatsOutput> bulletPlayerEventsOutput = bulletPlayerEventsWindowed.map(new MapGamesToOutput());
        DataStream<PlayerStatsOutput> othersPlayerEventsOutput = othersPlayerEventsWindowed.map(new MapGamesToOutput());
        DataStream<PlayerStatsOutput> summedPlayerEventsOutput = summedPlayerStatsStream.map(new MapGamesToOutput());













        // 4. DEFINICIJA SINK-A
        // Sink za transformisane poteze

       MongoSink<PlayerStatsOutput> mongoOthersSink = MongoSink.<PlayerStatsOutput>builder()
        .setUri("mongodb://admin:admin@mongodb:27017/?authSource=admin")
        .setDatabase("PlayerStats")
        .setCollection("player_stats_past_7_days")
        .setBatchSize(1000)
        .setBatchIntervalMs(1000)
        .setMaxRetries(3)
        .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
        .setSerializationSchema((event, context) -> {

            Document doc = generateMongoDocument(event);

            Document filter = new Document("playerName", event.getPlayerName());
            Document update = new Document("$set",new Document("othersStats",doc));
            return new com.mongodb.client.model.UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));

        })
        .build();
        MongoSink<PlayerStatsOutput> mongoBlitzSink = MongoSink.<PlayerStatsOutput>builder()
                .setUri("mongodb://admin:admin@mongodb:27017/?authSource=admin")
                .setDatabase("PlayerStats")
                .setCollection("player_stats_past_7_days")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema((event, context) -> {

                    Document doc = generateMongoDocument(event);

                    Document filter = new Document("playerName", event.getPlayerName());
                    Document update = new Document("$set",new Document("blitzStats",doc));
                    return new com.mongodb.client.model.UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));

                })
                .build();
        MongoSink<PlayerStatsOutput> mongoBulletSink = MongoSink.<PlayerStatsOutput>builder()
                .setUri("mongodb://admin:admin@mongodb:27017/?authSource=admin")
                .setDatabase("PlayerStats")
                .setCollection("player_stats_past_7_days")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema((event, context) -> {

                    Document doc = generateMongoDocument(event);

                    Document filter = new Document("playerName", event.getPlayerName());
                    Document update = new Document("$set",new Document("bulletStats",doc));
                    return new com.mongodb.client.model.UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));

                })
                .build();
        MongoSink<PlayerStatsOutput> mongoRapidSink = MongoSink.<PlayerStatsOutput>builder()
                .setUri("mongodb://admin:admin@mongodb:27017/?authSource=admin")
                .setDatabase("PlayerStats")
                .setCollection("player_stats_past_7_days")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema((event, context) -> {

                    Document doc = generateMongoDocument(event);

                    Document filter = new Document("playerName", event.getPlayerName());
                    Document update = new Document("$set",new Document("rapidStats",doc));
                    return new com.mongodb.client.model.UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));

                })
                .build();
        MongoSink<PlayerStatsOutput> mongoSummedSink = MongoSink.<PlayerStatsOutput>builder()
                .setUri("mongodb://admin:admin@mongodb:27017/?authSource=admin")
                .setDatabase("PlayerStats")
                .setCollection("player_stats_past_7_days")
                .setBatchSize(1000)
                .setBatchIntervalMs(1000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema((event, context) -> {

                    Document doc = generateMongoDocument(event);

                    Document filter = new Document("playerName", event.getPlayerName());
                    Document update = new Document("$set",new Document("summedStats",doc));
                    return new com.mongodb.client.model.UpdateOneModel<>(filter, update, new UpdateOptions().upsert(true));

                })
                .build();

        blitzPlayerEventsOutput.sinkTo(mongoBlitzSink);
        rapidPlayerEventsOutput.sinkTo(mongoRapidSink);
        bulletPlayerEventsOutput.sinkTo(mongoBulletSink);
        othersPlayerEventsOutput.sinkTo(mongoOthersSink);
        summedPlayerEventsOutput.sinkTo(mongoSummedSink);

        env.execute("Kafka to Mongo process finished games job");
    }

    private static Document generateMongoDocument(PlayerStatsOutput event) {
        if (event.isDefault()) {
            return new Document();
        }

        Document doc = new Document();
        doc.append("playerName", event.getPlayerName());
        doc.append("totalGamesPlayed", event.getTotalGamesPlayed());
        doc.append("totalWins", event.getTotalWins());
        doc.append("winPercentage", event.getWinPercentage());

        CastlingStatsOutput castling = event.getCastlingStats();
        if (castling != null) {
            Document castlingDoc = new Document();
            castlingDoc.append("castlingPlayed", castling.getCastlingPlayed());
            castlingDoc.append("castlingPlayedPercent", castling.getCastlingPlayedPercent());
            castlingDoc.append("castlingWinPercentage", castling.getCastlingWinPercentage());
            castlingDoc.append("castlingLossPercentage", castling.getCastlingLossPercentage());
            castlingDoc.append("castlingDrawPercentage", castling.getCastlingDrawPercentage());

            if (castling.getShortCastlingStats() != null) {
                castlingDoc.append("shortCastlingStats", generateCastlingTypeDoc(castling.getShortCastlingStats()));
            }

            if (castling.getLongCastlingStats() != null) {
                castlingDoc.append("longCastlingStats", generateCastlingTypeDoc(castling.getLongCastlingStats()));
            }

            doc.append("castlingStats", castlingDoc);
        }

        StreakStatsOutput streakStats = event.getStreakStats();
        if (streakStats != null) {
            Document streakDoc = new Document();
            streakDoc.append("currentStreak", generateStreakOutputDoc(streakStats.getCurrentStreak()));
            streakDoc.append("streakWithHighestWinProbability", generateStreakOutputDoc(streakStats.getStreakWithHighestWinProbability()));
            streakDoc.append("streakWithHighestLoseProbability", generateStreakOutputDoc(streakStats.getStreakWithHighestLoseProbability()));
            streakDoc.append("streakWithHighestDrawProbability", generateStreakOutputDoc(streakStats.getStreakWithHighestDrawProbability()));
            doc.append("streakStats", streakDoc);
        }

        CheckStatsOutput checkStats = event.getCheckStats();
        if (checkStats != null) {
            Document checkDoc = new Document();
            checkDoc.append("numberOfChecks", checkStats.getNumberOfChecks());
            checkDoc.append("checksPerGame", checkStats.getChecksPerGame());
            checkDoc.append("numberOfChecksWithMostWins", checkStats.getNumberOfChecksWithMostWins());
            checkDoc.append("numberOfChecksWithMostLoses", checkStats.getNumberOfChecksWithMostLoses());
            checkDoc.append("numberOfChecksWinPercentage", checkStats.getNumberOfChecksWinPercentage());
            checkDoc.append("numberOfChecksLosePercentage", checkStats.getNumberOfChecksLosePercentage());

            if (checkStats.getBestFigureCheckStat() != null) {
                checkDoc.append("bestFigureCheckStat", generateFigureCheckDoc(checkStats.getBestFigureCheckStat()));
            }
            if (checkStats.getBestPeriodCheckStat() != null) {
                checkDoc.append("bestPeriodCheckStat", generatePeriodCheckDoc(checkStats.getBestPeriodCheckStat()));
            }

            doc.append("checkStats", checkDoc);
        }

        return doc;
    }


    private static Document generateCastlingTypeDoc(CastlingTypeStats stats) {
        Document doc = new Document();
        doc.append("castlingPlayed", stats.getCastlingPlayed());
        doc.append("castlingPlayedPercentage", stats.getCastlingPlayedPercentage());
        doc.append("castlingWonPercentage", stats.getCastlingWonPercentage());
        doc.append("castlingDrawPercentage", stats.getCastlingDrawPercentage());
        doc.append("castlingLossPercentage", stats.getCastlingLossPercentage());

        if (stats.getEarlyPeriodStats() != null)
            doc.append("earlyPeriodStats", generatePeriodCastlingDoc(stats.getEarlyPeriodStats()));
        if (stats.getMiddlePeriodStats() != null)
            doc.append("middlePeriodStats", generatePeriodCastlingDoc(stats.getMiddlePeriodStats()));
        if (stats.getLatePeriodStats() != null)
            doc.append("latePeriodStats", generatePeriodCastlingDoc(stats.getLatePeriodStats()));

        return doc;
    }

    private static Document generatePeriodCastlingDoc(CastlingPeriodStatsOutput periodStats) {
        Document doc = new Document();
        doc.append("castlingPlayed", periodStats.getCastlingPlayed());
        doc.append("castlingPlayedPercentage", periodStats.getCastlingPlayedPercentage());
        doc.append("castlingPeriodWonPercentage", periodStats.getCastlingPeriodWonPercentage());
        doc.append("castlingDrawPercentage", periodStats.getCastlingDrawPercentage());
        doc.append("castlingLostPercentage", periodStats.getCastlingLostPercentage());
        return doc;
    }

    private static Document generateStreakOutputDoc(StreakOutput streak) {
        if (streak == null) return null;
        Document doc = new Document();
        doc.append("streakNumber", streak.getStreakNumber());
        doc.append("streakResult", streak.getStreakResult() != null ? streak.getStreakResult().name() : null);
        doc.append("nextGameWinPercentage", streak.getNextGameWinPercentage());
        doc.append("nextGameLosePercentage", streak.getNextGameLosePercentage());
        doc.append("nextGameDrawPercentage", streak.getNextGameDrawPercentage());
        return doc;
    }

    private static Document generateFigureCheckDoc(FigureCheckStatOutput fcs) {
        if (fcs == null) return null;
        Document doc = new Document();
        doc.append("figure", fcs.getFigure() != null ? fcs.getFigure().name() : null);
        doc.append("numberOfChecks", fcs.getNumberOfChecks());
        doc.append("score", fcs.getScore());
        doc.append("checkPercentage", fcs.getCheckPercentage());
        doc.append("numberOfWins", fcs.getNumberOfWins());
        doc.append("winPercentage", fcs.getWinPercentage());
        return doc;
    }

    private static Document generatePeriodCheckDoc(PeriodCheckStatOutput pcs) {
        if (pcs == null) return null;
        Document doc = new Document();
        doc.append("period", pcs.getPeriod() != null ? pcs.getPeriod().name() : null);
        doc.append("numberOfChecks", pcs.getNumberOfChecks());
        doc.append("checkPercentage", pcs.getCheckPercentage());
        doc.append("numberOfWins", pcs.getNumberOfWins());
        doc.append("winPercentage", pcs.getWinPercentage());
        return doc;
    }
}
