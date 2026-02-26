package com.flink.streaming.deserialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flink.streaming.models.GameEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.IOException;

public class GameEventDeserializationSchema implements DeserializationSchema<GameEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public GameEvent deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, GameEvent.class);
    }

    @Override
    public boolean isEndOfStream(GameEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<GameEvent> getProducedType() {
        return TypeInformation.of(GameEvent.class);
    }
}
