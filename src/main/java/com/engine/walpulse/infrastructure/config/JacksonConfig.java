package com.engine.walpulse.infrastructure.config;

import com.engine.walpulse.domain.model.LsnPosition;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer lsnCustomizer() {
        return builder -> builder.serializerByType(LsnPosition.class, new JsonSerializer<LsnPosition>() {
            @Override
            public void serialize(LsnPosition lsn, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                if (lsn == null) {
                    gen.writeNull();
                } else {
                    gen.writeString(lsn.asString());
                }
            }
        });
    }
}
