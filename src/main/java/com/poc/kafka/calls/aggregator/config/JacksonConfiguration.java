package com.poc.kafka.calls.aggregator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.poc.kafka.calls.dto.CallReport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

@Configuration
public class JacksonConfiguration {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public JsonSerde<CallReport> jsonSerde(ObjectMapper objectMapper) {
        Map<String, Object> configs = ImmutableMap.of(
                JsonDeserializer.TRUSTED_PACKAGES, "*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, String.class,
                JsonDeserializer.KEY_DEFAULT_TYPE,  CallReport.class);

        JsonSerde<CallReport> jsonSerde = new JsonSerde<>(objectMapper);
        jsonSerde.deserializer().configure(configs, true);

        return jsonSerde;
    }
}
