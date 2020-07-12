package com.poc.kafka.calls.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.poc.kafka.calls.dto.CallReport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;
import java.util.Properties;

public class KafkaConsumerConfig {

    private static ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    private static JsonSerde<CallReport> jsonSerde() {
        Map<String, Object> configs = ImmutableMap.of(
                JsonDeserializer.TRUSTED_PACKAGES, "*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, String.class,
                JsonDeserializer.KEY_DEFAULT_TYPE,  CallReport.class);

        JsonSerde<CallReport> jsonSerde = new JsonSerde<>(objectMapper());
        jsonSerde.deserializer().configure(configs, true);

        return jsonSerde;
    }

    public static Properties producerProps(String bootstrapAddress, String groupId) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, jsonSerde().deserializer().getClass());
        props.put(JsonDeserializer.KEY_DEFAULT_TYPE,  CallReport.class);


        return props;
    }
}

