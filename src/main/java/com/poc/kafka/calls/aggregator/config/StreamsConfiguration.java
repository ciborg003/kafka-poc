package com.poc.kafka.calls.aggregator.config;
import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Properties;
import java.util.UUID;

import static com.poc.kafka.calls.config.KafkaConfigs.CALL_REPORT_TOPIC;

@Configuration
public class StreamsConfiguration {

    public static final String AGGREGATED_CALL_REPORT_TOPIC_STORE = "aggregated-call-report";
    public static final String WINDOWED_CALL_REPORT_TOPIC_STORE = "windowed-call-report";

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    public Properties streamsConfiguration(String applicationId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JsonSerde.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Long.class);
        props.put(JsonDeserializer.KEY_DEFAULT_TYPE, Contact.class);

        return props;
    }

    @Bean
    public KafkaStreams streams(JsonSerde<CallReport> jsonSerde) {
        StreamsBuilder builder = new StreamsBuilder();

        KGroupedStream<Contact, Long> groupedByContactStream = builder.stream(CALL_REPORT_TOPIC, Consumed.with(Serdes.String(), jsonSerde))
                .map((key, callReport) -> new KeyValue<>(callReport.getFrom(), callReport.getDurability()))
                .groupByKey();

        groupedByContactStream
                .reduce(Long::sum, Materialized.as(AGGREGATED_CALL_REPORT_TOPIC_STORE))
                .toStream().print(Printed.toSysOut());

        groupedByContactStream.windowedBy(TimeWindows.of(10000))
                .reduce(Long::sum, Materialized.as(WINDOWED_CALL_REPORT_TOPIC_STORE))
                .toStream().print(Printed.toSysOut());

        return new KafkaStreams(builder.build(), streamsConfiguration(UUID.randomUUID().toString()));
    }
}
