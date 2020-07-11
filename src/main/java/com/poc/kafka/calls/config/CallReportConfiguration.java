package com.poc.kafka.calls.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class CallReportConfiguration {

  @Value(value = "${kafka.bootstrapAddress}") private String bootstrapAddress;
  private ObjectMapper objectMapper = new ObjectMapper();

  public static final String CALL_REPORT_TOPIC = "call-report";
  public static final String CALL_REPORT_TOPIC_2 = "call-report-2";
  public static final String AGGREGATED_CALL_REPORT_TOPIC_STORE = "aggregated-call-report";
  public static final String WINDOWED_CALL_REPORT_TOPIC_STORE = "windowed-call-report";
  public static final String AGGREGATED_CALL_REPORT_TOPIC_OUTPUT = "aggregated-call-report-output";
  public static final String WINDOWED_CALL_REPORT_TOPIC_OUTPUT = "windowed-call-report-output";

  @Bean
  public NewTopic topic1() {
    return new NewTopic(CALL_REPORT_TOPIC, 1, (short) 1);
  }

  @Bean
  public KafkaStreamsConfiguration streamConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Long.class);
    props.put(JsonDeserializer.KEY_DEFAULT_TYPE, Contact.class);

    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  public FactoryBean<StreamsBuilder> streamsBuilder(KafkaStreamsConfiguration streamConfiguration) {
    return new StreamsBuilderFactoryBean(streamConfiguration);
  }

  @Bean
  public KStream<String, CallReport> kStream(StreamsBuilder streamsBuilder) {

    KStream<String, CallReport> stream = streamsBuilder.stream(CALL_REPORT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(CallReport.class)));
    stream
        .map((key, callReport) -> new KeyValue<>(callReport.getFrom(), callReport.getDurability()))
        .groupByKey()
        .reduce((val1, val2) -> val1 + val2, Materialized.as(AGGREGATED_CALL_REPORT_TOPIC_STORE))
        .toStream()
        .to(AGGREGATED_CALL_REPORT_TOPIC_OUTPUT, Produced.with(new JsonSerde<>(Contact.class), Serdes.Long()));


    return stream;
  }

  @Bean
  public KafkaStreamsConfiguration windowedStreamsConfiguration() {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams2");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JsonSerde.class);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JsonSerde.class);
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
    props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
    props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Long.class);
    props.put(JsonDeserializer.KEY_DEFAULT_TYPE, Contact.class);

    return new KafkaStreamsConfiguration(props);
  }

  @Bean
  public FactoryBean<StreamsBuilder> windowedStreamsBuilder(KafkaStreamsConfiguration windowedStreamsConfiguration) {
    return new StreamsBuilderFactoryBean(windowedStreamsConfiguration);
  }

  @Bean
  public KStream<String, CallReport> windowedStream(StreamsBuilder windowedStreamsBuilder) {

    KStream<String, CallReport> stream = windowedStreamsBuilder.stream(CALL_REPORT_TOPIC_2, Consumed.with(Serdes.String(), new JsonSerde<>(CallReport.class)));
    stream
            .map((key, callReport) -> new KeyValue<>(callReport.getFrom(), callReport.getDurability()))
            .groupByKey()
            .windowedBy(TimeWindows.of(5))
            .reduce((val1, val2) -> val1 + val2, Materialized.as(WINDOWED_CALL_REPORT_TOPIC_STORE))
            .toStream()
            .to(WINDOWED_CALL_REPORT_TOPIC_OUTPUT, Produced.with(new WindowedSerdes.TimeWindowedSerde<>(new JsonSerde<>(Contact.class)), Serdes.Long()));


    return stream;
  }



  @Bean("aggregatedKafkaListenerContainerFactory")
  public ConcurrentKafkaListenerContainerFactory<Contact, Long> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Contact, Long> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

  public ConsumerFactory<Contact, Long> consumerFactory() {
    ContainerProperties containerProps = new ContainerProperties(AGGREGATED_CALL_REPORT_TOPIC_OUTPUT);
    containerProps.setMessageListener((MessageListener<Contact, CallReport>) contactCallReportConsumerRecord -> {});

    return new DefaultKafkaConsumerFactory<>(consumerProps());
  }

  private Map<String, Object> consumerProps() {
    return new HashMap<String, Object>() {{
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
      put(ConsumerConfig.GROUP_ID_CONFIG, "Aggregated-Spring-Consumer");
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new JsonSerde<>(Contact.class).deserializer().getClass());
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
      put(JsonDeserializer.TRUSTED_PACKAGES, "*");
      put(JsonDeserializer.VALUE_DEFAULT_TYPE, Long.class);
      put(JsonDeserializer.KEY_DEFAULT_TYPE, Contact.class);
    }};
  }

  @Bean("windowedKafkaListenerContainerFactory")
  public ConcurrentKafkaListenerContainerFactory<Contact, Long> windowedKafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Contact, Long> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(windowedTopicConsumerFactory());
    return factory;
  }

  public ConsumerFactory<Contact, Long> windowedTopicConsumerFactory() {
    ContainerProperties containerProps = new ContainerProperties(WINDOWED_CALL_REPORT_TOPIC_OUTPUT);
    containerProps.setMessageListener((MessageListener<Contact, CallReport>) contactCallReportConsumerRecord -> {});

    return new DefaultKafkaConsumerFactory<>(windowedTopicConsumerProps());
  }

  private Map<String, Object> windowedTopicConsumerProps() {
    return new HashMap<String, Object>() {{
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
      put(ConsumerConfig.GROUP_ID_CONFIG, "Windowed-Spring-Consumer");
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());
      put(JsonDeserializer.TRUSTED_PACKAGES, "*");
      put(JsonDeserializer.VALUE_DEFAULT_TYPE, Long.class);
      put(JsonDeserializer.KEY_DEFAULT_TYPE, Contact.class);
    }};
  }

  @Bean
  @Primary
  public TimeWindowedDeserializer<Contact> timeWindowedDeserializer() {
    return new TimeWindowedDeserializer<>(new JsonDeserializer<>(Contact.class));
  }
}
