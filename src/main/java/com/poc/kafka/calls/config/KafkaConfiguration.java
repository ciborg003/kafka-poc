package com.poc.kafka.calls.config;

import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

import static com.poc.kafka.calls.config.CallReportConfiguration.CALL_REPORT_TOPIC;
import static com.poc.kafka.calls.config.CallReportConfiguration.CALL_REPORT_TOPIC_2;

@Configuration
public class KafkaConfiguration {

  @Value(value = "${kafka.bootstrapAddress}")
  private String bootstrapAddress;

  @Bean
  public KafkaAdmin kafkaAdmin() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

    return new KafkaAdmin(configs);
  }

  @Bean
  public KafkaTemplate<String, CallReport> callReportKafkaTemplate() {
    Map<String, Object> senderProps = senderProps();
    ProducerFactory<String, CallReport> pf = new DefaultKafkaProducerFactory<>(senderProps);
    return new KafkaTemplate<>(pf);
  }

  private Map<String, Object> senderProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new JsonSerde<>(CallReport.class).serializer().getClass());
    return props;
  }

  @Bean("kafkaListenerContainerFactory")
  public ConcurrentKafkaListenerContainerFactory<String, CallReport> kafkaListenerContainerFactory(ConsumerFactory<String, CallReport> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, CallReport> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    return factory;
  }

  @Bean("consumerFactory")
  public ConsumerFactory<String, CallReport> consumerFactory() {
    ContainerProperties containerProps = new ContainerProperties(CALL_REPORT_TOPIC, CALL_REPORT_TOPIC_2);
    containerProps.setMessageListener((MessageListener<Contact, CallReport>) contactCallReportConsumerRecord -> {});

    return new DefaultKafkaConsumerFactory<>(consumerProps());
  }

  private Map<String, Object> consumerProps() {
    return new HashMap<String, Object>() {{
      put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
      put(ConsumerConfig.GROUP_ID_CONFIG, "Spring-Consumer");
      put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new JsonSerde<>(CallReport.class).deserializer().getClass());
      put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    }};
  }
}
