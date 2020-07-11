package com.poc.kafka.calls;

import com.poc.kafka.calls.config.CallReportConfiguration;
import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CallReportConsumer {

//  @KafkaListener(topics = "call-report", containerFactory = "kafkaListenerContainerFactory")
//  public void listenReport(CallReport callReport) {
//    System.out.println("Consumer-CallReport: " + callReport);
//  }

  @KafkaListener(
          topics = CallReportConfiguration.AGGREGATED_CALL_REPORT_TOPIC_OUTPUT,
          containerFactory = "aggregatedKafkaListenerContainerFactory")
  public void listenAggregatedReports(ConsumerRecord<Contact, Long> consumer) {
    System.out.println(String.format("AGGREGATED-CallReport. Key:%s, Value:%s", consumer.key(), consumer.value()));
  }

  @KafkaListener(
          topics = CallReportConfiguration.WINDOWED_CALL_REPORT_TOPIC_OUTPUT,
          containerFactory = "windowedKafkaListenerContainerFactory")
  public void listenWindowedReports(ConsumerRecord<Contact, Long> consumer) {
    System.out.println(String.format("WINDOWED-CallReport. Key:%s, Value:%s", consumer.key(), consumer.value()));
  }
}
