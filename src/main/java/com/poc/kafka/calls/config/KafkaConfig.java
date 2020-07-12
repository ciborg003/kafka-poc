package com.poc.kafka.calls.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    public static final String CALL_REPORT_TOPIC = "call-report";

    @Bean
    public NewTopic callReportTopic() {
        return new NewTopic(CALL_REPORT_TOPIC, 3, (short) 3);
    }
}
