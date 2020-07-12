package com.poc.kafka.calls.consumer;

import com.poc.kafka.calls.config.KafkaConfig;
import com.poc.kafka.calls.consumer.config.KafkaConsumerConfig;
import com.poc.kafka.calls.driver.DataDriverApp;
import com.poc.kafka.calls.dto.CallReport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

import java.time.Duration;
import java.util.regex.Pattern;

@SpringBootApplication
public class ConsumerApp implements ApplicationRunner {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;



    @Override
    public void run(ApplicationArguments args) throws Exception {
        KafkaConsumer<String, CallReport> consumer1 = createKafkaConsumer();
        KafkaConsumer<String, CallReport> consumer2 = createKafkaConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer1.close();
            consumer2.close();
        }));

        startConsumer(consumer1, "1");
        startConsumer(consumer2, "2");

    }

    private void startConsumer(KafkaConsumer<String, CallReport> consumer, String id) {
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, CallReport> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, CallReport> record : records)
                    System.out.printf("Consumer%s offset = %d, key = %s, value = %s%n", id, record.offset(), record.key(), record.value());
            }
        }).start();
    }


    private KafkaConsumer<String, CallReport> createKafkaConsumer() {
        KafkaConsumer<String, CallReport> consumer = new KafkaConsumer<>(KafkaConsumerConfig.producerProps(bootstrapAddress, "1"));
        consumer.subscribe(Pattern.compile(KafkaConfig.CALL_REPORT_TOPIC));

        return consumer;
    }



    public static void main(String[] args) {
        new SpringApplicationBuilder(ConsumerApp.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }
}
