package com.poc.kafka.calls.aggregator;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class AggregatorApp implements ApplicationRunner {

    private final KafkaStreams streams;

    public static void main(String[] args) {
        SpringApplication.run(AggregatorApp.class);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
