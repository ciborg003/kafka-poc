package com.poc.kafka.calls.driver;

import com.poc.kafka.calls.driver.config.KafkaProducerConfig;
import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import com.poc.kafka.calls.storage.ContactStorage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.concurrent.ThreadLocalRandom;

import static com.poc.kafka.calls.config.KafkaConfig.CALL_REPORT_TOPIC;

@SpringBootApplication(scanBasePackages = {
        "com.poc.kafka.calls.config",
        "com.poc.kafka.calls.driver"
})
public class DataDriverApp implements ApplicationRunner {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        KafkaProducer<String, CallReport> kafkaProducer = buildKafkaProducer();

        while (true) {
            CallReport callReport = generateCallReport();
            kafkaProducer.send(new ProducerRecord<>(CALL_REPORT_TOPIC, callReport.getFrom().getMobilePhone(), callReport));

            Thread.sleep(1000);
        }
    }

    private CallReport generateCallReport() {
        Contact source = ContactStorage.getRandomContact();
        Contact destination;
        do {
            destination = ContactStorage.getRandomContact();
        } while (source.equals(destination));
        long durability = ThreadLocalRandom.current().nextInt(1000);

        return new CallReport(source, destination, durability);
    }

    private KafkaProducer<String, CallReport> buildKafkaProducer() {
        return new KafkaProducer<>(
                KafkaProducerConfig.producerProps(bootstrapAddress),
                new StringSerializer(),
                new JsonSerde<>(CallReport.class).serializer());
    }


    public static void main(String[] args) {
        new SpringApplicationBuilder(DataDriverApp.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }
}
