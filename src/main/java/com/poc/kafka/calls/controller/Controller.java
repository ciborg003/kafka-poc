package com.poc.kafka.calls.controller;

import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import com.poc.kafka.calls.repository.ContactRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.poc.kafka.calls.config.CallReportConfiguration.CALL_REPORT_TOPIC;
import static com.poc.kafka.calls.config.CallReportConfiguration.CALL_REPORT_TOPIC_2;

@RestController
public class Controller {

  private final ContactRepository contactRepository;
  private final KafkaTemplate<String, CallReport> kafkaTemplate;
  private final List<String> mobilePhones = Arrays.asList("+375291111111", "+375292222222",
      "+375293333333", "+375294444444", "+375295555555", "+375296666666", "+375297777777");
  private final Random random = new Random();

  private boolean running;

  @Autowired
  public Controller(ContactRepository contactRepository,
      KafkaTemplate kafkaTemplate) {
    this.contactRepository = contactRepository;
    this.kafkaTemplate = kafkaTemplate;
  }

  @GetMapping("/")
  public void startKafka() {

    AtomicLong count = new AtomicLong(0L);
    IntStream.range(0, 20)
        .mapToObj(value -> {
          CallReport callReport = generateCallReport();
          count.addAndGet(callReport.getDurability());
          kafkaTemplate.send(CALL_REPORT_TOPIC, callReport);
          kafkaTemplate.send(CALL_REPORT_TOPIC_2, callReport);
          return callReport;
        })
        .collect(Collectors.groupingBy(CallReport::getFrom,Collectors.summingLong(CallReport::getDurability)))
        .forEach((contact, aLong) -> System.out.println(contact + ": " + aLong));
    System.out.println(count.get());
  }

  @GetMapping("/start")
  public void start() {

    running = true;
    while (running) {
      CallReport callReport = generateCallReport();
      kafkaTemplate.send(CALL_REPORT_TOPIC, callReport);
      kafkaTemplate.send(CALL_REPORT_TOPIC_2, callReport);
    }
  }

  @GetMapping("/stop")
  public void stop() {
    running = false;
  }

  private CallReport generateCallReport() {
    int indexFrom = random.nextInt(mobilePhones.size());
    int indexTo = indexFrom;

    while (indexTo == indexFrom) {
      indexTo = random.nextInt(mobilePhones.size());
    }

    Contact contactFrom = contactRepository.findContactByMobilePhone(mobilePhones.get(indexFrom));
    Contact contactTo = contactRepository.findContactByMobilePhone(mobilePhones.get(indexTo));

    int durability = random.nextInt(1000);

    return CallReport.builder()
        .from(contactFrom)
        .to(contactTo)
        .durability(durability)
        .build();

  }
}
