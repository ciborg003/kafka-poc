package com.poc.kafka.calls;

import static com.poc.kafka.calls.config.CallReportConfiguration.CALL_REPORT_TOPIC;

import com.poc.kafka.calls.dto.CallReport;
import com.poc.kafka.calls.dto.Contact;
import com.poc.kafka.calls.repository.ContactRepository;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class CallsApplication implements ApplicationRunner {

  private final ContactRepository contactRepository;
  private final List<String> mobilePhones = Arrays.asList("+375291111111", "+375292222222",
      "+375293333333", "+375294444444", "+375295555555", "+375296666666", "+375297777777");

  @Autowired
  public CallsApplication(ContactRepository contactRepository) {
    this.contactRepository = contactRepository;
  }


  public static void main(String[] args) {
    SpringApplication.run(CallsApplication.class, args);
  }


  @Override
  public void run(ApplicationArguments args) throws Exception {

    List<Contact> contacts = mobilePhones.stream()
        .map(Contact::new)
        .collect(Collectors.toList());

    contactRepository.saveAll(contacts);
  }
}
