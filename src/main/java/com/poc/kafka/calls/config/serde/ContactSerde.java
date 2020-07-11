package com.poc.kafka.calls.config.serde;

import com.poc.kafka.calls.dto.Contact;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class ContactSerde extends WrapperSerde<Contact> {

  public ContactSerde() {
    super(new JsonSerializer<>(), new JsonDeserializer<>(Contact.class));
  }
}