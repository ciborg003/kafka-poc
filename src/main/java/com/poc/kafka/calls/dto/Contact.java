package com.poc.kafka.calls.dto;

import java.util.UUID;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(of = "mobilePhone")
public class Contact {

  private String mobilePhone;
  private String name;

  public Contact(String mobilePhone, String name) {
    this.mobilePhone = mobilePhone;
    this.name = name;
  }
}
