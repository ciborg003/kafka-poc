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
@Entity
@EqualsAndHashCode(of = "id")
public class Contact {

  @Id
  private UUID id = UUID.randomUUID();
  private String mobilePhone;

  public Contact(String mobilePhone) {
    this.mobilePhone = mobilePhone;
  }
}
