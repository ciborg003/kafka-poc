package com.poc.kafka.calls.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallReport {

  private Contact from;
  private Contact to;
  private long durability;

}
