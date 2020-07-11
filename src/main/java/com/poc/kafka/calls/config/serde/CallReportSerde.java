package com.poc.kafka.calls.config.serde;

import com.poc.kafka.calls.dto.CallReport;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class CallReportSerde extends WrapperSerde<CallReport> {

  public CallReportSerde() {
    super(new JsonSerializer<>(), new JsonDeserializer<>(CallReport.class));
  }
}
