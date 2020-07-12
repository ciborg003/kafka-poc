package com.poc.kafka.calls.aggregator.service;

import com.poc.kafka.calls.dto.Contact;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import static com.poc.kafka.calls.aggregator.config.StreamsConfiguration.AGGREGATED_CALL_REPORT_TOPIC_STORE;
import static com.poc.kafka.calls.aggregator.config.StreamsConfiguration.WINDOWED_CALL_REPORT_TOPIC_STORE;

@RestController
@RequiredArgsConstructor
public class CallAggregatorService {

    private final KafkaStreams streams;

    @GetMapping("/aggregate")
    public ResponseEntity<Map<Contact, Long>> getAggregatedTable() {
        ReadOnlyKeyValueStore<Contact, Long> store = streams.store(AGGREGATED_CALL_REPORT_TOPIC_STORE, QueryableStoreTypes.keyValueStore());
        KeyValueIterator<Contact, Long> iterator = store.all();

        Map<Contact, Long> responseTable = new HashMap<>();
        while (iterator.hasNext()) {
            KeyValue<Contact, Long> keyValue = iterator.next();
            responseTable.put(keyValue.key, keyValue.value);
        }

        return ResponseEntity.ok(responseTable);
    }

    @GetMapping("/window")
    public ResponseEntity<Map<Contact, Long>> getWindowedTable() {
        ReadOnlyWindowStore<Contact, Long> store = streams.store(WINDOWED_CALL_REPORT_TOPIC_STORE, QueryableStoreTypes.windowStore());
        KeyValueIterator<Windowed<Contact>, Long> iterator = store.all();

        Map<Contact, Long> responseTable = new HashMap<>();
        while (iterator.hasNext()) {
            KeyValue<Windowed<Contact>, Long> keyValue = iterator.next();
            responseTable.put(keyValue.key.key(), keyValue.value);
        }

        return ResponseEntity.ok(responseTable);
    }
}
