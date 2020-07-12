package com.poc.kafka.calls.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.poc.kafka.calls.dto.Contact;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class ContactStorage {

    private static final Random RANDOM = new Random();
    private static final List<Contact> CONTACTS = ImmutableList.of(
            new Contact("+375291111111", "Alex"),
            new Contact("+375292222222", "John"),
            new Contact("+375293333333", "Pavel"),
            new Contact("+375294444444", "Steve"),
            new Contact("+375295555555", "Nick"),
            new Contact("+375296666666", "Michael"),
            new Contact("+375297777777", "Donald"));

    public static Contact getRandomContact() {
        return CONTACTS.get(RANDOM.nextInt(CONTACTS.size()));
    }
}
