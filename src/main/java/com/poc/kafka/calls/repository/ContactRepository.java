package com.poc.kafka.calls.repository;

import com.poc.kafka.calls.dto.Contact;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ContactRepository extends JpaRepository<Contact, UUID> {

  Contact findContactByMobilePhone(String mobilePhone);
}
