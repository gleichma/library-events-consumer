package com.learnkafka.libraryevents.consumer.jpa;

import org.springframework.data.repository.CrudRepository;

import com.learnkafka.libraryevents.consumer.entity.LibraryEvent;

public interface LibraryEventRepository extends CrudRepository<LibraryEvent, Integer> {

}
