package com.learnkafka.libraryevents.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.learnkafka.libraryevents.consumer.service.LibraryEventService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class LibraryEventsConsumer {

	@Autowired
	private LibraryEventService LibraryEventService;
	
	@KafkaListener(topics = {"library-events"})
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		log.info("Consumer record: {}", consumerRecord);
		
		LibraryEventService.processLibraryEvent(consumerRecord);
	}
}
