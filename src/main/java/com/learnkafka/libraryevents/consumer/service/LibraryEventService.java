package com.learnkafka.libraryevents.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryevents.consumer.entity.LibraryEvent;
import com.learnkafka.libraryevents.consumer.jpa.LibraryEventRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class LibraryEventService {
	@Autowired
	private ObjectMapper objectMapper;
	
	@Autowired
	private LibraryEventRepository libraryEventRepository;
	
	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonMappingException, JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("libraryEvent: {}", libraryEvent);
		
		switch (libraryEvent.getLibraryEventType()) {
		case NEW:
			save(libraryEvent);
			break;
		case UPDATE:
			validate(libraryEvent);
			save(libraryEvent);
			break;
		default:
			log.info("Invalid libraryEvenetType {}", libraryEvent.getLibraryEventType());
		}
	}

	private void validate(LibraryEvent libraryEvent) {
		if( libraryEvent.getLibraryEventId()==null ) {
			throw new IllegalArgumentException("Library event id is missing");
		}
		if( libraryEventRepository.findById(libraryEvent.getLibraryEventId()).isEmpty()){
			throw new IllegalArgumentException("NOta valid Library event");
		}
			
		log.info("Validation is successful for the libraryEvent: {}", libraryEvent);
	}

	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		
		libraryEventRepository.save(libraryEvent);
		log.info("Successfully persisted libraryEvent: {}", libraryEvent);
		
	}
}
