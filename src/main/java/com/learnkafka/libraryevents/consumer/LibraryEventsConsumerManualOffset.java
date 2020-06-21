package com.learnkafka.libraryevents.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
//@Component
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String>{

	public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
		log.info("Consumer record: {}", consumerRecord);
	}

	@KafkaListener(topics = {"library-events"})
	@Override
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
		log.info("Consumer record: {}", consumerRecord);
		
		acknowledgment.acknowledge();
		
	}
}
