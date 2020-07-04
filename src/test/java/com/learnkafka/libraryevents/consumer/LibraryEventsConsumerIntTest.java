package com.learnkafka.libraryevents.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryevents.consumer.entity.Book;
import com.learnkafka.libraryevents.consumer.entity.LibraryEvent;
import com.learnkafka.libraryevents.consumer.entity.LibraryEventType;
import com.learnkafka.libraryevents.consumer.jpa.LibraryEventRepository;
import com.learnkafka.libraryevents.consumer.service.LibraryEventService;

@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
		"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
})
public class LibraryEventsConsumerIntTest {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	
	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	private KafkaListenerEndpointRegistry endpointRegistry;
	
	@Autowired
	private ObjectMapper objectMapper;
	
	@Autowired
	private LibraryEventRepository libraryEventRepository;
	
	@SpyBean
	private LibraryEventsConsumer libraryEventsConsumer;
	
	@SpyBean
	private LibraryEventService libraryEventsService;
	
	@BeforeEach
	void setUp() {
		for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}
	
	@AfterEach
	void tearDown() {
		libraryEventRepository.deleteAll();
	}
	
	@Test
	public void publishNewLibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).libraryEventType(LibraryEventType.NEW).book(book).build();

		String json = objectMapper.writeValueAsString(libraryEvent);
		kafkaTemplate.sendDefault(json).get();
		
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		// then
		verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
		assertThat(libraryEventRepository.findAll()).hasSize(1);
		LibraryEvent created = libraryEventRepository.findAll().iterator().next();
		assertThat(created.getLibraryEventId()).isNotNull();
		assertThat(created.getLibraryEventType()).isEqualTo(LibraryEventType.NEW);
		assertThat(created.getBook().getBookName()).isEqualTo("Kafka using Spring Boot");
		
	}

	@Test
	public void publishUpdateLibraryEvent() throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book book = Book.builder().bookId(123).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build();
		LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(12).libraryEventType(LibraryEventType.NEW).book(book).build();
		libraryEventRepository.save(libraryEvent);
		
		Book updatedBook = Book.builder().bookId(123).bookName("Kafka using Spring Boot X").bookAuthor("Dilip").build();
		LibraryEvent updateLibraryEvent = LibraryEvent.builder().libraryEventId(12).libraryEventType(LibraryEventType.UPDATE).book(updatedBook).build();
		
		
		String json = objectMapper.writeValueAsString(updateLibraryEvent);
		kafkaTemplate.sendDefault(json).get();
		
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		// then
		verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		assertThat(libraryEventRepository.findById(12)).isEqualTo(Optional.empty());
	}

	@Test
	public void publishUpdateLibraryEventWithNonExistingId() throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book updatedBook = Book.builder().bookId(123).bookName("Kafka using Spring Boot X").bookAuthor("Dilip").build();
		LibraryEvent updateLibraryEvent = LibraryEvent.builder().libraryEventId(12).libraryEventType(LibraryEventType.UPDATE).book(updatedBook).build();
		
		
		String json = objectMapper.writeValueAsString(updateLibraryEvent);
		kafkaTemplate.sendDefault(json).get();
		
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		// then
		verify(libraryEventsConsumer, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsConsumer).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsService, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		assertThat(libraryEventRepository.findAll()).describedAs("Library event should not be stored").hasSize(0);
		assertThat(libraryEventRepository.findById(12)).describedAs("Library event should not be stored").isEqualTo(Optional.empty());
	}

	@Test
	public void publishUpdateLibraryEventWithNullId() throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		Book updatedBook = Book.builder().bookId(123).bookName("Kafka using Spring Boot X").bookAuthor("Dilip").build();
		LibraryEvent updateLibraryEvent = LibraryEvent.builder().libraryEventId(null).libraryEventType(LibraryEventType.UPDATE).book(updatedBook).build();
		
		
		String json = objectMapper.writeValueAsString(updateLibraryEvent);
		kafkaTemplate.sendDefault(json).get();
		
		// when
		CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);
		
		// then
		verify(libraryEventsConsumer, times(3)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsService, times(3)).processLibraryEvent(isA(ConsumerRecord.class));
		assertThat(libraryEventRepository.findAll()).describedAs("Library event should not be stored").hasSize(0);
	}

	
}
