package com.learnkafka.libraryevents.consumer.entity;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToOne;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LibraryEvent {
	@Id
	@GeneratedValue
	private Integer libraryEventId;
	@Enumerated(EnumType.STRING)
	private LibraryEventType libraryEventType;

	@OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL)
	@ToString.Exclude
	private Book book;
}
