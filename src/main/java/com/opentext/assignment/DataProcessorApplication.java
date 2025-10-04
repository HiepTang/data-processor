package com.opentext.assignment;

import com.opentext.assignment.model.Message;
import com.opentext.assignment.model.MessageSequence;
import com.opentext.assignment.model.Repository;
import com.opentext.assignment.processor.MessageProcessor;
import com.opentext.assignment.repository.FileRepository;
import lombok.Data;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.util.stream.Stream;

/**
 * Spring Boot entry point. On startup it creates a simple demo data object and submits
 * a COMPRESS message so developers can see the system working end-to-end.
 */
@SpringBootApplication
public class DataProcessorApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(DataProcessorApplication.class, args);
        MessageProcessor processor = context.getBean(MessageProcessor.class);
        Repository repository = context.getBean(Repository.class);

        // Initialize demo object
        String objectId = "demo-object";
        String raw = "AAabBBBCccDDdddDDEEE";
        Stream<Character> initialData = raw.chars().mapToObj(c -> (char) c);
        ((FileRepository) repository).create(objectId, initialData);

        // Mock MessageSequence with a single COMPRESS message
        MessageSequence mockSequence = () -> Stream.of(
                new SimpleMessage(objectId, Message.Type.COMPRESS)
        );

        processor.process(mockSequence);
    }

    /** Minimal local Message implementation for the demo sequence. */
    @Data
    static class SimpleMessage implements Message {
        private final String dataObjectId;
        private final Type type;
    }
}