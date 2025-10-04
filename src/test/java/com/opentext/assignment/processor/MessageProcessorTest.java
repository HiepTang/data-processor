package com.opentext.assignment.processor;

import com.opentext.assignment.model.Message;
import com.opentext.assignment.model.MessageSequence;
import com.opentext.assignment.repository.FileRepository;
import com.opentext.assignment.service.CompressionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MessageProcessorTest {

    private FileRepository repository;
    private MessageProcessor processor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        repository = new FileRepository();
        ReflectionTestUtils.setField(repository, "baseDirPath", tempDir.toString());
        processor = new MessageProcessor(repository, new CompressionService());
    }

    @Test
    void testMultipleObjects() throws IOException {
        // Initialize two objects
        repository.create("obj1", "AAABBBC".chars().mapToObj(c -> (char) c));
        repository.create("obj2", "XYZ".chars().mapToObj(c -> (char) c));

        MessageSequence sequence = () -> Stream.of(
                new TestMessage("obj1", Message.Type.COMPRESS),
                new TestMessage("obj2", Message.Type.COMPRESS)
        );
        processor.process(sequence);

        Path dataPath1 = tempDir.resolve("obj1.data");
        Path metaPath1 = tempDir.resolve("obj1.meta");
        Path dataPath2 = tempDir.resolve("obj2.data");
        Path metaPath2 = tempDir.resolve("obj2.meta");

        assertEquals("A3B3C", Files.readString(dataPath1));
        assertEquals("true", Files.readString(metaPath1));
        assertEquals("XYZ", Files.readString(dataPath2)); // No compression needed
        assertEquals("true", Files.readString(metaPath2));
    }

    @Test
    void testLargeDataObject() throws IOException {
        String objectId = "large-object";
        StringBuilder largeData = new StringBuilder();
        for (int i = 0; i < 1_000_000; i++) largeData.append("A");
        repository.create(objectId, largeData.toString().chars().mapToObj(c -> (char) c));

        MessageSequence sequence = () -> Stream.of(
                new TestMessage(objectId, Message.Type.COMPRESS)
        );
        processor.process(sequence);

        Path dataPath = tempDir.resolve(objectId + ".data");
        Path metaPath = tempDir.resolve(objectId + ".meta");

        assertEquals("A1000000", Files.readString(dataPath));
        assertEquals("true", Files.readString(metaPath));
        assertFalse(Files.exists(tempDir.resolve(objectId + ".data.tmp")));
    }

    @Test
    void testConcurrentOperations() throws IOException {
        // Initialize three objects
        repository.create("obj1", "AAAA".chars().mapToObj(c -> (char) c));
        repository.create("obj2", "BBBB".chars().mapToObj(c -> (char) c));
        repository.create("obj3", "CCCC".chars().mapToObj(c -> (char) c));

        MessageSequence sequence = () -> Stream.of(
                new TestMessage("obj1", Message.Type.COMPRESS),
                new TestMessage("obj1", Message.Type.DECOMPRESS),
                new TestMessage("obj2", Message.Type.COMPRESS),
                new TestMessage("obj3", Message.Type.COMPRESS)
        );
        processor.process(sequence);

        Path dataPath1 = tempDir.resolve("obj1.data");
        Path metaPath1 = tempDir.resolve("obj1.meta");
        Path dataPath2 = tempDir.resolve("obj2.data");
        Path dataPath3 = tempDir.resolve("obj3.data");

        assertEquals("AAAA", Files.readString(dataPath1)); // Decompressed last
        assertEquals("false", Files.readString(metaPath1));
        assertEquals("B4", Files.readString(dataPath2));
        assertEquals("C4", Files.readString(dataPath3));
    }

    private record TestMessage(String dataObjectId, Message.Type type) implements Message {
        @Override
        public String getDataObjectId() {
            return dataObjectId;
        }

        @Override
        public Type getType() {
            return type;
        }
    }
}