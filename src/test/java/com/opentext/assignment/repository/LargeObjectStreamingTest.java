package com.opentext.assignment.repository;

import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.processor.MessageProcessor;
import com.opentext.assignment.service.CompressionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class LargeObjectStreamingTest {

    private FileRepository repository;
    private MessageProcessor processor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        repository = new FileRepository();
        ReflectionTestUtils.setField(repository, "baseDirPath", tempDir.toString());
        // Use larger buffer to speed up test a bit
        ReflectionTestUtils.setField(repository, "bufferSize", 1 << 16);
        processor = new MessageProcessor(repository, new CompressionService());
    }

    @Test
    void testCompressDecompressMultiMegabyte() throws IOException {
        String id = "large-stream";
        int size = 5 * 1024 * 1024; // 5 MiB of 'A'
        Stream<Character> data = Stream.generate(() -> 'A').limit(size);
        repository.create(id, data);

        // Compress
        processor.process(() -> Stream.of(new SimpleMessage(id, com.opentext.assignment.model.Message.Type.COMPRESS)));
        assertEquals("true", Files.readString(tempDir.resolve(id + ".meta")));
        assertEquals("A" + size, Files.readString(tempDir.resolve(id + ".data")));
        assertFalse(Files.exists(tempDir.resolve(id + ".data.tmp")));

        // Decompress back
        processor.process(() -> Stream.of(new SimpleMessage(id, com.opentext.assignment.model.Message.Type.DECOMPRESS)));
        assertEquals("false", Files.readString(tempDir.resolve(id + ".meta")));
        // Validate size and content by reading file length (string read would be heavy but acceptable here);
        String text = Files.readString(tempDir.resolve(id + ".data"));
        assertEquals(size, text.length());
        assertTrue(text.chars().allMatch(c -> c == 'A'));
    }

    private record SimpleMessage(String dataObjectId, com.opentext.assignment.model.Message.Type type)
            implements com.opentext.assignment.model.Message {
        @Override public String getDataObjectId() { return dataObjectId; }
        @Override public Type getType() { return type; }
    }
}
