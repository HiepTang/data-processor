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

import static org.junit.jupiter.api.Assertions.*;

class DeleteRaceTest {

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

    private record SimpleMessage(String dataObjectId, Message.Type type) implements Message {
        @Override public String getDataObjectId() { return dataObjectId; }
        @Override public Type getType() { return type; }
    }

    @Test
    void testDeleteClearsQueueAndRemovesFiles() throws IOException {
        String id = "race-delete";
        repository.create(id, "AAABBBCCC".chars().mapToObj(c -> (char) c));

        // Sequence: compress, compress, delete, decompress, compress (the last two must be ignored)
        MessageSequence seq = () -> Stream.of(
                new SimpleMessage(id, Message.Type.COMPRESS),
                new SimpleMessage(id, Message.Type.COMPRESS),
                new SimpleMessage(id, Message.Type.DELETE),
                new SimpleMessage(id, Message.Type.DECOMPRESS),
                new SimpleMessage(id, Message.Type.COMPRESS)
        );
        processor.process(seq);

        // After DELETE, files should be removed
        assertFalse(Files.exists(tempDir.resolve(id + ".data")));
        assertFalse(Files.exists(tempDir.resolve(id + ".meta")));
        assertFalse(Files.exists(tempDir.resolve(id + ".data.tmp")));
        assertFalse(Files.exists(tempDir.resolve(id + ".meta.tmp")));

        // Submitting more messages should be ignored
        MessageSequence afterDelete = () -> Stream.of(
                new SimpleMessage(id, Message.Type.COMPRESS),
                new SimpleMessage(id, Message.Type.DECOMPRESS)
        );
        processor.process(afterDelete);
        // Still no files recreated
        assertFalse(Files.exists(tempDir.resolve(id + ".data")));
        assertFalse(Files.exists(tempDir.resolve(id + ".meta")));
    }
}
