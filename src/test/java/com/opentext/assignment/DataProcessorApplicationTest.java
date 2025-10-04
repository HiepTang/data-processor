package com.opentext.assignment;

import com.opentext.assignment.model.Message;
import com.opentext.assignment.model.MessageSequence;
import com.opentext.assignment.model.Repository;
import com.opentext.assignment.processor.MessageProcessor;
import com.opentext.assignment.repository.FileRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class DataProcessorApplicationTest {

    @Autowired
    private Repository repository;

    @Autowired
    private MessageProcessor processor;

    @Test
    void testFullFlow(@TempDir Path tempDir) throws IOException {
        ReflectionTestUtils.setField(repository, "baseDirPath", tempDir.toString());

        String objectId = "demo-object";
        String raw = "AAabBBBCccDDdddDDEEE";
        ((FileRepository) repository).create(objectId, raw.chars().mapToObj(c -> (char) c));

        MessageSequence sequence = () -> Stream.of(
                new DataProcessorApplication.SimpleMessage(objectId, Message.Type.COMPRESS)
        );
        processor.process(sequence);

        Path dataPath = tempDir.resolve(objectId + ".data");
        Path metaPath = tempDir.resolve(objectId + ".meta");
        assertEquals("A2abB3Cc2D2d3D2E3", Files.readString(dataPath));
        assertEquals("true", Files.readString(metaPath));
        assertFalse(Files.exists(tempDir.resolve(objectId + ".data.tmp")));
    }

    @Test
    void testFullSequence(@TempDir Path tempDir) throws IOException {
        ReflectionTestUtils.setField(repository, "baseDirPath", tempDir.toString());

        // Initialize objects
        ((FileRepository) repository).create("1", "AAAA".chars().mapToObj(c -> (char) c));
        ((FileRepository) repository).create("2", "BBBB".chars().mapToObj(c -> (char) c));
        ((FileRepository) repository).create("3", "CCCC".chars().mapToObj(c -> (char) c));
        ((FileRepository) repository).create("4", "DDDD".chars().mapToObj(c -> (char) c));

        MessageSequence sequence = () -> Stream.of(
                new DataProcessorApplication.SimpleMessage("1", Message.Type.CREATE),
                new DataProcessorApplication.SimpleMessage("1", Message.Type.COMPRESS),
                new DataProcessorApplication.SimpleMessage("2", Message.Type.CREATE),
                new DataProcessorApplication.SimpleMessage("2", Message.Type.COMPRESS),
                new DataProcessorApplication.SimpleMessage("3", Message.Type.CREATE),
                new DataProcessorApplication.SimpleMessage("1", Message.Type.DECOMPRESS),
                new DataProcessorApplication.SimpleMessage("3", Message.Type.COMPRESS),
                new DataProcessorApplication.SimpleMessage("4", Message.Type.CREATE),
                new DataProcessorApplication.SimpleMessage("1", Message.Type.COMPRESS),
                new DataProcessorApplication.SimpleMessage("2", Message.Type.DECOMPRESS),
                new DataProcessorApplication.SimpleMessage("1", Message.Type.DELETE),
                new DataProcessorApplication.SimpleMessage("4", Message.Type.COMPRESS),
                new DataProcessorApplication.SimpleMessage("3", Message.Type.DELETE),
                new DataProcessorApplication.SimpleMessage("4", Message.Type.DELETE)
        );
        processor.process(sequence);

        assertEquals("BBBB", Files.readString(tempDir.resolve("2.data")));
        assertEquals("false", Files.readString(tempDir.resolve("2.meta")));
    }
}
