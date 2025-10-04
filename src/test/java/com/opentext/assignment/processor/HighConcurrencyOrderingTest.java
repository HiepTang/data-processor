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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class HighConcurrencyOrderingTest {

    private FileRepository repository;
    private MessageProcessor processor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        repository = new FileRepository();
        ReflectionTestUtils.setField(repository, "baseDirPath", tempDir.toString());
        processor = new MessageProcessor(repository, new CompressionService());
        // Use all cores by default; MessageProcessor handles sizing itself
    }

    private record SimpleMessage(String dataObjectId, Message.Type type) implements Message {
        @Override public String getDataObjectId() { return dataObjectId; }
        @Override public Type getType() { return type; }
    }

    @Test
    void testPerIdOrderingUnderHighLoadSameId() throws IOException {
        String id = "hot-object";
        repository.create(id, "AAAAABBBBBCCCCCDDDD".chars().mapToObj(c -> (char) c));

        // 200 alternating operations for the same ID
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            messages.add(new SimpleMessage(id, (i % 2 == 0) ? Message.Type.COMPRESS : Message.Type.DECOMPRESS));
        }
        MessageSequence seq = () -> messages.stream();
        processor.process(seq);

        // After even number of toggles, state should be back to original (uncompressed)
        assertTrue(Files.exists(tempDir.resolve(id + ".data")));
        assertEquals("false", Files.readString(tempDir.resolve(id + ".meta")));
        assertEquals("AAAAABBBBBCCCCCDDDD", Files.readString(tempDir.resolve(id + ".data")));
        assertFalse(Files.exists(tempDir.resolve(id + ".data.tmp")));
    }

    @Test
    void testCrossIdParallelismWith100PlusMessages() throws Exception {
        // Prepare 20 objects
        int objectCount = 20;
        for (int i = 0; i < objectCount; i++) {
            String id = "obj-" + i;
            String payload = "A".repeat(1000 + i); // slightly varying size
            repository.create(id, payload.chars().mapToObj(c -> (char) c));
        }

        // Submit 120 messages across the 20 IDs in parallel using multiple process() calls
        int messageBatches = 120;
        ExecutorService exec = Executors.newFixedThreadPool(8);
        CountDownLatch latch = new CountDownLatch(messageBatches);
        for (int i = 0; i < messageBatches; i++) {
            int idx = i;
            exec.submit(() -> {
                try {
                    String id = "obj-" + (idx % objectCount);
                    Message.Type type = (idx % 2 == 0) ? Message.Type.COMPRESS : Message.Type.DECOMPRESS;
                    MessageSequence sequence = () -> Stream.of(new SimpleMessage(id, type));
                    processor.process(sequence);
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS));
        exec.shutdownNow();

        // Validate consistency per ID regardless of final state (race across process() calls can change parity per ID)
        for (int k = 0; k < objectCount; k++) {
            String id = "obj-" + k;
            String data = Files.readString(tempDir.resolve(id + ".data"));
            String meta = Files.readString(tempDir.resolve(id + ".meta"));
            String original = "A".repeat(1000 + k);
            if ("true".equals(meta)) {
                int count = 1000 + k;
                assertEquals("A" + count, data, "Compressed content mismatch for id=" + id);
            } else if ("false".equals(meta)) {
                assertEquals(original, data, "Uncompressed content mismatch for id=" + id);
            } else {
                fail("Invalid meta for id=" + id + ": " + meta);
            }
        }

        // No temp files should remain
        long tmpCount = Files.list(tempDir)
                .filter(p -> p.getFileName().toString().endsWith(".tmp"))
                .count();
        assertEquals(0, tmpCount);
    }
}
