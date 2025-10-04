package com.opentext.assignment.service;


import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.model.FileDataObject;
import com.opentext.assignment.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CompressionServiceTest {

    private final CompressionService service = new CompressionService();

    @Test
    void testCompress(@TempDir Path tempDir) throws Exception {
        String objectId = "test-object";
        Path dataPath = tempDir.resolve(objectId + ".data");
        DataObject obj = new FileDataObject(objectId, dataPath, false);
        obj.setData("AAABBBC".chars().mapToObj(c -> (char) c));

        service.processOperation(obj, Message.Type.COMPRESS);

        assertTrue(obj.isCompressed());
        assertEquals("A3B3C", obj.getData().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString());
    }

    @Test
    void testDecompress(@TempDir Path tempDir) throws Exception {
        String objectId = "test-object";
        Path dataPath = tempDir.resolve(objectId + ".data");
        DataObject obj = new FileDataObject(objectId, dataPath, true);
        obj.setData("A3B3C".chars().mapToObj(c -> (char) c));

        service.processOperation(obj, Message.Type.DECOMPRESS);

        assertFalse(obj.isCompressed());
        assertEquals("AAABBBC", obj.getData().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString());
    }

    @Test
    void testSkipAlreadyCompressed(@TempDir Path tempDir) {
        String objectId = "test-object";
        Path dataPath = tempDir.resolve(objectId + ".data");
        DataObject obj = new FileDataObject(objectId, dataPath, true);
        obj.setData("A3B3C".chars().mapToObj(c -> (char) c));

        service.processOperation(obj, Message.Type.COMPRESS);

        assertTrue(obj.isCompressed()); // Unchanged
        assertEquals("A3B3C", obj.getData().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString());
    }
}
