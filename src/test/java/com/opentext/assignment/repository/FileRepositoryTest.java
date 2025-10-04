package com.opentext.assignment.repository;


import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.model.FileDataObject;
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

class FileRepositoryTest {

    private FileRepository repository;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        repository = new FileRepository();
        ReflectionTestUtils.setField(repository, "baseDirPath", tempDir.toString());
    }

    @Test
    void testCreateAndFindById() throws IOException {
        String objectId = "test-object";
        String data = "AAABBBC";
        repository.create(objectId, data.chars().mapToObj(c -> (char) c));

        Optional<DataObject> optObj = repository.findById(objectId);
        assertTrue(optObj.isPresent());
        DataObject obj = optObj.get();
        assertEquals(objectId, obj.getId());
        assertFalse(obj.isCompressed());
        assertEquals(data, obj.getData().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString());
        assertFalse(Files.exists(tempDir.resolve(objectId + ".data.tmp")));
    }

    @Test
    void testSaveCompressed() throws IOException {
        String objectId = "test-object";
        String data = "AAABBBC";
        repository.create(objectId, data.chars().mapToObj(c -> (char) c));

        FileDataObject obj = (FileDataObject) repository.findById(objectId).get();
        obj.setData("A3B3C".chars().mapToObj(c -> (char) c));
        obj.setCompressed(true);
        repository.save(obj);

        Path dataPath = tempDir.resolve(objectId + ".data");
        Path metaPath = tempDir.resolve(objectId + ".meta");
        assertEquals("A3B3C", Files.readString(dataPath));
        assertEquals("true", Files.readString(metaPath));
        assertFalse(Files.exists(tempDir.resolve(objectId + ".data.tmp")));
    }

    @Test
    void testFindByIdNonExistent() {
        Optional<DataObject> optObj = repository.findById("non-existent");
        assertFalse(optObj.isPresent());
    }

    @Test
    void testCreateEmptyData() throws IOException {
        String objectId = "empty-object";
        repository.create(objectId, Stream.empty());

        Optional<DataObject> optObj = repository.findById(objectId);
        assertTrue(optObj.isPresent());
        assertEquals("", optObj.get().getData().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString());
        assertFalse(optObj.get().isCompressed());
    }
}