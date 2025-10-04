package com.opentext.assignment.repository;

import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.model.FileDataObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * File-system based Repository implementation.
 * <p>
 * Key properties:
 * - All writes use temporary files and atomic move to avoid partial reads.
 * - Buffered I/O with configurable buffer size for performance.
 * - Streaming write: data is not loaded into memory.
 * </p>
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class FileRepository implements com.opentext.assignment.model.Repository {

    @Value("${data.storage.dir:/tmp/data-objects}")
    private String baseDirPath;

    @Value("${repository.buffer.size:8192}")
    private int bufferSize;

    /** Resolve and ensure the base directory exists. */
    private Path getBaseDir() {
        Path baseDir = Paths.get(baseDirPath);
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize repository", e);
        }
        return baseDir;
    }

    /**
     * Create a new object with the provided initial data in uncompressed form.
     * Uses temp files + atomic moves to write data then meta.
     */
    public void create(String objectId, Stream<Character> initialData) {
        Path dataPath = getBaseDir().resolve(objectId + ".data");
        Path metaPath = getBaseDir().resolve(objectId + ".meta");
        Path tempDataPath = getBaseDir().resolve(objectId + ".data.tmp");
        Path tempMetaPath = getBaseDir().resolve(objectId + ".meta.tmp");

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(Files.newOutputStream(tempDataPath), Math.max(1024, bufferSize)), StandardCharsets.UTF_8));
             Stream<Character> dataStream = initialData) {
            dataStream.forEachOrdered(ch -> {
                try {
                    writer.write(ch);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            writer.flush();
            Files.move(tempDataPath, dataPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            Files.writeString(tempMetaPath, "false", StandardCharsets.UTF_8);
            Files.move(tempMetaPath, metaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log.info("Created object: {}", objectId);
        } catch (IOException e) {
            log.error("Failed to create object: {}", objectId, e);
            throw new RuntimeException("Create failed for " + objectId, e);
        } finally {
            try {
                if (Files.exists(tempDataPath)) {
                    Files.delete(tempDataPath);
                }
                if (Files.exists(tempMetaPath)) {
                    Files.delete(tempMetaPath);
                }
            } catch (IOException e) {
                log.warn("Failed to delete temp file(s) for {}: {}", objectId, e.getMessage());
            }
        }
    }

    @Override
    public Optional<DataObject> findById(String objectId) {
        Path dataPath = getBaseDir().resolve(objectId + ".data");
        Path metaPath = getBaseDir().resolve(objectId + ".meta");
        if (!Files.exists(dataPath)) {
            return Optional.empty();
        }
        try {
            boolean isCompressed = Files.exists(metaPath) &&
                    Boolean.parseBoolean(Files.readString(metaPath).trim());
            return Optional.of(new FileDataObject(objectId, dataPath, isCompressed));
        } catch (IOException e) {
            log.error("Failed to read object: {}", objectId, e);
            return Optional.empty();
        }
    }

    /**
     * Persist the DataObject's current data stream to the file-system.
     * Data is written to a temp file then atomically moved; meta is written afterwards.
     */
    @Override
    public void save(DataObject obj) {
        if (!(obj instanceof FileDataObject fileObj)) {
            throw new IllegalArgumentException("Unsupported DataObject type");
        }
        Path baseDir = getBaseDir();
        Path dataPath = baseDir.resolve(obj.getId() + ".data");
        Path metaPath = baseDir.resolve(obj.getId() + ".meta");
        Path tempDataPath = baseDir.resolve(obj.getId() + ".data.tmp");
        Path tempMetaPath = baseDir.resolve(obj.getId() + ".meta.tmp");

        try (Stream<Character> dataStream = obj.getData();
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new BufferedOutputStream(Files.newOutputStream(tempDataPath), Math.max(1024, bufferSize)), StandardCharsets.UTF_8))) {
            dataStream.forEachOrdered(ch -> {
                try {
                    writer.write(ch);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            writer.flush();
            log.debug("Wrote data to temp file: {}", tempDataPath);

            // Ensure target data file is replaced atomically where possible
            if (Files.exists(dataPath)) {
                try {
                    Files.delete(dataPath);
                    log.debug("Deleted existing file: {}", dataPath);
                } catch (IOException e) {
                    log.warn("Failed to delete existing file: {}", dataPath, e);
                }
            }
            Files.move(tempDataPath, dataPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

            // Write meta only after data is fully in place
            Files.writeString(tempMetaPath, String.valueOf(obj.isCompressed()), StandardCharsets.UTF_8);
            Files.move(tempMetaPath, metaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            log.info("Saved object: {}", obj.getId());
        } catch (IOException e) {
            log.error("Failed to save object: {}", obj.getId(), e);
            throw new RuntimeException("Save failed for " + obj.getId(), e);
        } finally {
            // clear transient stream to avoid accidental reuse and allow GC
            fileObj.setData(null);
            try {
                if (Files.exists(tempDataPath)) {
                    Files.delete(tempDataPath);
                    log.debug("Cleaned up temp file: {}", tempDataPath);
                }
                Path tempMetaPath2 = tempMetaPath; // final reference for inner use
                if (Files.exists(tempMetaPath2)) {
                    Files.delete(tempMetaPath2);
                    log.debug("Cleaned up temp meta file: {}", tempMetaPath2);
                }
            } catch (IOException e) {
                log.warn("Failed to delete temp file(s) for {}: {}", obj.getId(), e.getMessage());
            }
        }
    }

    /** Delete all files associated with the given ID, including any temp files. */
    public void delete(String objectId) {
        Path baseDir = getBaseDir();
        Path dataPath = baseDir.resolve(objectId + ".data");
        Path metaPath = baseDir.resolve(objectId + ".meta");
        Path tempDataPath = baseDir.resolve(objectId + ".data.tmp");
        Path tempMetaPath = baseDir.resolve(objectId + ".meta.tmp");
        try {
            Files.deleteIfExists(dataPath);
            Files.deleteIfExists(metaPath);
            Files.deleteIfExists(tempDataPath);
            Files.deleteIfExists(tempMetaPath);
            log.info("Deleted object files for ID: {}", objectId);
        } catch (IOException e) {
            log.warn("Failed to delete files for {}: {}", objectId, e.getMessage());
        }
    }
}