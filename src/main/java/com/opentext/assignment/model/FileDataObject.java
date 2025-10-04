package com.opentext.assignment.model;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * File-backed implementation of DataObject.
 * <p>
 * - getData() lazily streams characters from the underlying file; it does not load the whole file.
 * - setData(Stream) stores a transient stream to be persisted by Repository.save().
 * - The compressed flag indicates the current persistent representation.
 * </p>
 */
@Data
@RequiredArgsConstructor
public class FileDataObject implements DataObject {
    private final String id;
    private final Path dataPath;
    private boolean compressed;
    /**
     * Transient data stream to be written by the repository on save(). If null,
     * getData() will provide a streaming view over the data file.
     */
    private Stream<Character> dataStream;

    public FileDataObject(String id, Path dataPath, boolean compressed) {
        this.id = id;
        this.dataPath = dataPath;
        this.compressed = compressed;

    }

    @Override
    public Stream<Character> getData() {
        if (dataStream != null) {
            return dataStream;
        }
        try {
            // Stream lines and flatMap to characters. For assignment data (single-line strings),
            // this is sufficient and memory-efficient. For arbitrary binary data, a Reader-based
            // implementation would be more appropriate.
            Stream<String> lines = Files.lines(dataPath);
            return lines
                    .flatMapToInt(String::chars)
                    .mapToObj(c -> (char) c)
                    .onClose(() -> {
                        try {
                            lines.close();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to close stream for ID: " + id, e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("Failed to read data for ID: " + id, e);
        }
    }

    @Override
    public void setData(Stream<Character> data) {
        this.dataStream = data;
    }

    @Override
    public boolean isCompressed() {
        return compressed;
    }

    @Override
    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }
}