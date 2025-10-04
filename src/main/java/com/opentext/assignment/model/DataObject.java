package com.opentext.assignment.model;

import java.util.stream.Stream;

/**
 * Simplified domain object for the assignment.
 * <p>
 * Only the character data and the compressed flag are modeled. Data is persisted separately
 * from other fields and can be in compressed (RLE) or uncompressed form. Implementations should
 * expose data as a Stream<Character> to support sizes larger than memory.
 * </p>
 */
public interface DataObject {
    /** @return unique identifier of this object */
    String getId();

    /**
     * Obtain a stream of characters representing the current data state.
     * The caller is responsible for consuming the stream and closing it if closeable.
     */
    Stream<Character> getData();

    /**
     * Set the data stream to be persisted by the repository on save().
     * Implementations should not eagerly materialize the stream.
     */
    void setData(Stream<Character> data);

    /** @return true if the current persistent representation is compressed */
    boolean isCompressed();

    /** Set the compressed flag to align with the current data stream. */
    void setCompressed(boolean compressed);
}
