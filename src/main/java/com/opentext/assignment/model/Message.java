package com.opentext.assignment.model;

/**
 * Identifies an event to process for a particular DataObject.
 * The {@link Type} specifies what operation to perform.
 */
public interface Message {
    /** @return the target DataObject ID */
    String getDataObjectId();
    /** @return the message type */
    Type getType();

    /**
     * Only COMPRESS and DECOMPRESS are relevant operations for the exercise, but
     * CREATE and DELETE may appear in the stream for ordering and cleanup semantics.
     */
    enum Type {
        /** The object is newly created; included to preserve ordering with subsequent messages. */
        CREATE,
        /** Compress the object's data (RLE). */
        COMPRESS,
        /** Decompress the object's data (reverse of RLE). */
        DECOMPRESS,
        /** Delete the object; no further messages will be applied. */
        DELETE
    }
}
