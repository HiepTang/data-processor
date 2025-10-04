package com.opentext.assignment.model;

import java.util.Optional;

/**
 * Repository abstraction to persist and retrieve DataObject instances.
 * <p>
 * Implementations should honor the assignment constraints:
 * - Data may be larger than memory and should be streamed.
 * - Reads should see only fully committed object states (no partial writes).
 * </p>
 */
public interface Repository {
    /**
     * Lookup an object by its ID.
     * @param objectId the unique identifier
     * @return an Optional containing the object if it exists; empty otherwise
     */
    Optional<DataObject> findById(String objectId);

    /**
     * Persist the given object, including its current data stream and compressed flag.
     * Implementations should use atomic techniques (e.g., temporary files + atomic move)
     * and must not load the whole stream into memory.
     * @param obj the object to save
     */
    void save(DataObject obj);
}
