package com.opentext.assignment.model;

import java.util.stream.Stream;

/**
 * A source of messages to be processed by the system.
 * Implementations can provide finite or infinite streams, but callers of
 * MessageProcessor.process() expect to iterate the stream and then shut down
 * after completion.
 */
public interface MessageSequence {
    /** @return a Stream of messages to process in order of encounter */
    Stream<Message> getMessages();
}
