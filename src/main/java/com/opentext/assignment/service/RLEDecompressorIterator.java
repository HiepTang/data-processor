package com.opentext.assignment.service;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterator that decompresses run-length encoded (RLE) strings.
 * Expects input in the format of a letter (uppercase or lowercase) followed by optional digits
 * (e.g., "A2" -> "AA", "A" -> "A", "A10" -> "AAAAAAAAAA").
 * Throws IllegalArgumentException for invalid input (non-letters, non-digits after letters,
 * zero or negative counts, or counts exceeding Integer.MAX_VALUE).
 */
public class RLEDecompressorIterator implements Iterator<Character> {
    private static final Logger log = LoggerFactory.getLogger(RLEDecompressorIterator.class);
    private final Iterator<Character> inputIterator;
    private Character currentChar;
    private int count;
    private int emitIndex;
    private Character nextChar;
    private boolean initialized;

    public RLEDecompressorIterator(Iterator<Character> inputIterator) {
        this.inputIterator = inputIterator;
        this.initialized = false;
    }

    private void ensureInitialized() {
        if (!initialized) {
            advanceGroup();
            initialized = true;
        }
    }

    private void advanceGroup() {
        count = 0;
        emitIndex = 0;
        if (nextChar != null) {
            currentChar = nextChar;
            nextChar = null;
        } else if (inputIterator.hasNext()) {
            currentChar = inputIterator.next();
        } else {
            currentChar = null;
            return;
        }
        if (!Character.isLetter(currentChar)) {
            throw new IllegalArgumentException("Expected letter, got: " + currentChar);
        }
        StringBuilder countStr = new StringBuilder();
        while (inputIterator.hasNext()) {
            Character next = inputIterator.next();
            if (Character.isDigit(next)) {
                countStr.append(next);
            } else {
                if (!Character.isLetter(next) && next != null) {
                    throw new IllegalArgumentException("Expected letter or digit, got: " + next);
                }
                nextChar = next;
                break;
            }
        }
        count = !countStr.isEmpty() ? parseCount(countStr.toString()) : 1;
        log.debug("Advance group: currentChar={}, count={}, nextChar={}", currentChar, count, nextChar);

    }

    private int parseCount(String countStr) {
        try {
            long parsed = Long.parseLong(countStr);
            if (parsed <= 0) {
                throw new IllegalArgumentException("Count must be positive, got: " + parsed);
            }
            if (parsed > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Count exceeds maximum: " + parsed);
            }
            return (int) parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid count: " + countStr, e);
        }
    }

    @Override
    public boolean hasNext() {
        ensureInitialized();
        return currentChar != null && emitIndex < count;
    }

    @Override
    public Character next() {
        ensureInitialized();
        if (!hasNext()) throw new NoSuchElementException();
        Character toReturn = currentChar;
        emitIndex++;
        if (emitIndex == count) {
            advanceGroup();
        }
        log.debug("Emitting: {}, emitIndex={}", toReturn, emitIndex);

        return toReturn;
    }
}