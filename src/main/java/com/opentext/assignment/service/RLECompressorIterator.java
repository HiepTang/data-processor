package com.opentext.assignment.service;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that performs run-length encoding (RLE) compression.
 * Emits each unique character followed by its count if > 1 (e.g., "AAAA" -> "A4").
 * Handles empty input ("" -> ""), single characters ("A" -> "A"), and multi-digit counts ("AAAAAAAAAA" -> "A10").
 */
public class RLECompressorIterator implements Iterator<Character> {
    private final Iterator<Character> inputIterator;
    private Character currentChar;
    private long count;
    private String countStr;
    private int countIndex;
    private Character nextChar;

    public RLECompressorIterator(Iterator<Character> inputIterator) {
        this.inputIterator = inputIterator;
        advanceGroup();
    }

    private void advanceGroup() {
        count = 0;
        countStr = null;
        countIndex = 0;
        if (nextChar != null) {
            currentChar = nextChar;
            nextChar = null;
        } else if (inputIterator.hasNext()) {
            currentChar = inputIterator.next();
        } else {
            currentChar = null;
            return;
        }
        count = 1;
        while (inputIterator.hasNext()) {
            Character next = inputIterator.next();
            if (next.equals(currentChar)) {
                count++;
            } else {
                nextChar = next;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return currentChar != null || (countStr != null && countIndex < countStr.length());
    }

    @Override
    public Character next() {
        if (!hasNext()) throw new NoSuchElementException();
        if (countStr == null) {
            // Emit the character first
            Character toReturn = currentChar;
            countStr = count > 1 ? Long.toString(count) : null;
            countIndex = 0; // start from first digit if we have a count
            if (countStr == null) {
                // no digits to emit, move to the next group immediately
                advanceGroup();
            }
            return toReturn;
        } else {
            // Emit next digit of the count
            char digit = countStr.charAt(countIndex);
            countIndex++;
            if (countIndex == countStr.length()) {
                // finished emitting digits for this group
                countStr = null;
                advanceGroup();
            }
            return digit;
        }
    }
}