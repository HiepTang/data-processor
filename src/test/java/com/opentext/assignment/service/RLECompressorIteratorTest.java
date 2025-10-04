package com.opentext.assignment.service;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RLECompressorIteratorTest {

    private String compress(String input) {
        Iterator<Character> iterator = new RLECompressorIterator(input.chars().mapToObj(c -> (char) c).iterator());
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .toString();
    }

    @Test
    void testBasicCompression() {
        String input = "AAabBBBCccDDdddDDEEE";
        assertEquals("A2abB3Cc2D2d3D2E3", compress(input));
    }

    @Test
    void testSingleCharacter() {
        assertEquals("A", compress("A"));
    }

    @Test
    void testAllSameCharacter() {
        assertEquals("A10", compress("AAAAAAAAAA"));
    }

    @Test
    void testEmptyInput() {
        assertEquals("", compress(""));
    }

    @Test
    void testSingleRepeats() {
        assertEquals("ABC", compress("ABC"));
    }

    @Test
    void testLargeCount() {
        assertEquals("A1000", compress("A".repeat(1000)));
    }

    @Test
    void testMultiDigitCounts() {
        assertEquals("A10B11", compress("AAAAAAAAAABBBBBBBBBBB"));
    }

    @Test
    void testBoundaryCount() {
        assertEquals("A9", compress("AAAAAAAAA"));
    }
}