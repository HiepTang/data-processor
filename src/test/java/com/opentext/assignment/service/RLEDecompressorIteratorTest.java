package com.opentext.assignment.service;

import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RLEDecompressorIteratorTest {

    private String decompress(String input) {
        Iterator<Character> iterator = new RLEDecompressorIterator(input.chars().mapToObj(c -> (char) c).iterator());
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .toString();
    }

    @Test
    void testBasicDecompression() {
        assertEquals("AAabBBBCccDDdddDDEEE", decompress("A2abB3Cc2D2d3D2E3"));
    }

    @Test
    void testSingleCharacter() {
        assertEquals("A", decompress("A"));
    }

    @Test
    void testAllSameCharacter() {
        assertEquals("AAAAAAAAAA", decompress("A10"));
    }

    @Test
    void testEmptyInput() {
        assertEquals("", decompress(""));
    }

    @Test
    void testSingleRepeats() {
        assertEquals("ABC", decompress("ABC"));
    }

    @Test
    void testInvalidCompressedData() {
        Iterator<Character> iterator = new RLEDecompressorIterator("A2#B".chars().mapToObj(c -> (char) c).iterator());
        assertThrows(IllegalArgumentException.class, () -> {
            StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
        });
    }

    @Test
    void testLargeCount() {
        assertEquals("A".repeat(1000), decompress("A1000"));
    }

    @Test
    void testBoundaryCount() {
        assertEquals("AAAAAAAAA", decompress("A9"));
    }

    @Test
    void testCaseSensitiveLetters() {
        assertEquals("Aabc", decompress("Aabc"));
    }

    @Test
    void testLeadingDigits() {
        Iterator<Character> iterator = new RLEDecompressorIterator("123A2".chars().mapToObj(c -> (char) c).iterator());
        assertThrows(IllegalArgumentException.class, () -> {
            StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
        });
    }

    @Test
    void testConsecutiveLetters() {
        assertEquals("ABCDE", decompress("ABCDE"));
    }

    @Test
    void testOverflowCount() {
        Iterator<Character> iterator = new RLEDecompressorIterator("A2147483648".chars().mapToObj(c -> (char) c).iterator());
        assertThrows(IllegalArgumentException.class, () -> {
            StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
        });
    }

    @Test
    void testZeroCount() {
        Iterator<Character> iterator = new RLEDecompressorIterator("A0".chars().mapToObj(c -> (char) c).iterator());
        assertThrows(IllegalArgumentException.class, () -> {
            StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
        });
    }

    @Test
    void testNegativeCount() {
        Iterator<Character> iterator = new RLEDecompressorIterator("A-1".chars().mapToObj(c -> (char) c).iterator());
        assertThrows(IllegalArgumentException.class, () -> {
            StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
        });
    }

    @Test
    void testMixedCaseLargeCounts() {
        assertEquals("A".repeat(10) + "b".repeat(20) + "C".repeat(30), decompress("A10b20C30"));
    }
}