package com.opentext.assignment.service;

import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Stateless service that transforms DataObject data streams using RLE.
 * It never materializes full content in memory; instead it returns transformed
 * streams backed by iterators.
 */
@Slf4j
@Service
public class CompressionService {

    /**
     * Apply compression or decompression depending on message type.
     * Updates both the DataObject's data stream and the compressed flag.
     */
    public void processOperation(DataObject obj, Message.Type type) {
        if (type == Message.Type.COMPRESS) {
            if (obj.isCompressed()) {
                log.warn("Object {} already compressed, skipping", obj.getId());
                return;
            }
            Stream<Character> compressed = compress(obj.getData());
            obj.setData(compressed);
            obj.setCompressed(true);
        } else if (type == Message.Type.DECOMPRESS) {
            if (!obj.isCompressed()) {
                log.warn("Object {} not compressed, skipping", obj.getId());
                return;
            }
            Stream<Character> decompressed = decompress(obj.getData());
            obj.setData(decompressed);
            obj.setCompressed(false);
        }
    }

    /**
     * Return a stream that emits RLE-compressed characters from the input stream.
     * Closing the returned stream closes the input.
     */
    private Stream<Character> compress(Stream<Character> input) {
        Iterator<Character> iterator = new RLECompressorIterator(input.iterator());
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
        ).onClose(() -> {
            input.close();
            log.debug("Closed input stream for compression");
        });
    }

    /**
     * Return a stream that emits characters from an RLE-compressed input stream.
     * Closing the returned stream closes the input.
     */
    private Stream<Character> decompress(Stream<Character> input) {
        Iterator<Character> iterator = new RLEDecompressorIterator(input.iterator());
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false
        ).onClose(() -> {
            input.close();
            log.debug("Closed input stream for decompression");
        });
    }
}