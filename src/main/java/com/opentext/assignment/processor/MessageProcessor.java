package com.opentext.assignment.processor;

import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.model.Message;
import com.opentext.assignment.model.MessageSequence;
import com.opentext.assignment.model.Repository;
import com.opentext.assignment.service.CompressionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Processes a sequence of messages to compress or decompress DataObjects.
 * Uses a thread pool to process operations concurrently for different DataObject IDs,
 * while ensuring sequential processing for the same ID using per-ID queues.
 * Lazily initializes ExecutorService to handle multiple process() calls without RejectedExecutionException.
 */
@Slf4j
@Component
public class MessageProcessor {

    private final Repository repository;
    private final CompressionService compressionService;
    /** Executor shared by per-ID drainers. Lazily (re)created per process() invocation. */
    private volatile ExecutorService executor;

    /** Per-ID FIFO queues holding operations in submission order. */
    private final ConcurrentHashMap<String, LinkedBlockingQueue<Runnable>> operationQueues = new ConcurrentHashMap<>();
    /** Per-ID guard ensuring only one drainer runs at a time for that ID. */
    private final ConcurrentHashMap<String, AtomicBoolean> processingFlags = new ConcurrentHashMap<>();
    /** Tombstone of IDs that have been deleted; subsequent messages are ignored. */
    private final Set<String> deletedIds = ConcurrentHashMap.newKeySet();

    @Value("${processor.thread.pool.size:0}")
    private int threadPoolSize;

    @Value("${processor.shutdown.timeout.seconds:60}")
    private long shutdownTimeoutSeconds;

    @Autowired
    public MessageProcessor(Repository repository, CompressionService compressionService) {
        this.repository = repository;
        this.compressionService = compressionService;
    }

    /**
     * Ensure an ExecutorService exists and is usable. Creates a fixed pool sized either by
     * configuration or by the number of available processors.
     */
    private synchronized ExecutorService ensureExecutor() {
        if (executor == null || executor.isShutdown() || executor.isTerminated()) {
            int effectiveSize = threadPoolSize > 0 ? threadPoolSize : Math.max(1, Runtime.getRuntime().availableProcessors());
            if (threadPoolSize <= 0 && log.isDebugEnabled()) {
                log.debug("The processor.thread.pool.size={}, using the availableProcessors {}", threadPoolSize, effectiveSize);
            }
            executor = Executors.newFixedThreadPool(effectiveSize);
            if (log.isDebugEnabled()) {
                log.debug("Created new ExecutorService with {} threads", effectiveSize);
            }
        }
        return executor;
    }

    /**
     * Consume all messages from the given sequence, enqueue per-ID operations preserving order,
     * and wait for completion with a graceful shutdown.
     */
    public void process(MessageSequence sequence) {
        ensureExecutor();
        sequence.getMessages().forEach(this::handleMessage);
        ExecutorService exec = executor;
        exec.shutdown();
        try {
            long effectiveTimeout = shutdownTimeoutSeconds > 0 ? shutdownTimeoutSeconds : 60L;
            if (shutdownTimeoutSeconds <= 0 && log.isDebugEnabled()) {
                log.debug("Invalid processor.shutdown.timeout.seconds={}, falling back to {}", shutdownTimeoutSeconds, effectiveTimeout);
            }
            if (!exec.awaitTermination(effectiveTimeout, java.util.concurrent.TimeUnit.SECONDS)) {
                log.warn("Executor did not terminate within {} seconds", effectiveTimeout);
            }
        } catch (InterruptedException e) {
            log.error("Executor shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Translate a Message into a Runnable operation and enqueue it on the per-ID queue.
     * Schedules a drainer when transitioning from idle to active.
     */
    private void handleMessage(Message message) {
        String id = message.getDataObjectId();
        Message.Type type = message.getType();

        // If deleted, ignore any subsequent messages for this ID
        if (deletedIds.contains(id)) {
            if (log.isDebugEnabled()) {
                log.debug("Ignoring message {} for deleted ID: {}", type, id);
            }
            return;
        }

        LinkedBlockingQueue<Runnable> queue = operationQueues.computeIfAbsent(id, k -> new LinkedBlockingQueue<>());
        AtomicBoolean isProcessing = processingFlags.computeIfAbsent(id, k -> new AtomicBoolean(false));

        Runnable operation;
        switch (type) {
            case CREATE -> {
                // No data operation required; enqueue a no-op to preserve ordering semantics
                operation = () -> log.debug("CREATE encountered for ID: {} (no-op)", id);
            }
            case DELETE -> {
                operation = () -> {
                    // Mark as deleted to prevent future submissions from being accepted
                    deletedIds.add(id);
                    // Best-effort delete from repository if supported
                    try {
                        if (repository instanceof com.opentext.assignment.repository.FileRepository fr) {
                            fr.delete(id);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to delete object {}: {}", id, e.getMessage());
                    }
                    // Clear any queued operations for this ID
                    LinkedBlockingQueue<Runnable> q = operationQueues.get(id);
                    if (q != null) {
                        q.clear();
                    }
                    if (log.isInfoEnabled()) {
                        log.info("Deleted ID: {} and cleared pending operations", id);
                    }
                };
            }
            case COMPRESS, DECOMPRESS -> {
                operation = () -> {
                    Optional<DataObject> optObj = repository.findById(id);
                    if (optObj.isEmpty()) {
                        log.warn("Object not found: {}", id);
                        return;
                    }
                    DataObject obj = optObj.get();
                    compressionService.processOperation(obj, type);
                    repository.save(obj);
                    if (log.isInfoEnabled()) {
                        log.info("Processed {} for ID: {}", type, id);
                    }
                };
            }
            default -> {
                // Unknown type, ignore
                operation = () -> log.debug("Skipping message type {} for ID: {}", type, id);
            }
        }

        queue.add(operation);

        if (isProcessing.compareAndSet(false, true)) {
            ensureExecutor().submit(() -> drainQueue(id));
        }
    }

    /**
     * Drain and execute all queued operations for the given ID sequentially.
     * If new operations arrive during draining, the method will reschedule itself
     * to ensure continued processing without overlapping drainers for the same ID.
     */
    private void drainQueue(String id) {
        LinkedBlockingQueue<Runnable> queue = operationQueues.get(id);
        AtomicBoolean isProcessing = processingFlags.get(id);

        try {
            while (!queue.isEmpty()) {
                Runnable op = queue.poll();
                if (op != null) {
                    op.run();
                }
            }
        } finally {
            isProcessing.set(false);
            if (queue.isEmpty()) {
                operationQueues.remove(id);
                processingFlags.remove(id);
            } else if (isProcessing.compareAndSet(false, true)) {
                ensureExecutor().submit(() -> drainQueue(id));
            }
        }
    }
}