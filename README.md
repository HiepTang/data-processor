# Data Processor (OpenText Coding Assignment)

A Spring Boot (Java 17) implementation of an ordered, concurrent message processor that compresses and decompresses large data objects using a streaming Run-Length Encoding (RLE) algorithm. The system preserves per-object ordering, allows parallelism across different objects, and performs atomic, memory-efficient file I/O.


## Quick Start

Prerequisites:
- Java 17 (JDK 17)
- Maven 3.8+
- Windows, Linux, or macOS filesystem (Windows examples shown)

Build and run tests:
- mvn -q -DskipTests=false clean test

Run the application (dev mode):
- mvn spring-boot:run

Build a runnable JAR and run:
- mvn -q -DskipTests clean package
- java -jar target\data-processor-0.0.1-SNAPSHOT.jar

When the app starts, it will:
- Create a demo object with raw data: AAabBBBCccDDdddDDEEE
- Submit a COMPRESS message for that object
- Process it via the concurrent MessageProcessor

Output files by default are written under the configured data.storage.dir (see Configuration). On Windows, the default in this repo is C:\\temp\\data-objects.


## Configuration

Edit src\main\resources\application.properties or override via JVM properties (-Dkey=value) or environment variables (SPRING-style):

- data.storage.dir: Directory where object data and meta files are stored.
  - Default (in this repo): C:/temp/data-objects
- processor.thread.pool.size: Thread pool size for MessageProcessor.
  - 0 (default) means use Runtime.availableProcessors()
- processor.shutdown.timeout.seconds: Graceful shutdown timeout when processing completes (default 60)
- repository.buffer.size: Buffer size in bytes for buffered I/O (default 65536)

Examples:
- java -Ddata.storage.dir=C:\\work\\objects -Dprocessor.thread.pool.size=8 -jar target\data-processor-0.0.1-SNAPSHOT.jar
- mvn -Dprocessor.thread.pool.size=4 spring-boot:run


## How It Works (Architecture Overview)

Key modules:

- MessageProcessor (com.opentext.assignment.processor)
  - Purpose: Process a MessageSequence with per-object ordering and cross-object parallelism.
  - Concurrency model:
    - Per-ID queues (LinkedBlockingQueue<Runnable>) stored in a ConcurrentHashMap
    - An AtomicBoolean per ID ensures a single draining task processes that ID sequentially
    - A configurable fixed thread pool executes drainers for different IDs concurrently
  - Supported message types:
    - CREATE: No-op queued to preserve order with subsequent messages
    - COMPRESS / DECOMPRESS: Fetch object, transform its data stream, and save
    - DELETE: Tombstones the ID (future messages are ignored), clears queued work, and removes files from the repository
  - Graceful termination: After feeding all messages, shuts down and awaits completion for a configurable timeout

- FileRepository (com.opentext.assignment.repository)
  - Storage layout: Two files per object: <id>.data and <id>.meta
  - Atomic writes: All writes go to temporary files (.data.tmp / .meta.tmp) then atomically moved to the final paths
  - Streaming I/O: Uses BufferedOutputStream + BufferedWriter with configurable buffer sizes for efficiency
  - delete(id): Removes data, meta, and any temporary files

- CompressionService (com.opentext.assignment.service)
  - Stateless service that transforms DataObject data streams between compressed and uncompressed states
  - Uses iterators:
    - RLECompressorIterator: emits character + run count (if > 1), count uses long to support very large runs
    - RLEDecompressorIterator: expands runs, validates input; currently limits counts to Integer.MAX_VALUE by design

- Data model (com.opentext.assignment.model)
  - DataObject: Provides Stream<Character> for data, plus a compressed flag
  - FileDataObject: Streams characters from a file lazily; setData accepts a stream for saving
  - Message: Types CREATE, COMPRESS, DECOMPRESS, DELETE
  - MessageSequence: Supplies a Stream<Message>


## Running and Inspecting the Demo

1) Start the application (mvn spring-boot:run or jar execution). It will process a demo COMPRESS operation for object demo-object.
2) Inspect the output directory (data.storage.dir):
   - demo-object.data: Contains the compressed text, e.g., A2abB3Cc2D2d3D2E3
   - demo-object.meta: Contains true (compressed) or false (uncompressed)

Windows (PowerShell) example:
- Get-Content C:\\temp\\data-objects\\demo-object.data
- Get-Content C:\\temp\\data-objects\\demo-object.meta


## Running Tests

- Run all tests: mvn -q test
- Run a specific test class:
  - mvn -q -Dtest=com.opentext.assignment.processor.MessageProcessorTest test

Extreme-scale test (disabled by default):
- The test ExtremeLargeObjectCompressionTest includes a 4 GiB-of-'A' compression prefix check. To enable, pass -Drun.extreme.tests=true:
  - mvn -q -Drun.extreme.tests=true -Dtest=com.opentext.assignment.service.ExtremeLargeObjectCompressionTest test

Note: The extreme test validates only the prefix of the compressed stream to avoid materializing multi-GB data.


## Performance Tuning

- processor.thread.pool.size: Increase to utilize more cores, or keep 0 to auto-size
- repository.buffer.size: Increase for large files (e.g., 262144 = 256 KiB) to reduce I/O syscall overhead
- data.storage.dir: Place on a fast local SSD for higher throughput
- Logging: Reduce logging level for com.opentext.assignment.* to INFO or WARN for performance runs


## Design Notes

- Ordering & parallelism: Per-ID queues preserve ordering for the same object while enabling parallel processing across different objects
- Race prevention: Atomic temp writes and atomic moves ensure readers never see partially written data; meta is written only after data
- Memory efficiency: Data is processed via streams and iterators; files are written using buffered streams to avoid loading entire content into memory
- DELETE semantics: Enqueued DELETE clears pending work for that ID and tombstones it so future messages are ignored


## Project Structure

- src/main/java/com/opentext/assignment
  - DataProcessorApplication.java — Spring Boot entry point and small demo
  - model/ — DataObject, FileDataObject, Message, MessageSequence, Repository
  - processor/ — MessageProcessor (concurrency, ordering, shutdown)
  - repository/ — FileRepository (atomic file ops, streaming I/O)
  - service/ — CompressionService, RLECompressorIterator, RLEDecompressorIterator
- src/main/resources/application.properties — default configuration
- src/test/java — unit and integration tests (concurrency, streaming, repository, RLE, extreme test)
- large_concurrent_improvemnet.md — detailed technical rationale for improvements


## Troubleshooting

- AccessDeniedException on Windows when saving data files:
  - The repository deletes existing files before moving temp files and uses REPLACE_EXISTING + ATOMIC_MOVE where supported. Ensure antivirus/locking tools are not holding files open.
- Executor did not terminate within N seconds:
  - Increase processor.shutdown.timeout.seconds if processing massive files or many messages
- Huge files or memory pressure:
  - Ensure data.storage.dir has enough free space; consider increasing repository.buffer.size and reduce logging verbosity
