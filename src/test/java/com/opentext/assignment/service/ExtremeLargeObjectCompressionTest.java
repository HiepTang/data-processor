package com.opentext.assignment.service;

import com.opentext.assignment.model.DataObject;
import com.opentext.assignment.model.FileDataObject;
import com.opentext.assignment.model.Message;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Extreme-scale compression/decompression tests. These are intentionally skipped by default
 * to avoid multi-GB processing in typical CI runs. Enable with -Drun.extreme.tests=true.
 *
 * Notes:
 * - RLECompressorIterator supports long run counts; compressing a 4GiB run requires counts
 *   exceeding Integer.MAX_VALUE. This test validates only the first emitted characters
 *   ("A" followed by digits of the count) without draining the entire stream.
 * - The current RLEDecompressorIterator rejects counts > Integer.MAX_VALUE by design. A matching
 *   decompression test is provided but disabled until iterators are upgraded to support long counts.
 */
class ExtremeLargeObjectCompressionTest {

    private final CompressionService service = new CompressionService();

    private static final long FOUR_GIB = 4L * 1024 * 1024 * 1024; // 4,294,967,296

    @Test
    void compress4GiBOfA_skippedByDefault() {
        // Basic environment snapshot prior to assumption
        logEnvironment("before-assumption");

        // Skip unless explicitly enabled
        boolean run = Boolean.getBoolean("run.extreme.tests");
        System.out.println("[DEBUG_LOG] Extreme test flag run.extreme.tests=" + run);
        Assumptions.assumeTrue(run,
                "Skipping extreme 4GiB test. Enable with -Drun.extreme.tests=true");

        // Metrics: wall and CPU start
        long wallStartNs = System.nanoTime();
        long cpuStartNs = getProcessCpuTimeNs();
        logMemory("start");

        // Prepare a DataObject streaming 4GiB of 'A' without materializing
        DataObject obj = new FileDataObject("huge-A", Path.of("NUL"), false);
        Stream<Character> hugeA = asCharStreamOfA(FOUR_GIB);
        obj.setData(hugeA);
        System.out.println("[DEBUG_LOG] Prepared input stream of 'A' x " + FOUR_GIB);

        // Compress (do not write to disk, just transform stream)
        System.out.println("[DEBUG_LOG] Starting compression transform...");
        service.processOperation(obj, Message.Type.COMPRESS);
        Stream<Character> compressed = obj.getData();

        // Expect the compressed prefix to be "A" + "4294967296"
        String expectedCount = Long.toString(FOUR_GIB);
        int prefixLen = 1 + expectedCount.length();
        String actualPrefix = readPrefix(compressed, prefixLen);
        assertEquals("A" + expectedCount, actualPrefix);
        compressed.close();

        // Metrics: wall and CPU end
        long wallEndNs = System.nanoTime();
        long cpuEndNs = getProcessCpuTimeNs();
        long wallMs = (wallEndNs - wallStartNs) / 1_000_000L;
        long cpuMs = cpuEndNs >= 0 && cpuStartNs >= 0 ? (cpuEndNs - cpuStartNs) / 1_000_000L : -1L;
        double cpuUtilPct = computeCpuUtilPercent(cpuEndNs - cpuStartNs, wallEndNs - wallStartNs);
        System.out.println("[DEBUG_LOG] Completed compression prefix check");
        logMemory("end");
        System.out.println("[DEBUG_LOG] Wall time ms=" + wallMs + ", Process CPU ms=" + cpuMs + 
                ", Approx CPU util %=" + (cpuUtilPct >= 0 ? String.format("%.2f", cpuUtilPct) : "N/A"));
    }

    @Disabled("RLEDecompressorIterator currently rejects counts > Integer.MAX_VALUE. Enable after upgrading to long counts.")
    @Test
    void decompress4GiBOfA_disabledUntilLongCountsSupported() {
        String payload = "A" + FOUR_GIB; // e.g., A4294967296
        DataObject obj = new FileDataObject("huge-A", Path.of("NUL"), true);
        obj.setData(payload.chars().mapToObj(c -> (char) c));

        service.processOperation(obj, Message.Type.DECOMPRESS);
        Stream<Character> decompressed = obj.getData();

        // Read a small prefix to validate it emits 'A's (no need to drain 4GiB)
        String prefix = readPrefix(decompressed, 16);
        assertEquals("AAAAAAAAAAAAAAAA", prefix);
        decompressed.close();
    }

    private Stream<Character> asCharStreamOfA(long count) {
        // Stream of 'A' repeated count times, lazily generated
        return LongStream.range(0, count).mapToObj(i -> 'A');
    }

    private String readPrefix(Stream<Character> stream, int limit) {
        Iterator<Character> it = stream.iterator();
        StringBuilder sb = new StringBuilder(limit);
        int i = 0;
        while (i < limit && it.hasNext()) {
            sb.append(it.next());
            i++;
        }
        return sb.toString();
    }

    // ---- Telemetry helpers ----

    private static void logEnvironment(String phase) {
        Runtime rt = Runtime.getRuntime();
        int cores = rt.availableProcessors();
        String osName = System.getProperty("os.name");
        String osArch = System.getProperty("os.arch");
        String javaVer = System.getProperty("java.version");
        System.out.println("[DEBUG_LOG] phase=" + phase + ", os=" + osName + " " + osArch + ", java=" + javaVer + ", cores=" + cores);
        logMemory(phase + "-env");
        double sysLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
        System.out.println("[DEBUG_LOG] phase=" + phase + ", systemLoadAverage=" + sysLoad);
    }

    private static void logMemory(String phase) {
        Runtime rt = Runtime.getRuntime();
        long free = rt.freeMemory();
        long total = rt.totalMemory();
        long max = rt.maxMemory();
        long used = total - free;
        System.out.println("[DEBUG_LOG] phase=" + phase + ", mem.used=" + used + ", mem.total=" + total + ", mem.max=" + max);
    }

    private static long getProcessCpuTimeNs() {
        try {
            var os = ManagementFactory.getOperatingSystemMXBean();
            if (os instanceof com.sun.management.OperatingSystemMXBean bean) {
                return bean.getProcessCpuTime();
            }
        } catch (Throwable ignore) {
            // ignore and fall through
        }
        return -1L;
    }

    private static double computeCpuUtilPercent(long cpuDeltaNs, long wallDeltaNs) {
        if (cpuDeltaNs < 0 || wallDeltaNs <= 0) return -1.0;
        int cores = Runtime.getRuntime().availableProcessors();
        double cpuTimePerCoreNs = (double) wallDeltaNs * cores;
        return (cpuDeltaNs / cpuTimePerCoreNs) * 100.0;
    }
}
