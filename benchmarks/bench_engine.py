package org.example.benchmark;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * High-performance throughput and latency microbenchmark for a hypothetical 'aether-packet-engine'.
 * Designed to measure raw packet processing ops/sec, p50, and p99 latency percentiles.
 */
public class AetherPacketEngineBenchmark {

    // Benchmark configuration parameters
    private static final int WARMUP_ITERATIONS = 500_000;
    private static final int MEASUREMENT_ITERATIONS = 5_000_000;
    private static final int REPORTING_INTERVAL_MS = 1000;

    // Simulated Aether Packet Engine 
    public static final class AetherPacketEngine {
        private long processedCount = 0;

        public boolean processPacket(byte[] payload, int length) {
            // Simulate optimized wire-speed packet parsing, checksum validation, and routing lookup
            if (length < 14) {
                return false;
            }
            // Artificial low-overhead compute representation to emulate engine mechanics
            int hash = 0;
            for (int i = 0; i < length; i++) {
                hash = 31 * hash + payload[i];
            }
            processedCount++;
            return hash != 1; // Dummy evaluation
        }

        public long getProcessedCount() {
            return processedCount;
        }
    }

    public static void main(String[] args) {
        AetherPacketEngine engine = new AetherPacketEngine();
        byte[] samplePacket = new byte[1500]; // Standard MTU size packet
        ThreadLocalRandom.current().nextBytes(samplePacket);

        System.out.println("Initializing Aether Packet Engine Benchmark...");
        System.out.printf("Warmup Iterations:       %,d%n", WARMUP_ITERATIONS);
        System.out.printf("Measurement Iterations:  %,d%n", MEASUREMENT_ITERATIONS);
        System.out.println("--------------------------------------------------");

        // 1. Warmup Phase (JIT Compilation, Escape Analysis, Tiered Compilation stabilization)
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            engine.processPacket(samplePacket, samplePacket.length);
        }

        // 2. Allocation & Latency Tracking Setup
        // Pre-allocate latency array to avoid GC pauses during measurement
        long[] latenciesNanos = new long[MEASUREMENT_ITERATIONS];

        // 3. Measurement Phase
        System.out.println("Executing measurement phase...");
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
            long startNano = System.nanoTime();
            
            // Core operation invocation
            engine.processPacket(samplePacket, samplePacket.length);
            
            long endNano = System.nanoTime();
            latenciesNanos[i] = endNano - startNano;
        }
        
        long endTime = System.nanoTime();
        long totalDurationNanos = endTime - startTime;

        // 4. Data Processing & Percentile Calculation
        Arrays.sort(latenciesNanos);

        double totalDurationSeconds = totalDurationNanos / 1_000_000_000.0;
        double opsPerSec = MEASUREMENT_ITERATIONS / totalDurationSeconds;

        double p50LatencyMs = latenciesNanos[(int) (MEASUREMENT_ITERATIONS * 0.50)] / 1_000_000.0;
        double p90LatencyMs = latenciesNanos[(int) (MEASUREMENT_ITERATIONS * 0.90)] / 1_000_000.0;
        double p99LatencyMs = latenciesNanos[(int) (MEASUREMENT_ITERATIONS * 0.99)] / 1_000_000.0;
        double p999LatencyMs = latenciesNanos[(int) (MEASUREMENT_ITERATIONS * 0.999)] / 1_000_000.0;
        double maxLatencyMs = latenciesNanos[MEASUREMENT_ITERATIONS - 1] / 1_000_000.0;

        // 5. Formatted Tabular Output to Stdout
        System.out.println("\n==================================================");
        System.out.println("         AETHER PACKET ENGINE BENCHMARK RESULTS   ");
        System.out.println("==================================================");
        System.out.printf("| %-25s | %-20s |%n", "Metric", "Result");
        System.out.println("|---------------------------|----------------------|");
        System.out.printf("| %-25s | %,20.2f |%n", "Throughput (ops/sec)", opsPerSec);
        System.out.printf("| %-25s | %,20.2f |%n", "Total Operations", (double) MEASUREMENT_ITERATIONS);
        System.out.printf("| %-25s | %,20.4f |%n", "Duration (seconds)", totalDurationSeconds);
        System.out.println("|---------------------------|----------------------|");
        System.out.printf("| %-25s | %18.4f ms |%n", "Latency p50", p50LatencyMs);
        System.out.printf("| %-25s | %18.4f ms |%n", "Latency p90", p90LatencyMs);
        System.out.printf("| %-25s | %18.4f ms |%n", "Latency p99", p99LatencyMs);
        System.out.printf("| %-25s | %18.4f ms |%n", "Latency p99.9", p999LatencyMs);
        System.out.printf("| %-25s | %18.4f ms |%n", "Latency Max", maxLatencyMs);
        System.out.println("==================================================");
    }
}