package com.aether.packet.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit, invariant, and concurrent stress-test suite for 'aether-packet-engine'.
 * Contains stubs for core domain types to ensure standalone compilation and execution.
 */
public class AetherPacketEngineTest {

    // =========================================================================
    // STUB DEFINITIONS (Aether Packet Engine Domain Model)
    // =========================================================================

    public enum PacketType {
        CONTROL, DATA, HEARTBEAT, FRAGMENT, TERMINATE
    }

    public static class PacketHeader {
        private final long sequenceId;
        private final int payloadLength;
        private final PacketType packetType;
        private final short checksum;

        public PacketHeader(long sequenceId, int payloadLength, PacketType packetType, short checksum) {
            this.sequenceId = sequenceId;
            this.payloadLength = payloadLength;
            this.packetType = packetType;
            this.checksum = checksum;
        }

        public long getSequenceId() { return sequenceId; }
        public int getPayloadLength() { return payloadLength; }
        public PacketType getPacketType() { return packetType; }
        public short getChecksum() { return checksum; }

        public static PacketHeader decode(ByteBuffer buffer) {
            if (buffer.remaining() < 19) {
                throw new IllegalArgumentException("Insufficient buffer capacity for header decoding");
            }
            long seq = buffer.getLong();
            int len = buffer.getInt();
            int typeOrdinal = buffer.getInt();
            if (typeOrdinal < 0 || typeOrdinal >= PacketType.values().length) {
                throw new IllegalStateException("Corrupted packet type ordinal: " + typeOrdinal);
            }
            PacketType type = PacketType.values()[typeOrdinal];
            short chk = buffer.getShort();
            return new PacketHeader(seq, len, type, chk);
        }

        public void encode(ByteBuffer buffer) {
            buffer.putLong(sequenceId);
            buffer.putInt(payloadLength);
            buffer.putInt(packetType.ordinal());
            buffer.putShort(checksum);
        }
    }

    public static class Packet {
        private final PacketHeader header;
        private final byte[] payload;

        public Packet(PacketHeader header, byte[] payload) {
            if (header == null) throw new IllegalArgumentException("Header cannot be null");
            if (payload == null) throw new IllegalArgumentException("Payload cannot be null");
            if (header.getPayloadLength() != payload.length) {
                throw new IllegalArgumentException("Header payload length mismatch: declared=" + 
                    header.getPayloadLength() + ", actual=" + payload.length);
            }
            this.header = header;
            this.payload = payload;
        }

        public PacketHeader getHeader() { return header; }
        public byte[] getPayload() { return payload; }

        public static short computeChecksum(byte[] data) {
            int sum = 0;
            for (byte b : data) {
                sum += (b & 0xFF);
            }
            return (short) (~sum & 0xFFFF);
        }
    }

    public interface PacketInterceptor {
        boolean intercept(Packet packet);
    }

    public static class PacketEngine {
        private final int capacity;
        private final BlockingQueue<Packet> inboundQueue;
        private final List<PacketInterceptor> interceptors;
        private final AtomicLong processedCount = new AtomicLong(0);
        private final AtomicLong droppedCount = new AtomicLong(0);
        private volatile boolean active = true;
        private final Object lifecycleLock = new Object();

        public PacketEngine(int capacity) {
            if (capacity <= 0) throw new IllegalArgumentException("Capacity must be positive");
            this.capacity = capacity;
            this.inboundQueue = new ArrayBlockingQueue<>(capacity);
            this.interceptors = new CopyOnWriteArrayList<>();
        }

        public void registerInterceptor(PacketInterceptor interceptor) {
            if (interceptor == null) throw new IllegalArgumentException("Interceptor cannot be null");
            interceptors.add(interceptor);
        }

        public boolean submit(Packet packet) {
            if (!active) return false;
            if (packet == null) throw new IllegalArgumentException("Packet cannot be null");

            for (PacketInterceptor interceptor : interceptors) {
                try {
                    if (!interceptor.intercept(packet)) {
                        droppedCount.incrementAndGet();
                        return false;
                    }
                } catch (Exception e) {
                    droppedCount.incrementAndGet();
                    return false;
                }
            }

            boolean offered = inboundQueue.offer(packet);
            if (!offered) {
                droppedCount.incrementAndGet();
            }
            return offered;
        }

        public Packet poll(long timeout, TimeUnit unit) throws InterruptedException {
            Packet p = inboundQueue.poll(timeout, unit);
            if (p != null) {
                processedCount.incrementAndGet();
            }
            return p;
        }

        public void shutdown() {
            synchronized (lifecycleLock) {
                active = false;
                inboundQueue.clear();
            }
        }

        public void recover() {
            synchronized (lifecycleLock) {
                inboundQueue.clear();
                processedCount.set(0);
                droppedCount.set(0);
                active = true;
            }
        }

        public boolean isActive() { return active; }
        public long getProcessedCount() { return processedCount.get(); }
        public long getDroppedCount() { return droppedCount.get(); }
        public int getQueueSize() { return inboundQueue.size(); }
        public int getCapacity() { return capacity; }
    }

    // =========================================================================
    // TEST SUITE
    // =========================================================================

    private PacketEngine engine;

    @BeforeEach
    void setUp() {
        engine = new PacketEngine(1024);
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    // =========================================================================
    // 1. BOUNDARY AND EDGE-CASE ASSERTIONS
    // =========================================================================

    @Test
    @DisplayName("Edge Case: Zero-length payload packing and decoding boundary invariants")
    void testZeroLengthPayloadBoundary() {
        byte[] payload = new byte[0];
        short checksum = Packet.computeChecksum(payload);
        PacketHeader header = new PacketHeader(1L, 0, PacketType.HEARTBEAT, checksum);
        Packet packet = new Packet(header, payload);

        ByteBuffer buffer = ByteBuffer.allocate(64);
        header.encode(buffer);
        buffer.flip();

        PacketHeader decodedHeader = PacketHeader.decode(buffer);
        assertEquals(header.getSequenceId(), decodedHeader.getSequenceId());
        assertEquals(header.getPayloadLength(), decodedHeader.getPayloadLength());
        assertEquals(header.getPacketType(), decodedHeader.getPacketType());
        assertEquals(header.getChecksum(), decodedHeader.getChecksum());

        assertTrue(engine.submit(packet));
        assertEquals(1, engine.getQueueSize());
    }

    @Test
    @DisplayName("Edge Case: Maximum allowable payload boundary assertions")
    void testMaximumPayloadBoundary() {
        int maxPayloadSize = 65535;
        byte[] payload = new byte[maxPayloadSize];
        Arrays.fill(payload, (byte) 0xAA);
        short checksum = Packet.computeChecksum(payload);
        PacketHeader header = new PacketHeader(Long.MAX_VALUE, maxPayloadSize, PacketType.DATA, checksum);
        
        Packet packet = new Packet(header, payload);
        assertEquals(maxPayloadSize, packet.getPayload().length);
        assertEquals(Long.MAX_VALUE, packet.getHeader().getSequenceId());
    }

    @Test
    @DisplayName("Boundary: Buffer underflow on header decode throws IllegalArgumentException")
    void testHeaderDecodeUnderflow() {
        ByteBuffer incompleteBuffer = ByteBuffer.allocate(10); // Header requires 19 bytes
        incompleteBuffer.putLong(123L);
        incompleteBuffer.put((byte) 0xFF);
        incompleteBuffer.flip();

        assertThrows(IllegalArgumentException.class, () -> PacketHeader.decode(incompleteBuffer));
    }

    @Test
    @DisplayName("Boundary: Packet instantiation with mismatched payload length fails validation")
    void testPayloadLengthMismatchInvariant() {
        byte[] payload = new byte[10];
        PacketHeader header = new PacketHeader(1L, 20, PacketType.DATA, (short) 0); // Mismatched length

        assertThrows(IllegalArgumentException.class, () -> new Packet(header, payload));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    @DisplayName("Boundary: Engine instantiation with invalid capacity throws exception")
    void testInvalidEngineCapacity(int invalidCapacity) {
        assertThrows(IllegalArgumentException.class, () -> new PacketEngine(invalidCapacity));
    }

    @Test
    @DisplayName("Edge Case: Queue full behavior explicitly drops packets and increments metrics")
    void testQueueCapacityExhaustion() {
        int smallCapacity = 4;
        PacketEngine smallEngine = new PacketEngine(smallCapacity);
        byte[] dummy = new byte[1];

        for (int i = 0; i < smallCapacity; i++) {
            Packet p = new Packet(new PacketHeader(i, 1, PacketType.DATA, (short)0), dummy);
            assertTrue(smallEngine.submit(p));
        }

        // Exceed capacity
        Packet overflowPacket = new Packet(new PacketHeader(99L, 1, PacketType.DATA, (short)0), dummy);
        assertFalse(smallEngine.submit(overflowPacket));
        assertEquals(1, smallEngine.getDroppedCount());
        assertEquals(smallCapacity, smallEngine.getQueueSize());
    }

    // =========================================================================
    // 2. MULTI-THREADED CONTENTION & RACE CONDITION STRESS TESTING
    // =========================================================================

    @Test
    @DisplayName("Concurrency: High-contention multi-producer and multi-consumer stress test")
    void testConcurrentProducersAndConsumersStress() throws InterruptedException {
        int threadCount = 16;
        int packetsPerThread = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount * 2);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount * 2);

        AtomicInteger totalProduced = new AtomicInteger();
        AtomicInteger totalConsumed = new AtomicInteger();

        // Spawn Producers
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    byte[] payload = new byte[]{(byte) threadId};
                    for (int i = 0; i < packetsPerThread; i++) {
                        PacketHeader hdr = new PacketHeader((threadId * packetsPerThread) + i, 1, PacketType.DATA, (short) 0);
                        Packet pkt = new Packet(hdr, payload);
                        if (engine.submit(pkt)) {
                            totalProduced.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    fail("Producer failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Spawn Consumers
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (true) {
                        Packet pkt = engine.poll(10, TimeUnit.MILLISECONDS);
                        if (pkt != null) {
                            totalConsumed.incrementAndGet();
                        } else {
                            // If producers are done and queue is empty, exit loop
                            if (doneLatch.getCount() <= threadCount && engine.getQueueSize() == 0) {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    fail("Consumer failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Unleash