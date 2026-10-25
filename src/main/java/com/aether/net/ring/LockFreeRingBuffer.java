package com.aether.net.ring;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * A cache-line padded (64-byte alignment) Single-Producer-Single-Consumer (SPSC)
 * lock-free circular ring buffer backed by a power-of-two array.
 * Uses explicit volatile semantics and memory fences for cross-core synchronization
 * without locks or atomic read-modify-write loops.
 */
public final class LockFreeRingBuffer<E> {

    private static final int CACHE_LINE_SIZE = 64;
    private static final int INT_BYTES = Integer.BYTES;

    // --- Producer Cache-Line Padding & State ---
    // We pad around the producer sequence to prevent false sharing with adjacent variables.
    private long p00, p01, p02, p03, p04, p05, p06, p07;
    private volatile long producerIndex;
    private long p10, p11, p12, p13, p14, p15, p16, p17;

    // --- Consumer Cache-Line Padding & State ---
    private long c00, c01, c02, c03, c04, c05, c06, c07;
    private volatile long consumerIndex;
    private long c10, c11, c12, c13, c14, c15, c16, c17;

    // --- Shared Immutable Ring State ---
    private final int capacity;
    private final int mask;
    private final Object[] buffer;

    private static final VarHandle PRODUCER_INDEX_HANDLE;
    private static final VarHandle CONSUMER_INDEX_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            PRODUCER_INDEX_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "producerIndex", long.class);
            CONSUMER_INDEX_HANDLE = l.findVarHandle(LockFreeRingBuffer.class, "consumerIndex", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Constructs a new LockFreeRingBuffer with the specified capacity.
     * Capacity is automatically rounded up to the nearest power of two.
     *
     * @-param requestedCapacity Desired capacity of the ring buffer
     */
    public LockFreeRingBuffer(final int requestedCapacity) {
        if (requestedCapacity <= 0) {
            throw new IllegalArgumentException("Capacity must be strictly positive: " + requestedCapacity);
        }
        this.capacity = ceilingNextPowerOfTwo(requestedCapacity);
        this.mask = this.capacity - 1;
        this.buffer = new Object[this.capacity];
        
        // Initialize indices
        PRODUCER_INDEX_HANDLE.setVolatile(this, 0L);
        CONSUMER_INDEX_HANDLE.setVolatile(this, 0L);
    }

    /**
     * Attempts to offer an element to the ring buffer (Single Producer).
     * Non-blocking. Returns true if successful, false if the ring is saturated.
     *
     * @param e The non-null element to enqueue
     * @return true if enqueued successfully, false if full
     */
    public boolean offer(final E e) {
        Objects.requireNonNull(e, "Element cannot be null");

        final long currentProducerIndex = (long) PRODUCER_INDEX_HANDLE.getAcquire(this);
        // We cache or load consumer index with acquire semantics
        final long currentConsumerIndex = (long) CONSUMER_INDEX_HANDLE.getAcquire(this);

        if (currentProducerIndex - currentConsumerIndex >= capacity) {
            return false; // Ring saturated
        }

        final int index = (int) currentProducerIndex & mask;
        buffer[index] = e;

        // Publish the write using release fence / volatile write
        PRODUCER_INDEX_HANDLE.setRelease(this, currentProducerIndex + 1);
        VarHandle.fullFence();

        return true;
    }

    /**
     * Attempts to poll an element from the ring buffer (Single Consumer).
     * Non-blocking. Returns the element, or null if the ring is empty.
     *
     * @return The dequeued element, or null if empty
     */
    @SuppressWarnings("unchecked")
    public E poll() {
        final long currentConsumerIndex = (long) CONSUMER_INDEX_HANDLE.getAcquire(this);
        final long currentProducerIndex = (long) PRODUCER_INDEX_HANDLE.getAcquire(this);

        if (currentConsumerIndex == currentProducerIndex) {
            return null; // Ring empty
        }

        final int index = (int) currentConsumerIndex & mask;
        final E e = (E) buffer[index];
        buffer[index] = null; // Prevent loitering for GC / object recycling

        // Publish the read using release fence / volatile write
        CONSUMER_INDEX_HANDLE.setRelease(this, currentConsumerIndex + 1);
        VarHandle.fullFence();

        return e;
    }

    /**
     * Inspects the next element to be consumed without removing it.
     *
     * @return The peeked element, or null if empty
     */
    @SuppressWarnings("unchecked")
    public E peek() {
        final long currentConsumerIndex = (long) CONSUMER_INDEX_HANDLE.getAcquire(this);
        final long currentProducerIndex = (long) PRODUCER_INDEX_HANDLE.getAcquire(this);

        if (currentConsumerIndex == currentProducerIndex) {
            return null;
        }

        final int index = (int) currentConsumerIndex & mask;
        return (E) buffer[index];
    }

    /**
     * Returns the exact capacity of the ring buffer (always a power of two).
     *
     * @return capacity
     */
    public int capacity() {
        return this.capacity;
    }

    /**
     * Returns the current number of elements in the ring buffer.
     * Note: This is a point-in-time snapshot and may be stale immediately after read
     * due to concurrent execution.
     *
     * @return approximate current size
     */
    public int size() {
        long currentProducerIndex = (long) PRODUCER_INDEX_HANDLE.getAcquire(this);
        long currentConsumerIndex = (long) CONSUMER_INDEX_HANDLE.getAcquire(this);
        long size = currentProducerIndex - currentConsumerIndex;
        if (size < 0) {
            return 0;
        }
        return size > capacity ? capacity : (int) size;
    }

    /**
     * Checks whether the ring buffer is currently empty.
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return ((long) CONSUMER_INDEX_HANDLE.getAcquire(this)) == ((long) PRODUCER_INDEX_HANDLE.getAcquire(this));
    }

    /**
     * Checks whether the ring buffer is currently saturated to capacity.
     *
     * @return true if full, false otherwise
     */
    public boolean isFull() {
        return size() >= capacity;
    }

    /**
     * Utility method to compute the next power of two for capacity enforcement.
     *
     * @param value raw input capacity
     * @return power-of-two ceiling
     */
    private static int ceilingNextPowerOfTwo(final int value) {
        if (value <= 0) {
            return 1;
        }
        int highestBit = Integer.highestOneBit(value);
        return (value == highestBit) ? highestBit : highestBit << 1;
    }
}