package com.aether.net.core;

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * High-performance off-heap PacketHeader utilizing direct Unsafe memory access.
 * Designed for zero GC overhead, flyweight parsing, and strict memory layout alignment
 * to support ultra-low-latency networking in the aether-packet-engine.
 */
public final class PacketHeader {

    // Memory layout offsets (in bytes)
    public static final int OFFSET_MAGIC       = 0;  // 4 bytes: Magic number for validation
    public static final int OFFSET_VERSION     = 4;  // 1 byte: Protocol version
    public static final int OFFSET_FLAGS       = 5;  // 1 byte: Bitfield flags
    public static final int OFFSET_STREAM_ID   = 6;  // 2 bytes: Stream identifier
    public static final int OFFSET_SEQUENCE    = 8;  // 8 bytes: Monotonic sequence number
    public static final int OFFSET_TIMESTAMP   = 16; // 8 bytes: High-resolution timestamp
    public static final int OFFSET_PAYLOAD_LEN = 24; // 4 bytes: Payload length in bytes
    public static final int OFFSET_CHECKSUM    = 28; // 4 bytes: Header/Payload checksum

    // Total byte size of the packet header
    public static final int BYTES = 32;

    // Expected magic number for protocol validation
    public static final int EXPECTED_MAGIC = 0xAE7HER01;

    // Flag masks
    public static final int FLAG_RETRANSMIT = 0x01;
    public static final int FLAG_POISON_PILL = 0x02;
    public static final int FLAG_URGENT      = 0x04;
    public static final int FLAG_ENCRYPTED   = 0x08;

    private static final sun.misc.Unsafe UNSAFE;

    static {
        try {
            java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (sun.misc.Unsafe) field.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private long memoryAddress;
    private boolean isOwner;

    /**
     * Constructs a flyweight PacketHeader wrapping an existing raw memory address.
     * 
     * @param memoryAddress The absolute off-heap memory address.
     */
    public PacketHeader(long memoryAddress) {
        if (memoryAddress == 0L) {
            throw new IllegalArgumentException("Memory address cannot be null (0L).");
        }
        this.memoryAddress = memoryAddress;
        this.isOwner = false;
    }

    /**
     * Private constructor for allocation factory method.
     */
    private PacketHeader(long memoryAddress, boolean isOwner) {
        this.memoryAddress = memoryAddress;
        this.isOwner = isOwner;
    }

    /**
     * Allocates a new PacketHeader off-heap.
     * 
     * @return A newly allocated PacketHeader instance owning its memory block.
     */
    public static PacketHeader allocate() {
        long address = UNSAFE.allocateMemory(BYTES);
        // Zero-initialize the memory block for safety
        UNSAFE.setMemory(address, BYTES, (byte) 0);
        return new PacketHeader(address, true);
    }

    /**
     * Reclaims the off-heap memory if this instance is the owner.
     */
    public void release() {
        if (isOwner && memoryAddress != 0L) {
            long addr = memoryAddress;
            memoryAddress = 0L;
            isOwner = false;
            UNSAFE.freeMemory(addr);
        }
    }

    /**
     * Binds this flyweight accessor to a new memory address without reallocation.
     * 
     * @param newAddress The new raw memory address.
     */
    public void wrap(long newAddress) {
        if (newAddress == 0L) {
            throw new IllegalArgumentException("Cannot wrap null memory address.");
        }
        if (isOwner) {
            throw new IllegalStateException("Cannot rewrap an allocating instance without releasing first.");
        }
        this.memoryAddress = newAddress;
    }

    public long address() {
        return memoryAddress;
    }

    // --- Accessors and Mutators (Hot-path optimized) ---

    public int getMagic() {
        return UNSAFE.getInt(memoryAddress + OFFSET_MAGIC);
    }

    public void setMagic(int magic) {
        UNSAFE.putInt(memoryAddress + OFFSET_MAGIC, magic);
    }

    public byte getVersion() {
        return UNSAFE.getByte(memoryAddress + OFFSET_VERSION);
    }

    public void setVersion(byte version) {
        UNSAFE.putByte(memoryAddress + OFFSET_VERSION, version);
    }

    public byte getFlags() {
        return UNSAFE.getByte(memoryAddress + OFFSET_FLAGS);
    }

    public void setFlags(byte flags) {
        UNSAFE.putByte(memoryAddress + OFFSET_FLAGS, flags);
    }

    public boolean isFlagSet(byte flagMask) {
        return (getFlags() & flagMask) == flagMask;
    }

    public void setFlag(byte flagMask, boolean enable) {
        byte current = getFlags();
        if (enable) {
            setFlags((byte) (current | flagMask));
        } else {
            setFlags((byte) (current & ~flagMask));
        }
    }

    public short getStreamId() {
        return UNSAFE.getShort(memoryAddress + OFFSET_STREAM_ID);
    }

    public void setStreamId(short streamId) {
        UNSAFE.putShort(memoryAddress + OFFSET_STREAM_ID, streamId);
    }

    public long getSequence() {
        return UNSAFE.getLong(memoryAddress + OFFSET_SEQUENCE);
    }

    public void setSequence(long sequence) {
        UNSAFE.putLong(memoryAddress + OFFSET_SEQUENCE, sequence);
    }

    public long getTimestamp() {
        return UNSAFE.getLong(memoryAddress + OFFSET_TIMESTAMP);
    }

    public void setTimestamp(long timestamp) {
        UNSAFE.putLong(memoryAddress + OFFSET_TIMESTAMP, timestamp);
    }

    public int getPayloadLength() {
        return UNSAFE.getInt(memoryAddress + OFFSET_PAYLOAD_LEN);
    }

    public void setPayloadLength(int payloadLength) {
        if (payloadLength < 0) {
            throw new IllegalArgumentException("Payload length cannot be negative.");
        }
        UNSAFE.putInt(memoryAddress + OFFSET_PAYLOAD_LEN, payloadLength);
    }

    public int getChecksum() {
        return UNSAFE.getInt(memoryAddress + OFFSET_CHECKSUM);
    }

    public void setChecksum(int checksum) {
        UNSAFE.putInt(memoryAddress + OFFSET_CHECKSUM, checksum);
    }

    /**
     * Validates structural invariants of the header.
     * 
     * @return true if the magic number matches expectations and payload length is non-negative.
     */
    public boolean validate() {
        if (getMagic() != EXPECTED_MAGIC) {
            return false;
        }
        if (getPayloadLength() < 0) {
            return false;
        }
        return true;
    }

    /**
     * Copies header data from another raw memory source.
     * 
     * @param sourceAddress Source memory address.
     */
    public void copyFrom(long sourceAddress) {
        UNSAFE.copyMemory(sourceAddress, this.memoryAddress, BYTES);
    }

    /**
     * Copies header data to another raw memory destination.
     * 
     * @param destAddress Destination memory address.
     */
    public void copyTo(long destAddress) {
        UNSAFE.copyMemory(this.memoryAddress, destAddress, BYTES);
    }
}