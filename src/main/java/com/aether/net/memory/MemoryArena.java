package com.aether.net.memory;

import sun.misc.Unsafe;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MemoryArena implements AutoCloseable {

    private static final Unsafe UNSAFE;
    private static final VarHandle ALLOCATED_HANDLE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
            ALLOCATED_HANDLE = MethodHandles.lookup().findVarHandle(MemoryArena.class, "allocated", boolean.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long baseAddress;
    private final long totalSize;
    private final int blockSize;
    private final int blockCount;
    private final long freeListAddress;
    private final long generationTableAddress;

    @SuppressWarnings("unused")
    private volatile boolean allocated = true;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MemoryArena(int blockCount, int blockSize) {
        if (blockCount <= 0) {
            throw new IllegalArgumentException("blockCount must be positive: " + blockCount);
        }
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException("blockSize must be a positive power of two: " + blockSize);
        }
        if (blockSize < 8) {
            throw new IllegalArgumentException("blockSize must be at least 8 bytes to store free-list pointers.");
        }

        this.blockCount = blockCount;
        this.blockSize = blockSize;
        this.totalSize = (long) blockCount * blockSize;

        this.baseAddress = UNSAFE.allocateMemory(totalSize);
        long freeListBytes = (long) blockCount * 8L;
        this.freeListAddress = UNSAFE.allocateMemory(freeListBytes);

        long genTableBytes = (long) blockCount * 4L;
        this.generationTableAddress = UNSAFE.allocateMemory(genTableBytes);

        for (int i = 0; i < blockCount; i++) {
            long blockAddr = baseAddress + ((long) i * blockSize);
            long nextPtr = (i < blockCount - 1) ? (freeListAddress + ((long) (i + 1) * 8L)) : 0L;
            UNSAFE.putLong(freeListAddress + ((long) i * 8L), blockAddr);
            UNSAFE.putInt(generationTableAddress + ((long) i * 4L), 1);
        }
    }

    public long allocate() {
        if (!((boolean) ALLOCATED_HANDLE.getAcquire(this))) {
            throw new IllegalStateException("MemoryArena is closed or not allocated.");
        }

        long currentFreeListHead = UNSAFE.getLong(freeListAddress);
        if (currentFreeListHead == 0L) {
            throw new OutOfMemoryError("MemoryArena is exhausted. No free blocks available.");
        }

        long blockAddress = currentFreeListHead;
        int blockIndex = (int) ((blockAddress - baseAddress) / blockSize);

        long nextFreeAddr = UNSAFE.getLong(freeListAddress + ((long) blockIndex * 8L));
        UNSAFE.putLong(freeListAddress, nextFreeAddr);

        return blockAddress;
    }

    public void release(long address) {
        validateAddress(address);

        int blockIndex = (int) ((address - baseAddress) / blockSize);

        long genAddr = generationTableAddress + ((long) blockIndex * 4L);
        int currentGen = UNSAFE.getInt(genAddr);
        UNSAFE.putIntVolatile(null, genAddr, currentGen + 1);

        long oldHead = UNSAFE.getLong(freeListAddress);
        UNSAFE.putLong(freeListAddress + ((long) blockIndex * 8L), oldHead);
        UNSAFE.putLongVolatile(null, freeListAddress, address);
    }

    public int getGeneration(long address) {
        validateAddress(address);
        int blockIndex = (int) ((address - baseAddress) / blockSize);
        return UNSAFE.getInt(generationTableAddress + ((long) blockIndex * 4L));
    }

    private void validateAddress(long address) {
        if (!((boolean) ALLOCATED_HANDLE.getAcquire(this))) {
            throw new IllegalStateException("MemoryArena is closed.");
        }
        if (address < baseAddress || address >= baseAddress + totalSize) {
            throw new IllegalArgumentException("Address 0x" + Long.toHexString(address) + " is outside the arena range.");
        }
        if ((address - baseAddress) % blockSize != 0) {
            throw new IllegalArgumentException("Address 0x" + Long.toHexString(address) + " is not aligned to block boundaries.");
        }
    }

    public int getBlockSize() {
        return blockSize;
    }

    public int getBlockCount() {
        return blockCount;
    }

    public long getBaseAddress() {
        return baseAddress;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            ALLOCATED_HANDLE.setRelease(this, false);
            VarHandle.fullFence();
            UNSAFE.freeMemory(baseAddress);
            UNSAFE.freeMemory(freeListAddress);
            UNSAFE.freeMemory(generationTableAddress);
        }
    }
}