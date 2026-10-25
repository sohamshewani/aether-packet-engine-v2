package com.aether.net.flow;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class SlidingWindowFlowController {

    private static final VarHandle HEAD_HANDLE;
    private static final VarHandle TAIL_HANDLE;
    private static final VarHandle WINDOW_SIZE_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD_HANDLE = l.findVarHandle(SlidingWindowFlowController.class, "headSequence", long.class);
            TAIL_HANDLE = l.findVarHandle(SlidingWindowFlowController.class, "tailSequence", long.class);
            WINDOW_SIZE_HANDLE = l.findVarHandle(SlidingWindowFlowController.class, "windowSize", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final long capacity;
    private final DropPolicy dropPolicy;

    @SuppressWarnings("unused")
    private volatile long headSequence;
    @SuppressWarnings("unused")
    private volatile long tailSequence;
    @SuppressWarnings("unused")
    private volatile int windowSize;

    public enum DropPolicy {
        DROP_OLDEST,
        DROP_NEWEST
    }

    public SlidingWindowFlowController(long capacity, int initialWindowSize, DropPolicy dropPolicy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be strictly positive: " + capacity);
        }
        if (initialWindowSize <= 0 || initialWindowSize > capacity) {
            throw new IllegalArgumentException("Initial window size must be within [1, capacity]");
        }
        if (dropPolicy == null) {
            throw new IllegalArgumentException("DropPolicy cannot be null");
        }

        this.capacity = capacity;
        this.dropPolicy = dropPolicy;
        this.headSequence = 0L;
        this.tailSequence = 0L;
        this.windowSize = initialWindowSize;
    }

    public boolean tryAcquire(long currentSequence) {
        while (true) {
            long currentTail = (long) TAIL_HANDLE.getAcquire(this);
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            int currentWindow = (int) WINDOW_SIZE_HANDLE.getAcquire(this);

            if (currentSequence < currentHead) {
                return false;
            }

            long inflight = currentTail - currentHead;
            if (inflight >= currentWindow || inflight >= capacity) {
                if (dropPolicy == DropPolicy.DROP_NEWEST) {
                    return false;
                } else {
                    long newHead = currentTail - currentWindow + 1;
                    if (newHead <= currentHead) {
                        newHead = currentHead + 1;
                    }
                    if (HEAD_HANDLE.compareAndSet(this, currentHead, newHead)) {
                        VarHandle.fullFence();
                    } else {
                        continue;
                    }
                }
            }

            if (TAIL_HANDLE.compareAndSet(this, currentTail, Math.max(currentTail, currentSequence + 1))) {
                VarHandle.fullFence();
                return true;
            }
        }
    }

    public void acknowledge(long acknowledgedSequence) {
        while (true) {
            long currentHead = (long) HEAD_HANDLE.getAcquire(this);
            if (acknowledgedSequence <= currentHead) {
                return;
            }
            if (HEAD_HANDLE.compareAndSet(this, currentHead, acknowledgedSequence)) {
                VarHandle.fullFence();
                return;
            }
        }
    }

    public void updateWindowSize(int newWindowSize) {
        if (newWindowSize <= 0 || newWindowSize > capacity) {
            throw new IllegalArgumentException("Window size must be within [1, capacity]");
        }
        WINDOW_SIZE_HANDLE.setRelease(this, newWindowSize);
        VarHandle.fullFence();
    }

    public long getHeadSequence() {
        return (long) HEAD_HANDLE.getAcquire(this);
    }

    public long getTailSequence() {
        return (long) TAIL_HANDLE.getAcquire(this);
    }

    public int getWindowSize() {
        return (int) WINDOW_SIZE_HANDLE.getAcquire(this);
    }

    public long getCapacity() {
        return capacity;
    }

    public DropPolicy getDropPolicy() {
        return dropPolicy;
    }

    public long getInflightCount() {
        long tail = (long) TAIL_HANDLE.getAcquire(this);
        long head = (long) HEAD_HANDLE.getAcquire(this);
        return tail >= head ? tail - head : 0L;
    }
}