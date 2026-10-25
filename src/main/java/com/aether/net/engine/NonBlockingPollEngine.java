package com.aether.net.engine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class NonBlockingPollEngine implements Runnable {

    private final Selector selector;
    private final ServerSocketChannel serverChannel;
    private final LockFreeRingBuffer ingressRing;
    private final LockFreeRingBuffer egressRing;
    private final MemoryArena memoryArena;
    private final SlidingWindowFlowController flowController;
    private final AtomicBoolean running;
    private final AtomicBoolean shutdownCompleted;
    private volatile Thread engineThread;
    
    private static final int MAX_SPINS = 100;
    private static final int MAX_YIELDS = 20;

    public NonBlockingPollEngine(
            int port,
            LockFreeRingBuffer ingressRing,
            LockFreeRingBuffer egressRing,
            MemoryArena memoryArena,
            SlidingWindowFlowController flowController) throws IOException {
        this.ingressRing = ingressRing;
        this.egressRing = egressRing;
        this.memoryArena = memoryArena;
        this.flowController = flowController;
        this.running = new AtomicBoolean(false);
        this.shutdownCompleted = new AtomicBoolean(false);

        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        this.serverChannel.configureBlocking(false);
        this.serverChannel.bind(new InetSocketAddress(port));
        this.serverChannel.register(this.selector, SelectionKey.op_accept);
    }

    public void start() {
        if (this.running.compareAndSet(false, true)) {
            this.engineThread = new Thread(this, "aether-poll-engine");
            this.engineThread.setDaemon(true);
            this.engineThread.start();
        }
    }

    public void shutdown() {
        if (this.running.compareAndSet(true, false)) {
            this.selector.wakeup();
        }
    }

    public boolean isShutdownCompleted() {
        return this.shutdownCompleted.get();
    }

    @Override
    public void run() {
        this.engineThread = Thread.currentThread();
        int spinCount = 0;
        int yieldCount = 0;

        try {
            while (this.running.get()) {
                boolean workDone = false;

                workDone |= pollEgressQueue();
                workDone |= pollNetworkEvents();

                if (workDone) {
                    spinCount = 0;
                    yieldCount = 0;
                } else {
                    if (spinCount < MAX_SPINS) {
                        spinCount++;
                        Thread.onSpinWait();
                    } else if (yieldCount < MAX_YIELDS) {
                        yieldCount++;
                        Thread.yield();
                    } else {
                        if (this.selector.keys().size() <= 1) {
                            LockSupport.parkNanos(1000L);
                        } else {
                            int selected = this.selector.selectNow();
                            if (selected > 0) {
                                spinCount = 0;
                                yieldCount = 0;
                            }
                        }
                    }
                }
            }

            drainAndShutdown();

        } catch (Throwable t) {
            System.err.println("[NonBlockingPollEngine] Fatal error in polling loop: " + t.getMessage());
            t.printStackTrace();
        } finally {
            closeResources();
            this.shutdownCompleted.set(true);
        }
    }

    private boolean pollNetworkEvents() throws IOException {
        int readyChannels = this.selector.selectNow();
        if (readyChannels == 0) {
            return false;
        }

        Set<SelectionKey> selectedKeys = this.selector.selectedKeys();
        Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

        while (keyIterator.hasNext()) {
            SelectionKey key = keyIterator.next();
            keyIterator.remove();

            if (!key.isValid()) {
                continue;
            }

            try {
                if (key.isAcceptable()) {
                    handleAccept();
                } else if (key.isReadable()) {
                    handleRead(key);
                } else if (key.isWritable()) {
                    handleWrite(key);
                }
            } catch (IOException e) {
                closeChannelKey(key);
            }
        }

        return true;
    }

    private void handleAccept() throws IOException {
        SocketChannel clientChannel = this.serverChannel.accept();
        if (clientChannel != null) {
            clientChannel.configureBlocking(false);
            clientChannel.register(this.selector, SelectionKey.op_read);
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        
        long blockAddress = this.memoryArena.acquireBlock();
        if (blockAddress == 0L) {
            return;
        }

        ByteBuffer directBuffer = MemoryArena.viewDirectBuffer(blockAddress, MemoryArena.BLOCK_SIZE);
        directBuffer.clear();

        int bytesRead = channel.read(directBuffer);
        if (bytesRead == -1) {
            this.memoryArena.releaseBlock(blockAddress);
            closeChannelKey(key);
            return;
        }

        if (bytesRead > 0) {
            if (!this.flowController.tryAcquire(bytesRead)) {
                this.memoryArena.releaseBlock(blockAddress);
                return;
            }

            long packetEnvelope = encodePacketEnvelope(blockAddress, bytesRead);
            boolean offered = this.ingressRing.tryOffer(packetEnvelope);
            if (!offered) {
                this.flowController.release(bytesRead);
                this.memoryArena.releaseBlock(blockAddress);
            }
        } else {
            this.memoryArena.releaseBlock(blockAddress);
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        Attachment attachment = (Attachment) key.attachment();
        if (attachment == null || attachment.writeBuffer == null) {
            key.interestOps(SelectionKey.op_read);
            key.attach(null);
            return;
        }

        ByteBuffer buf = attachment.writeBuffer;
        channel.write(buf);

        if (!buf.hasRemaining()) {
            this.memoryArena.releaseBlock(attachment.blockAddress);
            key.attach(null);
            key.interestOps(SelectionKey.op_read);
        }
    }

    private boolean pollEgressQueue() {
        long packetEnvelope = this.egressRing.tryPoll();
        if (packetEnvelope == 0L) {
            return false;
        }

        long blockAddress = decodeBlockAddress(packetEnvelope);
        int length = decodeLength(packetEnvelope);

        ByteBuffer directBuffer = MemoryArena.viewDirectBuffer(blockAddress, length);
        directBuffer.limit(length);
        directBuffer.position(0);

        boolean dispatched = false;
        for (SelectionKey key : this.selector.keys()) {
            SelectableChannel channel = key.channel();
            if (channel instanceof SocketChannel) {
                SocketChannel socketChannel = (SocketChannel) channel;
                try {
                    int written = socketChannel.write(directBuffer);
                    if (written < length) {
                        ByteBuffer remainingBuffer = ByteBuffer.allocateDirect(length - written);
                        remainingBuffer.put(directBuffer);
                        remainingBuffer.flip();
                        
                        key.attach(new Attachment(blockAddress, remainingBuffer));
                        key.interestOps(SelectionKey.op_read | SelectionKey.op_write);
                    } else {
                        this.memoryArena.releaseBlock(blockAddress);
                    }
                    dispatched = true;
                    break;
                } catch (IOException e) {
                    closeChannelKey(key);
                }
            }
        }

        if (!dispatched) {
            this.memoryArena.releaseBlock(blockAddress);
        }

        this.flowController.release(length);
        return true;
    }

    private void drainAndShutdown() {
        while (pollEgressQueue()) {
            Thread.onSpinWait();
        }
        
        long ingressEnvelope;
        while ((ingressEnvelope = this.ingressRing.tryPoll()) != 0L) {
            long blockAddress = decodeBlockAddress(ingressEnvelope);
            this.memoryArena.releaseBlock(blockAddress);
        }
    }

    private void closeChannelKey(SelectionKey key) {
        try {
            key.channel().close();
        } catch (IOException ignored) {
        }
        Object attachment = key.attachment();
        if (attachment instanceof Attachment) {
            Attachment att = (Attachment) attachment;
            if (att.blockAddress != 0L) {
                this.memoryArena.releaseBlock(att.blockAddress);
            }
        }
        key.cancel();
    }

    private void closeResources() {
        try {
            for (SelectionKey key : this.selector.keys()) {
                closeChannelKey(key);
            }
            this.selector.close();
            this.serverChannel.close();
        } catch (IOException ignored) {
        }
    }

    private static long encodePacketEnvelope(long blockAddress, int length) {
        return (blockAddress & 0xFFFFFFFFFFFFL) | ((long) (length & 0xFFFF) << 48);
    }

    private static long decodeBlockAddress(long envelope) {
        return envelope & 0xFFFFFFFFFFFFL;
    }

    private static int decodeLength(long envelope) {
        return (int) ((envelope >>> 48) & 0xFFFF);
    }

    private static final class Attachment {
        final long blockAddress;
        final ByteBuffer writeBuffer;

        Attachment(long blockAddress, ByteBuffer writeBuffer) {
            this.blockAddress = blockAddress;
            this.writeBuffer = writeBuffer;
        }
    }
}