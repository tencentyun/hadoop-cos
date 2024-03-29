package org.apache.hadoop.fs.cosn;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;


/**
 * 固定容量的内存分配器
 * 可以指定容量，之后通过内存分配器分配内存，确保内存使用量可控，以防发生OOM
 */
@ThreadSafe
public interface MemoryAllocator {

    /**
     * 分配size大小的内存，超时时间120天
     * 注意：不要使用强引用指向其底层数组array()，避免内存无法被gc释放
     *
     * @param size 申请的内存大小
     * @return 包装有byte数组的Memory对象，当此对象强引用不可达时，会被GC回收。（更快的回收方式是直接调用free方法）
     * Memory对象自动释放计数仅限capacity大于0的场景，因为小于0的场景没有意义。
     * @throws CosNOutOfMemoryException 分配器可分配空间不足时抛出
     * @throws InterruptedException 等待过程中被中断抛出
     */

    Memory allocate(int size) throws CosNOutOfMemoryException, InterruptedException;


    /**
     * @param size    申请的内存大小
     * @param timeout 超时时间
     * @param unit    超时时间单位
     * @return 包装有byte数组的Memory对象，当此对象强引用不可达时，会被GC回收。（更快的回收方式是直接调用free方法）
     * Memory对象自动释放计数仅限capacity大于0的场景，因为小于0的场景没有意义。
     * @throws CosNOutOfMemoryException 分配器可分配空间不足时抛出
     * @throws InterruptedException 等待过程中被中断抛出
     */
    Memory allocate(int size, long timeout, TimeUnit unit)
        throws CosNOutOfMemoryException, InterruptedException;

    long getTotalBytes();

    long getAllocatedBytes();

    long getAvailableBytes();

    long getCount();

    long getFailedCount();

    class Factory {
        /**
         * @param capacity 容量（单位Bytes）
         */
        public static MemoryAllocator create(long capacity) {
            if (capacity < 0) {
                return new UnboundedMemoryAllocator();
            } else {
                return new BoundedMemoryAllocator(capacity);
            }
        }
    }

    class UnboundedMemoryAllocator implements MemoryAllocator {

        private UnboundedMemoryAllocator() {

        }

        @Override
        public long getTotalBytes() {
            return -1;
        }

        @Override
        public long getAllocatedBytes() {
            return -1;
        }

        @Override
        public long getAvailableBytes() {
            return -1;
        }

        @Override
        public long getCount() {
            return -1;
        }

        @Override
        public long getFailedCount() {
            return -1;
        }

        @Override
        public Memory allocate(int size) throws CosNOutOfMemoryException, InterruptedException {
            return new Memory(size);
        }

        @Override
        public Memory allocate(int size, long timeout, TimeUnit unit)
            throws CosNOutOfMemoryException, InterruptedException {
            return allocate(size);
        }
    }

    class Memory {
        private byte[] array;

        private Memory(int size) {
            array = new byte[size];
        }

        /**
         * Returns the underlying byte array.
         *
         * @return The underlying byte array.
         */
        public byte[] array() {
            return array;
        }

        public void free() {
            array = null;
        }
    }

    class BoundedMemoryAllocator implements MemoryAllocator {

        private static final Logger LOG = LoggerFactory.getLogger(BoundedMemoryAllocator.class);

        private final long totalBytes;
        private final AtomicLong failedCount;
        private final ReferenceQueue<Memory> refQueue;
        private volatile long availableBytes;
        private final Map<Reference<?>, Integer> refPageMap;
        private final Lock lock;
        private final Condition condition;

        private final ExecutorService cleaner;

        private BoundedMemoryAllocator(long capacity) {
            if (capacity == 0) {
                throw new IllegalArgumentException("capacity must be larger than 0.");
            }
            totalBytes = capacity;
            failedCount = new AtomicLong();
            refQueue = new ReferenceQueue<>();
            availableBytes = totalBytes;
            refPageMap = new ConcurrentHashMap<>();
            lock = new ReentrantLock();
            condition = lock.newCondition();
            cleaner = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("MemoryAllocator-Cleaner");
                    thread.setPriority(Thread.MAX_PRIORITY);
                    thread.setDaemon(true);
                    return thread;
                }
            });
            startCleaner();
        }

        public long getTotalBytes() {
            return totalBytes;
        }

        public long getAllocatedBytes() {
            return totalBytes - availableBytes;
        }

        public long getAvailableBytes() {
            return availableBytes;
        }

        public long getCount() {
            return refPageMap.size();
        }

        public long getFailedCount() {
            return failedCount.get();
        }


        public Memory allocate(int size) throws CosNOutOfMemoryException, InterruptedException {
            return allocate(size, 120, TimeUnit.DAYS);
        }


        public Memory allocate(int size, long timeout, TimeUnit unit)
            throws CosNOutOfMemoryException, InterruptedException {
            LOG.debug("try allocating memory: " + size);
            if (!tryRequest(size, timeout, unit)) {
                failedCount.incrementAndGet();
                throw new CosNOutOfMemoryException("allocate failed. available space not enough.");
            }

            AutoFreeMemory memory = new AutoFreeMemory(size);
            PhantomReference<AutoFreeMemory> phantomReference =
                new PhantomReference<>(memory, refQueue);
            memory.phantomReference = phantomReference;
            refPageMap.put(phantomReference, size);

            LOG.debug("allocated memory, size: " + size);
            return memory;
        }

        private static class AutoFreeMemory extends Memory {
            private PhantomReference<AutoFreeMemory> phantomReference;

            public AutoFreeMemory(int size) {
                super(size);
            }

            @Override
            public synchronized void free() {
                super.free();
                if (phantomReference != null) {
                    phantomReference.enqueue();
                    phantomReference = null;
                }
            }
        }


        private boolean tryRequest(int size, long timeout, TimeUnit unit)
            throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            lock.lockInterruptibly();
            try {
                while (availableBytes < size) {
                    if (nanos <= 0) {
                        return false;
                    }
                    nanos = condition.awaitNanos(nanos);
                }
                availableBytes -= size;
            } finally {
                lock.unlock();
            }
            return true;
        }

        private void startCleaner() {
            cleaner.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            // Wait for a Ref, with a timeout to avoid getting hung
                            // due to a race with clear/clean
                            Reference<?> reference = refQueue.remove(60 * 1000);
                            if (reference == null) {
                                continue;
                            }
                            reference.clear();
                            int size = refPageMap.remove(reference);
                            LOG.debug("release a read buffer, page: " + size);
                            lock.lock();
                            try {
                                availableBytes += size;
                                condition.signalAll();
                            } finally {
                                lock.unlock();
                            }
                        } catch (InterruptedException ignored) {
                            LOG.debug("received a interrupt, ignored");
                        } catch (Throwable e) {
                            // ignore exceptions from the cleanup action
                            // (including interruption of cleanup thread)
                        }
                    }
                }
            });
        }

    }

}
