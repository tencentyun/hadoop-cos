package org.apache.hadoop.fs.cosn;


public final class ReadBufferHolder {

    private static MemoryAllocator memoryAllocator;

    public static synchronized void initialize(long capacity) {
        if (memoryAllocator != null) {
            return;
        }
        if (capacity == 0) {
            capacity = (long) (Runtime.getRuntime().maxMemory() * 0.8);
        }
        memoryAllocator = MemoryAllocator.Factory.create(capacity);
    }


    public static MemoryAllocator getBufferAllocator() {
        return memoryAllocator;
    }

    public synchronized static void clear() {
        memoryAllocator = null;
    }


}
