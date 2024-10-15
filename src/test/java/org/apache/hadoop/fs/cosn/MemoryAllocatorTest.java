package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.cosn.MemoryAllocator.Memory;
import org.junit.Test;

import java.lang.reflect.Executable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemoryAllocatorTest {


    @Test
    public void test1() throws InterruptedException, CosNOutOfMemoryException {
        final MemoryAllocator memoryAllocator = MemoryAllocator.Factory.create(300 * 1024 * 1024);
        List<Memory> memory = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            memory.add(memoryAllocator.allocate(1024 * 1024));
        }

        long t1 = System.currentTimeMillis();

        try {
            memoryAllocator.allocate(1024 * 1024, 1, TimeUnit.SECONDS);
        } catch (CosNOutOfMemoryException e) {
            // ignore
        }

        long t2 = System.currentTimeMillis();
        assertTrue(t2 - t1 >= 1000 && t2 - t1 <= 1100);

        memory.remove(0).free();
        memory.add(memoryAllocator.allocate(1024 * 1024, 1, TimeUnit.SECONDS));


        memory.remove(0);
        try {
            memoryAllocator.allocate(1024 * 1024, 0, TimeUnit.SECONDS);
        } catch (CosNOutOfMemoryException e) {
            // ignore
        }

        System.gc();
        System.runFinalization();
        memory.add(memoryAllocator.allocate(1024 * 1024, 10, TimeUnit.MILLISECONDS));

        assertEquals(memoryAllocator.getTotalBytes(), memoryAllocator.getAllocatedBytes());

        memory.clear();
        System.gc();
        System.runFinalization();
        Thread.sleep(100);
        assertEquals(0, memoryAllocator.getAllocatedBytes());
    }

    @Test
    public void test2() {
        MemoryAllocator unboundedMemoryAllocator = MemoryAllocator.Factory.create(-1);
        assertEquals(-1, unboundedMemoryAllocator.getTotalBytes());
        assertTrue(unboundedMemoryAllocator instanceof MemoryAllocator.UnboundedMemoryAllocator);


        MemoryAllocator boundedMemoryAllocator = MemoryAllocator.Factory.create(100);
        assertEquals(100, boundedMemoryAllocator.getTotalBytes());
        assertTrue(boundedMemoryAllocator instanceof MemoryAllocator.BoundedMemoryAllocator);

        try {
            MemoryAllocator.Factory.create(0);
        } catch (IllegalArgumentException e) {
        }
    }


}