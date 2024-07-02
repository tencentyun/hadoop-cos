package org.apache.hadoop.fs.cosn;

import org.apache.hadoop.fs.cosn.MemoryAllocator.Memory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

class MemoryAllocatorTest {


    @Test
    public void test1() throws InterruptedException, CosNOutOfMemoryException {
        final MemoryAllocator memoryAllocator = MemoryAllocator.Factory.create(300 * 1024 * 1024);
        List<Memory> memory = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            memory.add(memoryAllocator.allocate(1024 * 1024));
        }

        long t1 = System.currentTimeMillis();
        Assertions.assertThrowsExactly(CosNOutOfMemoryException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                memoryAllocator.allocate(1024 * 1024, 1, TimeUnit.SECONDS);
            }
        });
        long t2 = System.currentTimeMillis();
        Assertions.assertTrue(t2 - t1 >= 1000 && t2 - t1 <= 1100);

        memory.remove(0).free();
        memory.add(memoryAllocator.allocate(1024 * 1024, 1, TimeUnit.SECONDS));


        memory.remove(0);
        Assertions.assertThrowsExactly(CosNOutOfMemoryException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                memoryAllocator.allocate(1024 * 1024, 0, TimeUnit.SECONDS);
            }
        });
        System.gc();
        System.runFinalization();
        memory.add(memoryAllocator.allocate(1024 * 1024, 10, TimeUnit.MILLISECONDS));

        Assertions.assertEquals(memoryAllocator.getTotalBytes(), memoryAllocator.getAllocatedBytes());

        memory.clear();
        System.gc();
        System.runFinalization();
        Thread.sleep(100);
        Assertions.assertEquals(0, memoryAllocator.getAllocatedBytes());
    }

    @Test
    public void test2() {
        MemoryAllocator unboundedMemoryAllocator = MemoryAllocator.Factory.create(-1);
        Assertions.assertEquals(-1, unboundedMemoryAllocator.getTotalBytes());
        Assertions.assertInstanceOf(MemoryAllocator.UnboundedMemoryAllocator.class, unboundedMemoryAllocator);


        MemoryAllocator boundedMemoryAllocator = MemoryAllocator.Factory.create(100);
        Assertions.assertEquals(100, boundedMemoryAllocator.getTotalBytes());
        Assertions.assertInstanceOf(MemoryAllocator.BoundedMemoryAllocator.class, boundedMemoryAllocator);

        Assertions.assertThrowsExactly(IllegalArgumentException.class, new Executable() {
            @Override
            public void execute() throws Throwable {
                MemoryAllocator.Factory.create(0);
            }
        });


    }


}