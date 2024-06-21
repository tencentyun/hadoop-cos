package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CosNCopyFileContext {
    private final ReentrantLock lock = new ReentrantLock();
    private Condition readyCondition = lock.newCondition();

    private AtomicBoolean copySuccess = new AtomicBoolean(true);
    private AtomicInteger copiesFinish = new AtomicInteger(0);

    private IOException copyException = null;

    public void lock() {
        this.lock.lock();
    }

    public void unlock() {
        this.lock.unlock();
    }

    public void awaitAllFinish(int copiesFinish) throws InterruptedException {
        while (this.copiesFinish.get() != copiesFinish && !this.hasException()) {
            this.readyCondition.await();
        }
    }

    public IOException getCopyException() {
        return copyException;
    }

    public void setCopyException(IOException copyException) {
        this.copyException = copyException;
    }

    public boolean hasException() {
        return this.copyException != null;
    }

    public void signalAll() {
        this.readyCondition.signalAll();
    }

    public boolean isCopySuccess() {
        return this.copySuccess.get();
    }

    public void setCopySuccess(boolean copySuccess) {
        this.copySuccess.set(copySuccess);
    }

    public void incCopiesFinish(int copiesFinish) {
        this.copiesFinish.addAndGet(copiesFinish);
    }

    public void incCopiesFinish() {
        this.copiesFinish.addAndGet(1);
    }
}
