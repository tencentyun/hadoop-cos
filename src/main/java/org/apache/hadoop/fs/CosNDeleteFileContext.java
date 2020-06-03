package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CosNDeleteFileContext {
    private final ReentrantLock lock = new ReentrantLock();
    private Condition readyCondition = lock.newCondition();

    private AtomicBoolean deleteSuccess = new AtomicBoolean(true);
    private AtomicInteger deletesFinish = new AtomicInteger(0);

    private IOException deleteException = null;

    public void lock() {
        this.lock.lock();
    }

    public void unlock() {
        this.lock.unlock();
    }

    public IOException getIOException() {
        // todo whether need to sprite the exception from the delete interface
        return this.deleteException;
    }

    public void setIOException(IOException e) {
        this.deleteException = e;
    }

    public boolean hasException() {
        if (this.deleteException != null) {
            return true;
        }
        return false;
    }

    public void awaitAllFinish(int deletesFinish) throws InterruptedException {
        while (this.deletesFinish.get() != deletesFinish) {
            this.readyCondition.await();
        }
    }

    public void signalAll() {
        this.readyCondition.signalAll();
    }

    public boolean isDeleteSuccess() {
        return this.deleteSuccess.get();
    }

    public void setDeleteSuccess(boolean deleteSuccess) {
        this.deleteSuccess.set(deleteSuccess);
    }

    public void incDeletesFinish() {
        this.deletesFinish.addAndGet(1);
    }
}

