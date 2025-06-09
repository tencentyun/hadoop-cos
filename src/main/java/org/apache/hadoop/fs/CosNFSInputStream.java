package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.cosn.MemoryAllocator;
import org.apache.hadoop.fs.cosn.CosNOutOfMemoryException;
import org.apache.hadoop.fs.cosn.ReadBufferHolder;
import org.apache.hadoop.fs.cosn.cache.FragmentCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;

import static org.apache.hadoop.fs.CosNConfigKeys.DEFAULT_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS;


public class CosNFSInputStream extends FSInputStream {
    public static final Logger LOG =
            LoggerFactory.getLogger(CosNFSInputStream.class);

    public static class ReadBuffer {
        public static final int INIT = 1;
        public static final int SUCCESS = 0;
        public static final int ERROR = -1;

        private final Lock lock = new ReentrantLock();
        private Condition readyCondition = lock.newCondition();
        private MemoryAllocator.Memory memory;
        private int status;
        private long start;
        private long end;
        private IOException exception;

        public ReadBuffer(long start, long end) {
            this.start = start;
            this.end = end;
            this.status = INIT;
            this.exception = null;
        }

        public void lock() {
            this.lock.lock();
        }

        public void unLock() {
            this.lock.unlock();
        }

        public void await(int waitStatus) throws InterruptedException {
            while (this.status == waitStatus) {
                readyCondition.await();
            }
        }

        public void signalAll() {
            readyCondition.signalAll();
        }

        @Nullable
        public byte[] getBuffer() {
            final MemoryAllocator.Memory finalMemory = memory;
            if (finalMemory == null) {
                return null;
            }
            return finalMemory.array();
        }

        public int length() {
            return (int) (end - start + 1);
        }

        public int getStatus() {
            return this.status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public void setException(IOException e) {
            this.exception = e;
        }

        public IOException getException() {
            return this.exception;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public void allocate(long timeout, TimeUnit unit) throws CosNOutOfMemoryException, InterruptedException {
            this.memory = ReadBufferHolder.getBufferAllocator()
                .allocate(this.length(), timeout, unit);
        }

        public void free() {
            if (this.memory != null) {
                this.memory.free();
                this.memory = null;
            }
        }
    }

    private enum ReopenLocation {
        PRE_READ_QUEUE,
        PREVIOUS_BUFFER,
        LOCAL_CACHE,
        NONE
    }

    private FileSystem.Statistics statistics;
    private final Configuration conf;
    private final NativeFileSystemStore store;
    private final String key;
    private long position;
    private long nextPos;
    private long lastByteStart;
    private final FileStatus fileStatus;
    private long partRemaining;
    private long bufferStart;
    private long bufferEnd;
    private final long preReadPartSize;
    private final int maxReadPartNumber;
    private ReadBuffer currentReadBuffer;
    private final AtomicBoolean closed;
    private final int socketErrMaxRetryTimes;

    private final ExecutorService readAheadExecutorService;
    private final Deque<ReadBuffer> readBufferQueue;
    // 设置一个 Previous buffer 用于暂存淘汰出来的队头元素，用以优化小范围随机读的性能
    private ReadBuffer previousReadBuffer;

    private final FragmentCache fragmentCache;

    private ReopenLocation reopenLocation = ReopenLocation.NONE;

    /**
     * Input Stream
     *
     * @param conf config
     * @param store native file system
     * @param statistics statis
     * @param key cos key
     * @param fileStatus file status
     * @param readAheadExecutorService thread executor
     */
    public CosNFSInputStream(
            Configuration conf,
            NativeFileSystemStore store,
            FileSystem.Statistics statistics,
            String key,
            FileStatus fileStatus,
            ExecutorService readAheadExecutorService, FragmentCache fragmentCache) {
        super();
        this.conf = conf;
        this.store = store;
        this.statistics = statistics;
        this.key = key;
        this.fileStatus = fileStatus;
        this.position = 0;
        this.nextPos = 0;
        this.lastByteStart = -1;
        this.bufferStart = -1;
        this.bufferEnd = -1;
        this.preReadPartSize = conf.getLong(
                CosNConfigKeys.READ_AHEAD_BLOCK_SIZE_KEY,
                CosNConfigKeys.DEFAULT_READ_AHEAD_BLOCK_SIZE);
        this.maxReadPartNumber = conf.getInt(
                CosNConfigKeys.READ_AHEAD_QUEUE_SIZE,
                CosNConfigKeys.DEFAULT_READ_AHEAD_QUEUE_SIZE);
        this.socketErrMaxRetryTimes = conf.getInt(
                CosNConfigKeys.CLIENT_SOCKET_ERROR_MAX_RETRIES,
                CosNConfigKeys.DEFAULT_CLIENT_SOCKET_ERROR_MAX_RETRIES);
        this.readAheadExecutorService = readAheadExecutorService;
        this.readBufferQueue =
                new ArrayDeque<>(this.maxReadPartNumber);
        this.fragmentCache = fragmentCache;
        this.closed = new AtomicBoolean(false);
    }

    private void tryFreeBuffer(ReadBuffer readBuffer) {
        if (readBuffer != null
                && readBuffer != previousReadBuffer
                && readBuffer != currentReadBuffer
                && (readBufferQueue.isEmpty() || readBufferQueue.peek() != readBuffer)) {
            if (null != this.fragmentCache && readBuffer.getBuffer() != null) {
                try {
                    this.fragmentCache.put(new FragmentCache.Fragment(this.fileStatus.getPath().toString(), readBuffer.getStart(), readBuffer.getBuffer()));
                } catch (IOException e) {
                    LOG.warn("Failed to add fragment to fragmentCache", e);
                }
            }
            readBuffer.free();
        }
    }

    private void setCurrentReadBuffer(ReadBuffer readBuffer) {
        ReadBuffer readyFree = currentReadBuffer;
        currentReadBuffer = readBuffer;
        tryFreeBuffer(readyFree);
    }

    public void setPreviousReadBuffer(ReadBuffer readBuffer) {
        ReadBuffer readyFree = previousReadBuffer;
        previousReadBuffer = readBuffer;
        tryFreeBuffer(readyFree);
    }

    private synchronized void reopen(long pos) throws IOException {
        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        } else if (pos > this.fileStatus.getLen()) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }

        setCurrentReadBuffer(null);
        this.bufferStart = -1;
        this.bufferEnd = -1;

        boolean isRandomIO = true;
        if (pos == this.nextPos) {
            isRandomIO = false;
        } else {
            // 发生了随机读，针对于小范围的回溯随机读，则直接看一下是否命中了前一次刚刚被淘汰出去的队头读缓存
            // 如果不是，那么随机读只可能是发生了超出前一块范围的回溯随机读，或者是在预读队列范围或者是超出预读队列范围。
            // 如果是在预读队列范围内，那么依赖在预读队列中查找直接定位到要读的块，如果是超出预读队列范围，那么队列会被排空，然后重新定位到要读的块和位置
            if (this.reopenLocation == ReopenLocation.PREVIOUS_BUFFER
                    && null != this.previousReadBuffer
                    && pos >= this.previousReadBuffer.getStart()
                    && pos <= this.previousReadBuffer.getEnd()) {
                setCurrentReadBuffer(previousReadBuffer);
                this.bufferStart = this.previousReadBuffer.getStart();
                this.bufferEnd = this.previousReadBuffer.getEnd();
                this.position = pos;
                this.partRemaining = (this.bufferEnd - this.bufferStart + 1) - (pos - this.bufferStart);
                this.nextPos = !this.readBufferQueue.isEmpty() ? this.readBufferQueue.getFirst().getStart() : pos + this.preReadPartSize;
                return;
            }

            // 查一下是否在 local cache 中
            if (this.reopenLocation == ReopenLocation.LOCAL_CACHE
                    && null != this.fragmentCache) {
                FragmentCache.Fragment fragment = this.fragmentCache.get(this.fileStatus.getPath().toString(), pos);
                if (fragment != null) {
                    ReadBuffer readBuffer = new ReadBuffer(fragment.getStartOffsetInFile(),fragment.getStartOffsetInFile() + fragment.getContent().length - 1);
                    try {
                        readBuffer.allocate(conf.getLong(CosNConfigKeys.COSN_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS,
                                DEFAULT_READ_BUFFER_ALLOCATE_TIMEOUT_SECONDS), TimeUnit.SECONDS);
                        System.arraycopy(fragment.getContent(), 0, readBuffer.getBuffer(), 0, fragment.getContent().length);
                        readBuffer.setStatus(ReadBuffer.SUCCESS);
                        setCurrentReadBuffer(readBuffer);
                        this.bufferStart = readBuffer.getStart();
                        this.bufferEnd = readBuffer.getEnd();
                        this.position = pos;
                        this.partRemaining = (this.bufferEnd - this.bufferStart + 1) - (pos - this.bufferStart);
                        this.nextPos = !this.readBufferQueue.isEmpty() ? this.readBufferQueue.getFirst().getStart() : pos + this.preReadPartSize;
                        return;
                    } catch (Exception e) {
                        LOG.error("allocate read buffer failed.", e);
                        // continue to reopen
                    }
                }
            }
        }
        // 在预读队列里面定位到要读的块
        while (!this.readBufferQueue.isEmpty()) {
            if (pos < this.readBufferQueue.getFirst().getStart() || pos > this.readBufferQueue.getFirst().getEnd()) {
                // 定位到要读的块，同时保存淘汰出来的队头元素，供小范围的随机读回溯
                setPreviousReadBuffer(this.readBufferQueue.poll());
            } else {
                break;
            }
        }
        // 规整到队头的下一个元素的起始位置
        this.nextPos = pos + this.preReadPartSize;

        int currentBufferQueueSize = this.readBufferQueue.size();
        if (currentBufferQueueSize == 0) {
            this.lastByteStart = pos - this.preReadPartSize;
        } else {
            ReadBuffer[] readBuffers =
                    this.readBufferQueue.toArray(new ReadBuffer[currentBufferQueueSize]);
            this.lastByteStart =
                    readBuffers[currentBufferQueueSize - 1].getStart();
        }

        int maxLen = this.maxReadPartNumber - currentBufferQueueSize;
        for (int i = 0; i < maxLen && i < (currentBufferQueueSize + 1) * 2; i++) {
            if (this.lastByteStart + this.preReadPartSize * (i + 1) > this.fileStatus.getLen()) {
                break;
            }

            long byteStart = this.lastByteStart + this.preReadPartSize * (i + 1);
            long byteEnd = byteStart + this.preReadPartSize - 1;
            if (byteEnd >= this.fileStatus.getLen()) {
                byteEnd = this.fileStatus.getLen() - 1;
            }

            ReadBuffer readBuffer = new ReadBuffer(byteStart, byteEnd);
            if (readBuffer.length() == 0) {
                readBuffer.setStatus(ReadBuffer.SUCCESS);
            } else {
                this.readAheadExecutorService.execute(
                        new CosNFileReadTask(this.conf, this.key, this.store,
                                readBuffer, this.socketErrMaxRetryTimes, closed));
            }

            this.readBufferQueue.add(readBuffer);
            if (isRandomIO) {
                break;
            }
        }

        ReadBuffer readBuffer = this.readBufferQueue.peek();
        IOException innerException = null;
        readBuffer.lock();
        try {
            readBuffer.await(ReadBuffer.INIT);
            if (readBuffer.getStatus() == ReadBuffer.ERROR) {
                innerException = readBuffer.getException();
                setCurrentReadBuffer(null);
                this.bufferStart = -1;
                this.bufferEnd = -1;
                if (readBuffer.getException().getCause() instanceof CosNOutOfMemoryException) {
                    throw readBuffer.getException();
                }
                if (readBuffer.getException() instanceof AccessDeniedException) {
                    throw readBuffer.getException();
                }
            } else {
                setCurrentReadBuffer(readBuffer);
                this.bufferStart = readBuffer.getStart();
                this.bufferEnd = readBuffer.getEnd();
            }
        } catch (InterruptedException e) {
            LOG.warn("interrupted exception occurs when wait a read buffer.");
        } finally {
            readBuffer.unLock();
        }

        if (null == this.currentReadBuffer) {
            LOG.error(String.format("Null IO stream key:%s", this.key), innerException);
            throw new IOException("Null IO stream.", innerException);
        }

        this.position = pos;
        this.partRemaining = (this.bufferEnd - this.bufferStart + 1) - (pos - this.bufferStart);
    }

    @Override
    public void seek(long pos) throws IOException {
        this.checkOpened();

        if (pos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
        }
        if (pos > this.fileStatus.getLen()) {
            throw new EOFException(FSExceptionMessages.CANNOT_SEEK_PAST_EOF);
        }

        if (this.position == pos) {
            return;
        }
        if (pos >= this.bufferStart &&  pos <= this.bufferEnd) {
            // 支持块内随机读
            LOG.debug("seek cache hit last pos {}, pos {}, this buffer start {}, end {}",
                    this.position, pos, this.bufferStart, this.bufferEnd);
            this.position = pos;
            this.partRemaining = this.bufferEnd - pos + 1;
        } else if (null != this.previousReadBuffer && pos >= this.previousReadBuffer.getStart() && pos <= this.previousReadBuffer.getEnd()) {
            // 在上一次刚刚被淘汰的预读块中
            this.position = pos;
            this.partRemaining = -1;    // 触发 reopen
            this.reopenLocation = ReopenLocation.PREVIOUS_BUFFER;
        } else if (!this.readBufferQueue.isEmpty() && pos >= this.readBufferQueue.getFirst().getStart() && pos <= this.readBufferQueue.getLast().getEnd()) {
            // 在预读队列中
            this.position = pos;
            this.partRemaining = -1;    // 触发 reopen
            this.reopenLocation = ReopenLocation.PRE_READ_QUEUE;
        } else if (null != this.fragmentCache && this.fragmentCache.contains(this.fileStatus.getPath().toString(), pos)) {
            // 命中分片缓存
            this.position = pos;
            this.partRemaining = -1;    // 触发 reopen
            this.reopenLocation = ReopenLocation.LOCAL_CACHE;
        } else {
            // 既不在预读队列中，也不在上一次刚刚被淘汰的预读块和本地缓存中，那么直接定位到要读的块和位置
            this.position = pos;
            this.partRemaining = -1;
            this.reopenLocation = ReopenLocation.NONE;
        }
    }

    @Override
    public long getPos() throws IOException {
        return this.position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read() throws IOException {
        this.checkOpened();

        if (this.partRemaining <= 0 && this.position < this.fileStatus.getLen()) {
            this.reopen(this.position);
        }

        int byteRead = -1;
        if (this.partRemaining != 0) {
            byte[] buffer = currentReadBuffer.getBuffer();
            Objects.requireNonNull(buffer);
            byteRead = buffer[(int) (buffer.length - this.partRemaining)] & 0xff;
        }
        if (byteRead >= 0) {
            this.position++;
            this.partRemaining--;
            if (null != this.statistics) {
                this.statistics.incrementBytesRead(1);
            }
        }

        return byteRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        this.checkOpened();

        if (len == 0) {
            return 0;
        }

        if (off < 0 || len < 0 || len > b.length) {
            throw new IndexOutOfBoundsException();
        }

        int bytesRead = 0;
        while (position < this.fileStatus.getLen() && bytesRead < len) {
            if (partRemaining <= 0) {
                reopen(position);
            }

            int bytes = 0;
            byte[] buffer = currentReadBuffer.getBuffer();
            Objects.requireNonNull(buffer);
            for (int i = buffer.length - (int) partRemaining;
                 i < buffer.length; i++) {
                b[off + bytesRead] = buffer[i];
                bytes++;
                bytesRead++;
                if (off + bytesRead >= len) {
                    break;
                }
            }

            if (bytes > 0) {
                this.position += bytes;
                this.partRemaining -= bytes;
            } else if (this.partRemaining != 0) {
                throw new IOException("Failed to read from stream. Remaining:" +
                        " " + this.partRemaining);
            }
        }
        if (null != this.statistics && bytesRead > 0) {
            this.statistics.incrementBytesRead(bytesRead);
        }

        return bytesRead == 0 ? -1 : bytesRead;
    }

    @Override
    public int available() throws IOException {
        this.checkOpened();

        long remaining = this.fileStatus.getLen() - this.position;
        if(remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        return (int) remaining;
    }
    @Override
    public void close() throws IOException {
        if (this.closed.get()) {
            return;
        }

        this.closed.set(true);
        while (!readBufferQueue.isEmpty()) {
            readBufferQueue.poll().free();
        }
        setCurrentReadBuffer(null);
        setPreviousReadBuffer(null);
        if (null != this.fragmentCache) {
            this.fragmentCache.remove(this.fileStatus.getPath().toString());
        }
    }

    private void checkOpened() throws IOException {
        if(this.closed.get()) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }
}
