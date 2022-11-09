package org.apache.hadoop.fs.cosn.multipart.upload;

import com.google.common.base.Preconditions;
import com.qcloud.cos.model.PartETag;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FileMetadata;
import org.apache.hadoop.fs.NativeFileSystemStore;
import org.apache.hadoop.fs.cosn.BufferInputStream;
import org.apache.hadoop.fs.cosn.LocalRandomAccessMappedBufferPool;
import org.apache.hadoop.fs.cosn.MD5Utils;
import org.apache.hadoop.fs.cosn.Unit;
import org.apache.hadoop.fs.cosn.buffer.CosNRandomAccessMappedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 目前暂时只给随机写流使用
 */
public class MultipartManager {
  private static final Logger LOG = LoggerFactory.getLogger(MultipartManager.class);

  // 它所能管理的最大文件大小
  private final long MAX_FILE_SIZE;
  private final long partSize;
  private final NativeFileSystemStore nativeStore;
  private final String cosKey;
  private String uploadId;
  private final List<PartETag> partETags = new ArrayList<>();
  private final List<CosNRandomAccessMappedBuffer> localParts = new ArrayList<>();
  private volatile boolean committed;
  private volatile boolean aborted;
  private volatile boolean closed;

  public MultipartManager(NativeFileSystemStore nativeStore,
    String cosKey, long partSize) {
    this.partSize = partSize;
    this.MAX_FILE_SIZE = this.partSize * 10000L;
    this.nativeStore = nativeStore;
    this.cosKey = cosKey;
    this.uploadId = null;
    this.committed = true;
    this.aborted = false;
    this.closed = false;
  }

  /**
   * 恢复当前文件的写
   *
   * @throws IOException
   */
  public void resumeForWrite() throws IOException {
    this.checkOpened();
    // 获取当前对象的长度
    FileMetadata fileMetadata = this.nativeStore.retrieveMetadata(this.cosKey);
    if (null == fileMetadata) {
      throw new FileNotFoundException(
          String.format("The cosKey [%s] is not exists.", this.cosKey));
    }
    // 对当前对象进行服务端拆块，然后就可以建立本地块索引映射
    this.splitParts(fileMetadata.getLength());
  }

  /**
   * 将当前文件拆块拆到指定长度
   *
   * @param newLen 指定的拆开长度
   * @throws IOException 拆块过程中发生任何异常
   */
  public void splitParts(long newLen) throws IOException {
    this.checkOpened();

    Preconditions.checkArgument(newLen >= 0 && newLen <= this.MAX_FILE_SIZE,
        String.format("The newLen should be in range [%d, %d].", 0, this.MAX_FILE_SIZE));

    FileMetadata fileMetadata = nativeStore.retrieveMetadata(cosKey);
    if (null == fileMetadata) {
      throw new IOException(String.format("The cos key [%s] is not found.", cosKey));
    }
    if (!fileMetadata.isFile()) {
      throw new IOException("The cos key [%s] is a directory object. Can not split parts for it.");
    }

    // 拆块需要进行重置
    this.reset();

    long copyRemaining = Math.min(newLen, fileMetadata.getLength());
    if (copyRemaining > 0) {
      long firstByte = 0;
      long lastByte = 0;
      if (copyRemaining >= this.partSize) {
        // 使用服务端copy
        this.initializeMultipartUploadIfNeed();
        try {
          lastByte = firstByte + this.partSize - 1;
          while (copyRemaining >= this.partSize) {
            LOG.debug("Executing the uploadPartCopy [cosKey: {}, uploadId: {}, partNumber: {}].",
                cosKey, this.uploadId, this.localParts.size() + 1);
            UploadPartCopy uploadPartCopy = new UploadPartCopy(cosKey, cosKey,
                this.localParts.size() + 1, firstByte, lastByte);
            this.uploadPartCopy(uploadPartCopy);
            // 补位
            this.localParts.add(null);
            copyRemaining -= ((lastByte - firstByte) + 1);
            firstByte = lastByte + 1;
            lastByte = firstByte + this.partSize - 1;
          }
        } catch (Exception exception) {
          LOG.error("Failed to breakDown the cos key [{}]. Abort it.", cosKey, exception);
          this.abort();
          throw new IOException(exception);
        }
      }
      if (copyRemaining > 0) {
        // 最后一块是拉到本地的
        this.initializeNewLocalCurrentPart();
        CosNRandomAccessMappedBuffer latestBuffer = this.localParts.get(this.localParts.size() - 1);
        lastByte = firstByte + copyRemaining - 1;
        this.fetchBlockFromRemote(latestBuffer, firstByte, lastByte);
        latestBuffer.flipRead();
      }
    }

    long deltaPadding = newLen - Math.min(newLen, fileMetadata.getLength());
    if (deltaPadding > 0) {
      long startPos = Math.min(newLen, fileMetadata.getLength());
      long endPos = newLen - 1;
      this.padBytes(startPos, endPos);
    }

    this.committed = false;
    this.aborted = false;
  }

  public CosNRandomAccessMappedBuffer getPart(int partNumber) throws IOException {
    this.checkOpened();

    if (this.aborted) {
      throw new IOException("The writing operation for the current file " +
          "has been committed or aborted.");
    }

    if (this.committed) {
      // 已经被提交了，则需要重新拆块
      this.resumeForWrite();
    }

    Preconditions.checkArgument(partNumber > 0,
        String.format("The partNumber [%d] should be a positive integer.", partNumber));
    // 先找一下本地是否有
    if (partNumber <= this.localParts.size()) {
      CosNRandomAccessMappedBuffer part = this.localParts.get(partNumber - 1);
      if (null == part) {
        // 本地没有，需要从远端下载
        this.downloadPart(partNumber);
      }
    } else {
      // partNumber 大于当前的 localPart.size，那么需要进行补充块
      // 计算补充范围
      // 取出最后一块出来
      if (this.localParts.size() == 0
          || this.localParts.get(this.localParts.size() - 1) == null) {
        // 初始化一个空块
        this.initializeNewLocalCurrentPart();
      }
      CosNRandomAccessMappedBuffer lastPart = this.localParts.get(this.localParts.size() - 1);
      lastPart.flipWrite();
      long startPos = this.localParts.size() * this.partSize - lastPart.remaining();
      long endPos = (partNumber - 1) * this.partSize - 1;
      // 填充
      if (startPos <= endPos) {
        this.padBytes(startPos, endPos);
      }
      if (this.localParts.size() == partNumber - 1) {
        this.initializeNewLocalCurrentPart();
      }
    }

    return this.localParts.get(partNumber - 1);
  }

  /**
   * 终止整个写入过程，丢弃掉所有修改
   */
  public void abort() {
    this.checkOpened();

    if (this.aborted) {
      LOG.warn("All modifications have been aborted. Skip the aborting operation.");
      return;
    }

    LOG.info("Aborting the MPU [{}]...", this.uploadId);
    // 清理远程块
    this.releaseRemoteParts();
    // 清理本地块
    this.releaseLocalParts();
    this.aborted = true;
  }

  /**
   * 提交所有本地修改到远程
   *
   * @throws IOException
   */
  public void commitLocalToRemote() throws IOException {
    this.checkOpened();

    if (this.committed) {
      LOG.info("All local modifications has been committed. " +
          "Skip to the committing operation.");
      return;
    }
    if (this.aborted) {
      LOG.warn("All local modifications has been aborted. " +
          "Nothing need to be committed.");
      return;
    }

    LOG.info("Committing all local parts to remote... ");
    if (null == this.uploadId && this.localParts.size() == 0) {
      // 传一个空文件上去
      LOG.info("Committing a empty file to remote...");
      this.nativeStore.storeEmptyFile(this.cosKey);
      return;
    }

    if (this.uploadId == null && this.localParts.size() == 1 && this.localParts.get(0) != null) {
      // 采用单文件上传即可
      CosNRandomAccessMappedBuffer lastPart = this.localParts.get(0);
      byte[] md5Hash = null;
      try {
        md5Hash = MD5Utils.calculate(lastPart);
      } catch (NoSuchAlgorithmException | IOException exception) {
        LOG.warn("Failed to calculate the MD5 hash for the single part.", exception);
      }
      this.nativeStore.storeFile(
          this.cosKey, new BufferInputStream(lastPart), md5Hash, lastPart.flipRead().remaining());
    } else {
      // 块数大于 1，使用 MPU 上传
      // 根据需要初始化一下 MPU
      this.initializeMultipartUploadIfNeed();
      // 首先将 localParts 中的块刷上去
      for (int index = 0; index < this.localParts.size(); index++) {
        CosNRandomAccessMappedBuffer part = this.localParts.get(index);
        if (null != part) {
          LOG.debug("Starting to upload the part number [{}] for the MPU [{}].",
              index + 1, this.uploadId);
          byte[] md5Hash = null;
          try {
            md5Hash = MD5Utils.calculate(part);
          } catch (NoSuchAlgorithmException | IOException exception) {
            LOG.warn("Failed to calculate the MD5 hash for the part [{}].",
                index + 1, exception);
          }
          part.flipRead();
          PartETag partETag = this.nativeStore.uploadPart(
              new BufferInputStream(part),
              this.cosKey, this.uploadId,
              index + 1, part.remaining(), md5Hash);

          if (partETag.getPartNumber() > this.partETags.size()) {
            this.partETags.add(partETag);
          } else {
            this.partETags.set(partETag.getPartNumber() - 1, partETag);
          }
          LOG.debug("Upload the part number [{}] successfully.", partETag.getPartNumber());
        }
      }
      // 最后执行 complete 操作
      this.nativeStore.completeMultipartUpload(this.cosKey, this.uploadId, this.partETags);
      LOG.info("Complete the MPU [{}] successfully.", this.uploadId);
    }

    this.committed = true;
  }

  public void close() {
    if (this.closed) {
      return;
    }

    this.releaseRemoteParts();
    this.releaseLocalParts();
    this.aborted = true;
    this.committed = true;
    this.closed = true;
  }

  public long getCurrentSize() {
    this.checkOpened();

    // 获取当前文件大小
    long currentFileSize = 0;
    for (CosNRandomAccessMappedBuffer entry : this.localParts) {
      if (null == entry) {
        currentFileSize += this.partSize;
      } else {
        currentFileSize += entry.flipRead().remaining();
      }
    }

    return currentFileSize;
  }

  public long getPartSize() {
    return this.partSize;
  }

  public long getMaxFileSizeLimit() {
    return this.MAX_FILE_SIZE;
  }

  private void uploadPartCopy(UploadPartCopy uploadPartCopy) throws IOException {
    this.checkOpened();

    Preconditions.checkNotNull(uploadPartCopy, "uploadPartCopy");

    LOG.debug("Start to copy the part: {}.", uploadPartCopy);
    PartETag partETag = nativeStore.uploadPartCopy(this.uploadId,
        uploadPartCopy.getSrcKey(), uploadPartCopy.getDestKey(), uploadPartCopy.getPartNumber(),
        uploadPartCopy.getFirstByte(), uploadPartCopy.getLastByte());
    if (partETags.size() >= uploadPartCopy.getPartNumber()) {
      partETags.set(uploadPartCopy.getPartNumber() - 1, partETag);
    } else {
      partETags.add(partETag);
    }
  }

  private void downloadPart(int partNumber) throws IOException {
    this.checkOpened();

    Preconditions.checkArgument(partNumber > 0 && partNumber <= 10000,
        "The partNumber should be a positive integer and less than or equal to 10000.");
    // 获取一下当前对象的长度
    FileMetadata fileMetadata = nativeStore.retrieveMetadata(cosKey);

    // 计算拉取范围
    long startPos = (long) (partNumber - 1) * this.partSize;
    long endPos = Math.min(partNumber * this.partSize - 1, fileMetadata.getLength());

    if (startPos > endPos) {
      throw new IOException(
          String.format("The partNumber pulled [%d] exceeds file size [%d]. part size: %d.",
              partNumber, fileMetadata.getLength(), this.partSize));
    }

    CosNRandomAccessMappedBuffer randomAccessMappedBuffer =
        this.getLocalPartResource(generateLocalPartName(cosKey, this.uploadId, partNumber),
            (int) this.partSize);
    // 然后从远端下载拉取
    this.fetchBlockFromRemote(randomAccessMappedBuffer, startPos, endPos);
    // 然后放置到 partNumber - 1 的位置即可
    this.localParts.set(partNumber - 1, randomAccessMappedBuffer);
  }

  private void fetchBlockFromRemote(
      CosNRandomAccessMappedBuffer buffer,
      long startPos, long endPos) throws IOException {
    Preconditions.checkArgument(startPos >= 0,
        String.format("The startPos [%d] should be a non-negative integer.", startPos));
    Preconditions.checkArgument(endPos >= 0,
        String.format("The endPos [%d] should be a non-negative integer.", endPos));
    Preconditions.checkArgument(startPos <= endPos,
        String.format("The startPos [%d] should be less than or equals to the endPos [%d].",
            startPos, endPos));
    Preconditions.checkArgument((endPos - startPos + 1) <= buffer.remaining(),
        String.format("The range [%d, %d] exceeds the buffer remaining capacity [%d].",
            startPos, endPos, buffer.remaining()));

    long remaining = endPos - startPos + 1;
    if (remaining > 0) {
      try (InputStream inputStream = nativeStore.retrieveBlock(
          cosKey, startPos, endPos)) {
        byte[] chunk = new byte[(int) Math.min(4 * Unit.KB, remaining)];
        int readBytes = inputStream.read(chunk);
        buffer.flipWrite();
        while (readBytes > 0 && remaining > 0) {
          buffer.put(chunk, 0, readBytes);
          remaining -= readBytes;
          chunk = new byte[(int) Math.min(4 * Unit.KB, remaining)];
          readBytes = inputStream.read(chunk);
        }
      }
    }
    buffer.flipRead();
  }

  /**
   * 在 [startPos, endPos] 这个范围内补充 (byte)0
   *
   * @param startPos 起始补充位置
   * @param endPos   终止补充位置
   * @throws IOException IO异常
   */
  private void padBytes(long startPos, long endPos) throws IOException {
    this.checkOpened();

    Preconditions.checkArgument(startPos >= 0 && endPos >= 0,
        String.format("The startPos [%d] and the endPos [%d] should be a non-negative integer.",
            startPos, endPos));
    Preconditions.checkArgument(startPos <= endPos,
        String.format("The startPos [%d] should be less than the endPos [%d].",
            startPos, endPos));

    // 预计算填充后的大小是否会超过最大文件限制
    CosNRandomAccessMappedBuffer lastPart;
    if (this.localParts.size() == 0
        || this.localParts.get(this.localParts.size() - 1) == null) {
      this.initializeNewLocalCurrentPart();
    }
    lastPart = this.localParts.get(this.localParts.size() - 1);
    lastPart.flipWrite();
    long prePaddingSize = (this.localParts.size() - 1) * partSize + lastPart.remaining()
        + (endPos - startPos + 1);
    Preconditions.checkArgument(prePaddingSize <= this.MAX_FILE_SIZE,
        String.format("The bytes [%d] padded exceeds the maximum file limit [%d]",
            prePaddingSize, this.MAX_FILE_SIZE));

    // 计算出 startPos 所在的 partIndex
    int partStartIndex = (int) (startPos / this.partSize);
    int partStartOffset = (int) (startPos % this.partSize);
    int partEndIndex = (int) (endPos / this.partSize);
    int partEndOffset = (int) (endPos % this.partSize);

    // 如果 localParts 的长度还没有到 partStartIndex，那么先填充到 startIndex 上面去
    for (int index = this.localParts.size(); index <= partStartIndex; index++) {
      // 生辰一个新的块
      this.initializeNewLocalCurrentPart();
      lastPart = this.localParts.get(this.localParts.size() - 1);
      lastPart.flipWrite();
      // 然后填充这个块
      while (lastPart.hasRemaining()) {
        byte[] chunk = new byte[(int) Math.min(4 * Unit.KB,
            lastPart.remaining())];
        Arrays.fill(chunk, (byte) 0);
        lastPart.put(chunk, 0, chunk.length);
      }
      lastPart.flipRead();
    }

    for (int index = partStartIndex; index <= partEndIndex; index++) {
      // 起始块，需要定位到块内偏移
      if (this.localParts.size() <= index) {
        // 初始化一个新块出来
        this.initializeNewLocalCurrentPart();
      }
      CosNRandomAccessMappedBuffer part = this.localParts.get(index);
      part.flipWrite();
      if (index == partStartIndex) {
        // 起始块，需要定位到块内偏移
        part.position(partStartOffset);
      }
      if (index == partEndIndex) {
        // 填充到 partEndOffset 位置
        while (part.position() <= partEndOffset) {
          byte[] chunk = new byte[(int)Math.min(4 * Unit.KB, partEndOffset - part.position() + 1)];
          Arrays.fill(chunk, (byte)0);
          part.put(chunk, 0, chunk.length);
        }
      } else {
        // 直接填充到这个 part 的末尾
        while (part.hasRemaining()) {
          byte[] chunk = new byte[(int) Math.min(4 * Unit.KB,
              part.remaining())];
          Arrays.fill(chunk, (byte) 0);
          part.put(chunk, 0, chunk.length);
        }
      }
      part.flipRead();
    }
  }
  private void releaseLocalParts() {
    this.checkOpened();

    Iterator<CosNRandomAccessMappedBuffer> iterator =
        this.localParts.iterator();
    while (iterator.hasNext()) {
      CosNRandomAccessMappedBuffer part = iterator.next();
      if (null != part) {
        LocalRandomAccessMappedBufferPool.getInstance().releaseFile(part);
      }
      iterator.remove();
    }
    // 清理掉本地保存的 localParts 数据结构
    this.localParts.clear();
  }

  private void releaseRemoteParts() {
    this.checkOpened();

    if (this.committed || this.aborted) {
      LOG.debug("All parts have been committed or aborted. " +
          "Skip to release for remote parts.");
      return;
    }

    try {
      if (null != this.uploadId && !this.uploadId.isEmpty()) {
        LOG.info("Begin to release remote parts for the cos key [{}]. upload id: {}.",
            cosKey, this.uploadId);
        try {
          // abort 掉远程块，就相当于清理掉云端的 parts 了。
          nativeStore.abortMultipartUpload(cosKey, this.uploadId);
        } catch (IOException e) {
          // 如果 abort 发生异常，则原先的 partCopy 块就残留在云端了。不影响当前使用，只需要用户手动去存储桶清理一下即可。
          LOG.warn("Abort the MPU [{}] for the cos key [{}].", this.uploadId, cosKey, e);
        }
      }
    } finally {
      // 清理本地保存的 partETags
      this.partETags.clear();
      this.uploadId = null;
      this.aborted = true;
    }
  }

  /**
   * 根据需要初始化一个 MPU 的 UploadId。
   *
   * @throws IOException
   */
  private void initializeMultipartUploadIfNeed() throws IOException {
    this.checkOpened();

    if (null == this.uploadId) {
      LOG.info("Initialize a multipart upload for the cos key [{}].", cosKey);
      this.uploadId = nativeStore.getUploadId(cosKey);
      this.aborted = false;
      this.committed = false;
    }
  }

  /**
   * 在 localParts 的末尾初始化一个新的本地块用于内容写入。
   *
   * @throws IOException
   */
  private void initializeNewLocalCurrentPart() throws IOException {
    this.checkOpened();

    CosNRandomAccessMappedBuffer lastPart =
        this.getLocalPartResource(generateLocalPartName(cosKey,
            this.uploadId, this.localParts.size() + 1), (int) this.partSize);
    lastPart.clear();
    this.localParts.add(lastPart);
  }

  private void reset() {
    this.checkOpened();

    // 先清理掉远端的parts
    this.releaseRemoteParts();
    // 然后清理掉本地的缓存块
    this.releaseLocalParts();
    this.uploadId = null;
    this.partETags.clear();
    this.aborted = false;
  }

  private void checkOpened() {
    Preconditions.checkState(!this.closed, "The multipart manager has been closed.");
  }

  private CosNRandomAccessMappedBuffer getLocalPartResource(String fileName, int size)
      throws IOException {
    this.checkOpened();

    if (LocalRandomAccessMappedBufferPool.getInstance().shouldRelease()) {
      // 本地的 POSIX extension 语义支持空间已经不够了，需要先尝试释放本地占用
      // 将当前所有修改提交到远端
      this.commitLocalToRemote();
      // 然后清理掉本地的所有空间
      this.releaseLocalParts();
      // 最后重新恢复写
      this.resumeForWrite();
    }

    // 尝试清理空间以后，如果还获取不到，那就只能抛出异常了
    return LocalRandomAccessMappedBufferPool.getInstance().create(fileName, size);
  }

  private static String generateLocalPartName(String cosKey, String uploadId,
                                              int partNumber) {
    // 使用 MD5 摘要来编码 cosKey 作为本地 cache 文件名
    String cacheFileName;
    try {
      cacheFileName = Hex.encodeHexString(MD5Utils.calculate(cosKey));
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Failed to calculate the md5 of the cosKey [{}]. " +
          "Replace it with another form.", cosKey, e);
      cacheFileName = cosKey.replace("/", "_");
    }
    if (null == uploadId) {
      return String.format("%s_null_%d", cacheFileName, partNumber);
    } else {
      return String.format("%s_%s_%s", cosKey, uploadId, partNumber);
    }
  }
}
