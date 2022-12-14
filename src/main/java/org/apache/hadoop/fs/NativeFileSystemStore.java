package org.apache.hadoop.fs;

import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.HeadBucketResult;
import com.qcloud.cos.model.PartETag;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface NativeFileSystemStore {

    void initialize(URI uri, Configuration conf) throws IOException;

    void initialize(URI uri, Configuration conf, RangerCredentialsClient rangerClient) throws IOException;

    HeadBucketResult headBucket(String bucketName) throws IOException;

    void storeFile(String key, File file, byte[] md5Hash) throws IOException;

    void storeFile(String key, InputStream inputStream, byte[] md5Hash,
                   long contentLength) throws IOException;

    void storeEmptyFile(String key) throws IOException;

    // must notice some mpu chunk error might have double head check.
    // which means sometimes CompleteMultipartUploadResult might be null.
    CompleteMultipartUploadResult completeMultipartUpload(String key,
                                                          String uploadId,
                                                          List<PartETag> partETagList) throws IOException;

    void abortMultipartUpload(String key, String uploadId) throws IOException;

    String getUploadId(String key) throws IOException;

    PartETag uploadPart(File file, String key, String uploadId, int partNum, byte[] md5hash) throws IOException;

    PartETag uploadPart(InputStream inputStream, String key, String uploadId,
                        int partNum, long partSize, byte[] md5hash) throws IOException;

    PartETag uploadPartCopy(String uploadId, String srcKey, String destKey, int partNum,
                            long firstByte, long lastByte) throws IOException;

    FileMetadata retrieveMetadata(String key) throws IOException;

    FileMetadata retrieveMetadata(String key, CosNResultInfo info) throws IOException;

    CosNSymlinkMetadata retrieveSymlinkMetadata(String symlink) throws IOException;

    CosNSymlinkMetadata retrieveSymlinkMetadata(String symlink, CosNResultInfo info) throws IOException;

    byte[] retrieveAttribute(String key, String attribute) throws IOException;

    void storeDirAttribute(String key, String attribute, byte[] value) throws IOException;

    void storeFileAttribute(String key, String attribute, byte[] value) throws IOException;

    void removeDirAttribute(String key, String attribute) throws IOException;

    void removeFileAttribute(String key, String attribute) throws IOException;

    InputStream retrieve(String key) throws IOException;

    InputStream retrieve(String key, long byteRangeStart) throws IOException;

    InputStream retrieveBlock(String key, long byteRangeStart,
                              long byteRangeEnd) throws IOException;

    boolean retrieveBlock(String key, long byteRangeStart, long blockSize,
                          String localBlockPath) throws IOException;

    long getFileLength(String key) throws IOException;

    CosNPartialListing list(String prefix, int maxListingLength) throws IOException;

    CosNPartialListing list(String prefix, int maxListingLength, CosNResultInfo info) throws IOException;

    CosNPartialListing list(String prefix, int maxListingLength,
                            String priorLastKey, boolean recursive)
            throws IOException;

    CosNPartialListing list(String prefix, int maxListingLength,
                            String priorLastKey, boolean recursive, CosNResultInfo info)
            throws IOException;

    void delete(String key) throws IOException;

    void deleteRecursive(String key) throws IOException;

    void copy(String srcKey, String dstKey) throws IOException;

    void rename(String srcKey, String dstKey) throws IOException;

    void createSymlink(String symLink, String targetKey) throws IOException;

    String getSymlink(String symlink) throws IOException;

    /**
     * Delete all keys with the given prefix. Used for testing.
     *
     * @throws IOException
     */
    void purge(String prefix) throws IOException;

    /**
     * Diagnostic method to dump state to the console.
     *
     * @throws IOException
     */
    void dump() throws IOException;

    /**
     * Used for outer to decide inner process.
     * if you use the CosNFileSystem gateway mode,
     * must set native store to posix process
     */
    void isPosixBucket(boolean isPosixBucket);

    RangerCredentialsClient getRangerCredentialsClient();

    void close();
}
