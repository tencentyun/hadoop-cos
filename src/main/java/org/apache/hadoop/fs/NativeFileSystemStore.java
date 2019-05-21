package org.apache.hadoop.fs;

import com.qcloud.cos.model.CompleteMultipartUploadResult;
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
interface NativeFileSystemStore {

    void initialize(URI uri, Configuration conf) throws IOException;

    void storeFile(String key, File file, byte[] md5Hash) throws IOException;

    void storeFile(String key, InputStream inputStream, byte[] md5Hash,
                   long contentLength) throws IOException;

    void storeEmptyFile(String key) throws IOException;

    CompleteMultipartUploadResult completeMultipartUpload(String key,
                                                          String uploadId,
                                                          List<PartETag> partETagList);

    void abortMultipartUpload(String key, String uploadId);

    String getUploadId(String key);

    PartETag uploadPart(File file, String key, String uploadId, int partNum) throws IOException;

    PartETag uploadPart(InputStream inputStream, String key, String uploadId,
                        int partNum, long partSize) throws IOException;

    FileMetadata retrieveMetadata(String key) throws IOException;

    InputStream retrieve(String key) throws IOException;

    InputStream retrieve(String key, long byteRangeStart) throws IOException;

    InputStream retrieveBlock(String key, long byteRangeStart,
                              long byteRangeEnd) throws IOException;

    boolean retrieveBlock(String key, long byteRangeStart, long blockSize,
                          String localBlockPath) throws IOException;

    long getFileLength(String key) throws IOException;

    PartialListing list(String prefix, int maxListingLength) throws IOException;

    PartialListing list(String prefix, int maxListingLength,
                        String priorLastKey, boolean recursive)
            throws IOException;

    void delete(String key) throws IOException;

    void copy(String srcKey, String dstKey) throws IOException;

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

    void close();
}
