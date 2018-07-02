/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.cosnative;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.utils.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.fs.cosnative.CosFileSystem.PATH_DELIMITER;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CosNativeFileSystemStore implements NativeFileSystemStore {

    public static final String USER_AGENT = "cos-hadoop-plugin-v5.2";

    private COSClient cosClient;
    private TransferManager transferManager;
    private String bucketName;
    final private int MAX_RETRY_TIME = 5;

    public static final Logger LOG = LoggerFactory.getLogger(CosNativeFileSystemStore.class);

    private void initCOSClient(Configuration conf) throws IOException {
        String appidStr = conf.get(CosNativeFileSystemConfigKeys.COS_APPID_KEY);
        String secretId = conf.get(CosNativeFileSystemConfigKeys.COS_SECRET_ID_KEY);
        if (secretId == null) {
            throw new IOException("config fs.cosn.userinfo.secretId miss!");
        }
        String secretKey = conf.get(CosNativeFileSystemConfigKeys.COS_SECRET_KEY_KEY);
        if (secretKey == null) {
            throw new IOException("config fs.cosn.userinfo.secretKey miss!");
        }
        String region = conf.get(CosNativeFileSystemConfigKeys.COS_REGION_KEY);
        String endpoint_suffix = conf.get(CosNativeFileSystemConfigKeys.COS_ENDPOINT_SUFFIX_KEY);
        if (null == region && null == endpoint_suffix) {
            throw new IOException("config fs.cosn.userinfo.region and fs.cosn.userinfo.endpoint_suffix specify at least one");
        }

        COSCredentials cosCred = null;
        if (appidStr == null) {
            cosCred = new BasicCOSCredentials(secretId, secretKey);
        } else {
            cosCred = new BasicCOSCredentials(appidStr, secretId, secretKey);
        }

        boolean useHttps = conf.getBoolean(CosNativeFileSystemConfigKeys.COS_USE_HTTPS_KEY, CosNativeFileSystemConfigKeys.DEFAULT_USE_HTTPS);

        ClientConfig config = null;
        if (null == region) {
            config = new ClientConfig(new Region(""));
        } else {
            config = new ClientConfig(new Region(region));
        }
        if (useHttps) {
            config.setHttpProtocol(HttpProtocol.https);
        }
        config.setEndPointSuffix(endpoint_suffix);

        config.setUserAgent(USER_AGENT);
        this.cosClient = new COSClient(cosCred, config);
    }

    private void initTransferManager(Configuration conf) throws IOException {
        int threadCount = 0;
        try {
            threadCount = conf.getInt(CosNativeFileSystemConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY, CosNativeFileSystemConfigKeys.DEFAULT_THREAD_POOL_SIZE);
        } catch (NumberFormatException e) {
            throw new IOException("fs.cosn.userinfo.upload_thread_pool value is invalid number");
        }
        if (threadCount <= 0) {
            throw new IOException("fs.cosn.userinfo.upload_thread_pool value must greater than 0");
        }
        this.transferManager =
                new TransferManager(this.cosClient, Executors.newFixedThreadPool(threadCount));
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        try {
            initCOSClient(conf);
            initTransferManager(conf);
            this.bucketName = uri.getHost();
        } catch (Exception e) {
            handleException(e, "");
        }
    }

    private void storeFileWithRetry(String key, InputStream inputStream, byte[] md5Hash, long length) throws IOException {
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentMD5(Base64.encodeAsString(md5Hash));
            LOG.info("md5: "+ utils.bytesToHexString(md5Hash) + " content_length: " + length);
            objectMetadata.setContentLength(length);
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, inputStream, objectMetadata);

            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            String debugMsg = String.format("store empty file success, cos key: %s, etag: %s", key,
                    putObjectResult.getETag());
            LOG.debug(debugMsg);
        } catch (Exception e) {
            String errMsg =
                    String.format("store empty file faild, cos key: %s, exception: %s",
                            key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        }
    }

    @Override
    public void storeFile(String key, File file, byte[] md5Hash) throws IOException {
        LOG.info("store file:, localPath:" + file.getCanonicalPath() + ", len:" + file.length());
        storeFileWithRetry(key, new BufferedInputStream(new FileInputStream(file)), md5Hash, file.length());
    }

    @Override
    public void storeFile(String key, InputStream inputStream, byte[] md5Hash, long contentLength) throws IOException {
        storeFileWithRetry(key, inputStream, md5Hash, contentLength);
    }

    // for cos, storeEmptyFile means create a directory
    @Override
    public void storeEmptyFile(String key) throws IOException {
        if (!key.endsWith(PATH_DELIMITER)) {
            key = key + PATH_DELIMITER;
        }

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(0);
        InputStream input = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, input, objectMetadata);
        try {
            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            String debugMsg = String.format("store empty file success, cos key: %s, etag: %s", key,
                    putObjectResult.getETag());
            LOG.debug(debugMsg);
        } catch (Exception e) {
            String errMsg =
                    String.format("store empty file faild, cos key: %s, exception: %s",
                            key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        }
    }

    public PartETag uploadPart(File file, String key, String uploadId, int partNum) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        return uploadPart(inputStream, key, uploadId, partNum, file.length());
    }

    @Override
    public PartETag uploadPart(InputStream inputStream, String key, String uploadId, int partNum, long partSize) throws IOException {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(this.bucketName);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(inputStream);
        uploadPartRequest.setPartNumber(partNum);
        uploadPartRequest.setPartSize(partSize);
        uploadPartRequest.setKey(key);

        try {
            UploadPartResult uploadPartResult = (UploadPartResult) callCOSClientWithRetry(uploadPartRequest);
            return uploadPartResult.getPartETag();
        } catch (Exception e) {
            String errMsg = String.format("current thread:%d, cos key: %s, upload id: %s, part num: %d, exception: %s", Thread.currentThread().getId(), key, uploadId, partNum, e.toString());
            handleException(new Exception(errMsg), key);
        }

        return null;
    }

    public void abortMultipartUpload(String key, String uploadId) {
        AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(
                bucketName, key, uploadId
        );
        cosClient.abortMultipartUpload(abortMultipartUploadRequest);
    }

    public String getUploadId(String key) {
        if (null == key || key.length() == 0) {
            return "";
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucketName, key);
        InitiateMultipartUploadResult initiateMultipartUploadResult = cosClient.initiateMultipartUpload(initiateMultipartUploadRequest);           // This code does not need to support retry
        return initiateMultipartUploadResult.getUploadId();
    }

    public CompleteMultipartUploadResult completeMultipartUpload(String key, String uploadId, List<PartETag> partETagList) {
        Collections.sort(partETagList, new Comparator<PartETag>() {
            @Override
            public int compare(PartETag o1, PartETag o2) {
                return o1.getPartNumber() - o2.getPartNumber();
            }
        });
        CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETagList);
        return cosClient.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private FileMetadata QueryObjectMetadata(String key) throws IOException {
        GetObjectMetadataRequest getObjectMetadataRequest =
                new GetObjectMetadataRequest(bucketName, key);
        try {
            ObjectMetadata objectMetadata =
                    (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
            long mtime = 0;
            long fileSize = 0;
            if (objectMetadata.getLastModified() != null) {
                mtime = objectMetadata.getLastModified().getTime();
            }
            fileSize = objectMetadata.getContentLength();
            FileMetadata fileMetadata =
                    new FileMetadata(key, fileSize, mtime, !key.endsWith(PATH_DELIMITER));
            LOG.debug("retive file MetaData key:{}, etag:{}, length:{}", key,
                    objectMetadata.getETag(), objectMetadata.getContentLength());
            return fileMetadata;
        } catch (CosServiceException e) {
            if (e.getStatusCode() != 404) {
                String errorMsg =
                        String.format("retrieve file Metadata file failure, key: %s, exception: %s",
                                key, e.toString());
                LOG.error(errorMsg);
                handleException(new Exception(errorMsg), key);
            }
        }
        return null;
    }

    @Override
    public FileMetadata retrieveMetadata(String key) throws IOException {
        if (key.endsWith(PATH_DELIMITER)) {
            key = key.substring(0, key.length() - 1);
        }

        if (!key.isEmpty()) {
            FileMetadata fileMetadata = QueryObjectMetadata(key);
            if (fileMetadata != null) {
                return fileMetadata;
            }
        }
        // judge if the key is directory
        key = key + PATH_DELIMITER;
        return QueryObjectMetadata(key);
    }

    /**
     * @param key The key is the object name that is being retrieved from the cos bucket
     * @return This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key) throws IOException {
        LOG.debug("retrive key:{}", key);
        GetObjectRequest getObjectRequest = new GetObjectRequest(this.bucketName, key);
        try {
            COSObject cosObject = (COSObject) callCOSClientWithRetry(getObjectRequest);
            return cosObject.getObjectContent();
        } catch (Exception e) {
            String errMsg = String.format("retrive key %s occur a exception %s", key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    /**
     * @param key The key is the object name that is being retrieved from the cos bucket
     * @return This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key, long byteRangeStart) throws IOException {
        try {
            LOG.debug("retrive key:{}, byteRangeStart:{}", key, byteRangeStart);
            long fileSize = getFileLength(key);
            long byteRangeEnd = fileSize - 1;
            GetObjectRequest getObjectRequest = new GetObjectRequest(this.bucketName, key);
            if (byteRangeEnd >= byteRangeStart) {
                getObjectRequest.setRange(byteRangeStart, fileSize - 1);
            }
            COSObject cosObject = (COSObject) callCOSClientWithRetry(getObjectRequest);
            return cosObject.getObjectContent();
        } catch (Exception e) {
            String errMsg =
                    String.format("retrive key %s with byteRangeStart %d occur a exception: %s",
                            key, byteRangeStart, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    @Override
    public InputStream retrieveBlock(String key, long byteRangeStart, long byteRangeEnd) throws IOException {
        try {
            GetObjectRequest request = new GetObjectRequest(this.bucketName, key);
            request.setRange(byteRangeStart, byteRangeEnd);
            COSObject cosObject = (COSObject) this.callCOSClientWithRetry(request);
            return cosObject.getObjectContent();
        } catch (CosServiceException e) {
            String errMsg =
                    String.format("retrive key %s with byteRangeStart %d occur a exception: %s",
                            key, byteRangeStart, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null;
        } catch (CosClientException e) {
            String errMsg =
                    String.format("retrive key %s with byteRangeStart %d occur a exception: %s",
                            key, byteRangeStart, e.toString());
            LOG.error("retrieve key: " + key + "'s block, start: " + String.valueOf(byteRangeStart) + " end: " + String.valueOf(byteRangeEnd), e);
            handleException(new Exception(errMsg), key);
            return null;
        }
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength) throws IOException {
        return list(prefix, maxListingLength, null, false);
    }

    @Override
    public PartialListing list(String prefix, int maxListingLength, String priorLastKey,
                               boolean recurse) throws IOException {

        return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength, priorLastKey);
    }

    /**
     * list objects
     *
     * @param prefix           prefix
     * @param delimiter        delimiter
     * @param maxListingLength max no. of entries
     * @param priorLastKey     last key in any previous search
     * @return a list of matches
     * @throws IOException on any reported failure
     */

    private PartialListing list(String prefix, String delimiter, int maxListingLength,
                                String priorLastKey) throws IOException {
        LOG.debug("list prefix:" + prefix);
        if (!prefix.startsWith(PATH_DELIMITER)) {
            prefix += PATH_DELIMITER;
        }
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);
        listObjectsRequest.setPrefix(prefix);
        listObjectsRequest.setDelimiter(delimiter);
        listObjectsRequest.setMarker(priorLastKey);
        listObjectsRequest.setMaxKeys(maxListingLength);
        ObjectListing objectListing = null;
        try {
            objectListing = (ObjectListing) callCOSClientWithRetry(listObjectsRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "prefix: %s, delimiter: %s, maxListingLength: %d, priorLastKey: %s. list occur a exception: %s",
                    prefix, (delimiter == null) ? "" : delimiter, maxListingLength, priorLastKey,
                    e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), prefix);
        }
        ArrayList<FileMetadata> fileMetadataArry = new ArrayList<>();
        ArrayList<FileMetadata> commonPrefixsArry = new ArrayList<>();

        List<COSObjectSummary> summaries = objectListing.getObjectSummaries();
        for (COSObjectSummary cosObjectSummary : summaries) {
            String filePath = cosObjectSummary.getKey();
            if (!filePath.startsWith(PATH_DELIMITER)) {
                filePath = PATH_DELIMITER + filePath;
            }
            if (filePath.equals(prefix)) {
                continue;
            }
            long mtime = 0;
            if (cosObjectSummary.getLastModified() != null) {
                mtime = cosObjectSummary.getLastModified().getTime();
            }
            long fileLen = cosObjectSummary.getSize();
            LOG.debug("add filepath: " + filePath);
            fileMetadataArry.add(new FileMetadata(filePath, fileLen, mtime, true));
        }
        List<String> commonPrefixes = objectListing.getCommonPrefixes();
        for (String commonPrefix : commonPrefixes) {
            if (!commonPrefix.startsWith(PATH_DELIMITER)) {
                commonPrefix = PATH_DELIMITER + commonPrefix;
            }
            LOG.debug("add commonprefix: " + commonPrefix);
            commonPrefixsArry.add(new FileMetadata(commonPrefix, 0, 0, false));
        }

        FileMetadata[] fileMetadata = new FileMetadata[fileMetadataArry.size()];
        for (int i = 0; i < fileMetadataArry.size(); ++i) {
            fileMetadata[i] = fileMetadataArry.get(i);
        }
        FileMetadata[] commonPrefixMetaData = new FileMetadata[commonPrefixsArry.size()];
        for (int i = 0; i < commonPrefixsArry.size(); ++i) {
            commonPrefixMetaData[i] = commonPrefixsArry.get(i);
        }
        LOG.debug("fileMetadata size: " + fileMetadata.length);
        LOG.debug("commonPrefixMetaData size: " + commonPrefixMetaData.length);
        // 如果truncated为false, 则表明已经遍历完
        if (!objectListing.isTruncated()) {
            return new PartialListing(null, fileMetadata, commonPrefixMetaData);
        } else {
            return new PartialListing(objectListing.getNextMarker(), fileMetadata, commonPrefixMetaData);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        LOG.debug("Deleting key: {} from bucket: {}", key, this.bucketName);
        try {
            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format("delete key %s occur a exception: %s", key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        }
    }

    public void rename(String srcKey, String dstKey) throws IOException {
        LOG.debug("store rename srcKey:{}, dstKey:{}", srcKey, dstKey);
        try {
            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName, dstKey);
            callCOSClientWithRetry(copyObjectRequest);
            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, srcKey);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "rename object faild, src cos key: %s, dst cos key: %s, exception: %s", srcKey,
                    dstKey, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), srcKey);
        }
    }

    @Override
    public void copy(String srcKey, String dstKey) throws IOException {
        LOG.debug("copy srcKey:{}, dstKey:{}", srcKey, dstKey);
        try {
            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName, dstKey);
            callCOSClientWithRetry(copyObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "rename object faild, src cos key: %s, dst cos key: %s, exception: %s", srcKey,
                    dstKey, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), srcKey);
        }
    }

    @Override
    public void purge(String prefix) throws IOException {
        throw new IOException("purge Not supported");
    }

    @Override
    public void dump() throws IOException {
        throw new IOException("dump Not supported");
    }

    // process Exception and print detail
    private void handleException(Exception e, String key) throws IOException {
        String cosPath = "cosn://" + bucketName + key;
        String exceptInfo = String.format("%s : %s", cosPath, e.toString());
        throw new IOException(exceptInfo);
    }

    @Override
    public boolean retrieveBlock(String key, long byteRangeStart, long blockSize,
                                 String localBlockPath) throws IOException {
        long fileSize = getFileLength(key);
        long byteRangeEnd = 0;
        try {
            GetObjectRequest request = new GetObjectRequest(this.bucketName, key);
            if (fileSize > 0) {
                byteRangeEnd = Math.min(fileSize - 1, byteRangeStart + blockSize - 1);
                request.setRange(byteRangeStart, byteRangeEnd);
            }
            cosClient.getObject(request, new File(localBlockPath));
            return true;
        } catch (Exception e) {
            String errMsg =
                    String.format("retrieveBlock key %s with range [%d - %d] occur a exception: %s",
                            key, byteRangeStart, byteRangeEnd, e.getMessage());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return false; // never will get here
        }
    }

    @Override
    public long getFileLength(String key) throws IOException {
        LOG.debug("getFile Length, cos key: {}", key);
        GetObjectMetadataRequest getObjectMetadataRequest =
                new GetObjectMetadataRequest(bucketName, key);
        try {
            ObjectMetadata objectMetadata =
                    (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
            return objectMetadata.getContentLength();
        } catch (Exception e) {
            String errMsg = String.format("getFileLength occur a exception, key %s, exception: %s",
                    key, e.getMessage());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return 0; // never will get here
        }
    }

    private <X> Object callCOSClientWithRetry(X request) throws CosServiceException, IOException {
        String sdkMethod = "";
        int retryIndex = 1;
        while (true) {
            try {
                if (request instanceof PutObjectRequest) {
                    sdkMethod = "putObject";
                    return this.cosClient.putObject((PutObjectRequest) request);
                } else if (request instanceof UploadPartRequest) {
                    sdkMethod = "uploadPart";
                    return this.cosClient.uploadPart((UploadPartRequest) request);
                } else if (request instanceof GetObjectMetadataRequest) {
                    sdkMethod = "queryObjectMeta";
                    return this.cosClient.getObjectMetadata((GetObjectMetadataRequest) request);
                } else if (request instanceof DeleteObjectRequest) {
                    sdkMethod = "deleteObject";
                    this.cosClient.deleteObject((DeleteObjectRequest) request);
                    return new Object();
                } else if (request instanceof CopyObjectRequest) {
                    sdkMethod = "copyFile";
                    CopyObjectResult copyObjectResult =
                            this.cosClient.copyObject((CopyObjectRequest) request);
                    return copyObjectResult;
                } else if (request instanceof GetObjectRequest) {
                    sdkMethod = "getObject";
                    COSObject cosObject = this.cosClient.getObject((GetObjectRequest) request);
                    return cosObject;
                } else if (request instanceof ListObjectsRequest) {
                    sdkMethod = "listObjects";
                    ObjectListing objectListing =
                            this.cosClient.listObjects((ListObjectsRequest) request);
                    return objectListing;
                } else {
                    throw new IOException("no such method");
                }
            } catch (CosServiceException cse) {
                String errMsg = String.format(
                        "------------------------call cos sdk failed, retryIndex: [%d / %d], call method: %s, exception: %s",
                        retryIndex, MAX_RETRY_TIME, sdkMethod, cse.toString());
                int stattusCode = cse.getStatusCode();
                // 对5xx错误进行重试
                if (stattusCode / 100 == 5) {
                    if (retryIndex <= MAX_RETRY_TIME) {
                        LOG.info(errMsg);
                        long sleepLeast = retryIndex * 300L;
                        long sleepBound = retryIndex * 500L;
                        try {
                            Thread.sleep(
                                    ThreadLocalRandom.current().nextLong(sleepLeast, sleepBound));
                            ++retryIndex;
                        } catch (InterruptedException e) {
                            throw new IOException(e.toString());
                        }
                    } else {
                        LOG.error(errMsg);
                        throw new IOException(errMsg);
                    }
                } else {
                    throw cse;
                }
            } catch (Exception e) {
                String errMsg = String.format("-----------------------call cos sdk failed, call method: %s, exception: %s",
                        sdkMethod, e.toString());
                LOG.error(errMsg);
                throw new IOException(errMsg);
            }
        }
    }
}
