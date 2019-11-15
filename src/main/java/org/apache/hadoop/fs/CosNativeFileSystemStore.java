package org.apache.hadoop.fs;

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
import org.apache.hadoop.fs.auth.COSCredentialProviderList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.fs.CosFileSystem.PATH_DELIMITER;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class CosNativeFileSystemStore implements NativeFileSystemStore {
    private COSClient cosClient;
    private TransferManager transferManager;
    private String bucketName;
    private int maxRetryTimes;
    private CosEncryptionSecrets encryptionSecrets;

    public static final Logger LOG =
            LoggerFactory.getLogger(CosNativeFileSystemStore.class);

    private void initCOSClient(URI uri, Configuration conf) throws IOException {
        String appidStr = conf.get(CosNConfigKeys.COSN_APPID_KEY);
        COSCredentialProviderList credentialProviderList =
                CosNUtils.createCosCredentialsProviderSet(uri, conf);
        String region = conf.get(CosNConfigKeys.COSN_REGION_KEY);
        if (null == region) {
            region = conf.get(CosNConfigKeys.COSN_REGION_PREV_KEY);
        }
        String endpoint_suffix =
                conf.get(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
        if (null == region) {
            endpoint_suffix =
                    conf.get(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_PREV_KEY);
        }
        if (null == region && null == endpoint_suffix) {
            String exceptionMsg = String.format("config %s and %s specify at " +
                            "least one.",
                    CosNConfigKeys.COSN_REGION_KEY,
                    CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
            throw new IOException(exceptionMsg);
        }

        COSCredentials cosCred = null;
        if (appidStr == null) {
            cosCred = new BasicCOSCredentials(
                    credentialProviderList.getCredentials().getCOSAccessKeyId(),
                    credentialProviderList.getCredentials().getCOSSecretKey());
        } else {
            cosCred = new BasicCOSCredentials(
                    appidStr,
                    credentialProviderList.getCredentials().getCOSAccessKeyId(),
                    credentialProviderList.getCredentials().getCOSSecretKey());
        }

        boolean useHttps = conf.getBoolean(
                CosNConfigKeys.COSN_USE_HTTPS_KEY,
                CosNConfigKeys.DEFAULT_USE_HTTPS);

        ClientConfig config = null;
        if (null == region) {
            config = new ClientConfig(new Region(""));
            config.setEndPointSuffix(endpoint_suffix);
        } else {
            config = new ClientConfig(new Region(region));
        }
        if (useHttps) {
            config.setHttpProtocol(HttpProtocol.https);
        }

        config.setUserAgent(conf.get(
                CosNConfigKeys.USER_AGENT,
                CosNConfigKeys.DEFAULT_USER_AGENT));

        this.maxRetryTimes = conf.getInt(
                CosNConfigKeys.COSN_MAX_RETRIES_KEY,
                CosNConfigKeys.DEFAULT_MAX_RETRIES);

        // 设置连接池的最大连接数目
        config.setMaxConnectionsCount(
                conf.getInt(
                        CosNConfigKeys.MAX_CONNECTION_NUM,
                        CosNConfigKeys.DEFAULT_MAX_CONNECTION_NUM
                )
        );

        // 设置是否进行服务器端加密
        String ServerSideEncryptionAlgorithm = conf.get(CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_ALGORITHM, "");
        CosEncryptionMethods CosSSE = CosEncryptionMethods.getMethod(
                ServerSideEncryptionAlgorithm);
        String SSEKey = conf.get(
                CosNConfigKeys.COSN_SERVER_SIDE_ENCRYPTION_KEY, "");
        CheckEncryptionMethod(config, CosSSE, SSEKey);

        this.encryptionSecrets = new CosEncryptionSecrets(CosSSE, SSEKey);
        this.cosClient = new COSClient(cosCred, config);

    }

    private void initTransferManager(Configuration conf) throws IOException {
        int threadCount = 0;
        try {
            threadCount = conf.getInt(
                    CosNConfigKeys.UPLOAD_THREAD_POOL_SIZE_KEY,
                    CosNConfigKeys.DEFAULT_UPLOAD_THREAD_POOL_SIZE);
        } catch (NumberFormatException e) {
            throw new IOException("fs.cosn.userinfo.upload_thread_pool value " +
                    "is invalid number");
        }
        if (threadCount <= 0) {
            throw new IOException("fs.cosn.userinfo.upload_thread_pool value " +
                    "must greater than 0");
        }
        this.transferManager =
                new TransferManager(this.cosClient,
                        Executors.newFixedThreadPool(threadCount));
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        try {
            initCOSClient(uri, conf);
            initTransferManager(conf);
            this.bucketName = uri.getHost();
        } catch (Exception e) {
            handleException(e, "");
        }
    }

    private void storeFileWithRetry(String key, InputStream inputStream,
                                    byte[] md5Hash, long length)
            throws IOException {
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.setContentMD5(Base64.encodeAsString(md5Hash));
            objectMetadata.setContentLength(length);
            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucketName, key, inputStream,
                            objectMetadata);
            this.setEncryptionMetadata(putObjectRequest, objectMetadata);

            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            String debugMsg = String.format("store file success, cos key: %s," +
                            " etag: %s", key,
                    putObjectResult.getETag());
            LOG.debug(debugMsg);
        } catch (CosServiceException cse) {
            // 避免并发上传的问题
            int statusCode = cse.getStatusCode();
            if (statusCode == 409) {
                // Check一下这个文件是否已经存在
                FileMetadata fileMetadata = this.QueryObjectMetadata(key);
                if (null == fileMetadata) {
                    // 如果文件不存在，则需要抛出异常
                    throw cse;
                }
                LOG.warn("Upload file: " + key + " concurrently.");
                return;
            } else {
                // 其他错误都要抛出来
                throw cse;
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("store file failed, cos key: %s, exception:" +
                                    " %s",
                            key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        }
    }

    @Override
    public void storeFile(String key, File file, byte[] md5Hash) throws IOException {
        LOG.info("store file:, localPath:" + file.getCanonicalPath() + ", " +
                "len:" + file.length());
        storeFileWithRetry(key,
                new BufferedInputStream(new FileInputStream(file)), md5Hash,
                file.length());
    }

    @Override
    public void storeFile(String key, InputStream inputStream, byte[] md5Hash
            , long contentLength) throws IOException {
        LOG.info("store file input stream md5 hash content length");
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
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                key, input, objectMetadata);
        try {
            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            String debugMsg = String.format("store empty file success, cos " +
                            "key: %s, etag: %s", key,
                    putObjectResult.getETag());
            LOG.debug(debugMsg);
        } catch (CosServiceException cse) {
            int statusCode = cse.getStatusCode();
            if (statusCode == 409) {
                // 并发上传文件导致，再check一遍文件是否存在
                FileMetadata fileMetadata = this.QueryObjectMetadata(key);
                if (null == fileMetadata) {
                    // 文件还是不存在，必须要抛出异常
                    throw cse;
                }
                LOG.warn("Upload file: " + key + " concurrently.");
                return;
            } else {
                // 其他错误必须抛出
                throw cse;
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("store empty file failed, cos key: %s, " +
                                    "exception: %s",
                            key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        }
    }

    public PartETag uploadPart(File file, String key, String uploadId,
                               int partNum) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        return uploadPart(inputStream, key, uploadId, partNum, file.length());
    }


    @Override
    public PartETag uploadPart(
            InputStream inputStream,
            String key, String uploadId, int partNum, long partSize) throws IOException {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(this.bucketName);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(inputStream);
        uploadPartRequest.setPartNumber(partNum);
        uploadPartRequest.setPartSize(partSize);
        uploadPartRequest.setKey(key);
        this.setEncryptionMetadata(uploadPartRequest, new ObjectMetadata());

        try {
            UploadPartResult uploadPartResult =
                    (UploadPartResult) callCOSClientWithRetry(uploadPartRequest);
            return uploadPartResult.getPartETag();
        } catch (Exception e) {
            String errMsg = String.format("current thread:%d, "
                            + "cos key: %s, upload id: %s, part num: %d, " +
                            "exception: %s",
                    Thread.currentThread().getId(), key, uploadId, partNum,
                    e.toString());
            handleException(new Exception(errMsg), key);
        }

        return null;
    }

    public void abortMultipartUpload(String key, String uploadId) {
        AbortMultipartUploadRequest abortMultipartUploadRequest =
                new AbortMultipartUploadRequest(
                        bucketName, key, uploadId
                );
        cosClient.abortMultipartUpload(abortMultipartUploadRequest);
    }

    public String getUploadId(String key) {
        if (null == key || key.length() == 0) {
            return "";
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        try {
            this.setEncryptionMetadata(initiateMultipartUploadRequest, new ObjectMetadata());
        } catch (Exception e) {
            String errMsg =
                    String.format("getUploadId failed, cos key: %s, " +
                                    "exception: %s",
                            key, e.toString());
            LOG.error(errMsg);
        }

        InitiateMultipartUploadResult initiateMultipartUploadResult =
                cosClient.initiateMultipartUpload(initiateMultipartUploadRequest);           // This code does not
        // need to support retry
        return initiateMultipartUploadResult.getUploadId();
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            String key, String uploadId, List<PartETag> partETagList) {
        Collections.sort(partETagList, new Comparator<PartETag>() {
            @Override
            public int compare(PartETag o1, PartETag o2) {
                return o1.getPartNumber() - o2.getPartNumber();
            }
        });
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId,
                        partETagList);
        return cosClient.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private FileMetadata QueryObjectMetadata(String key) throws IOException {
        GetObjectMetadataRequest getObjectMetadataRequest =
                new GetObjectMetadataRequest(bucketName, key);

        this.setEncryptionMetadata(getObjectMetadataRequest, new ObjectMetadata());
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
                    new FileMetadata(key, fileSize, mtime,
                            !key.endsWith(PATH_DELIMITER));
            LOG.debug("retrieve file MetaData key:{}, etag:{}, length:{}", key,
                    objectMetadata.getETag(),
                    objectMetadata.getContentLength());
            return fileMetadata;
        } catch (CosServiceException e) {
            if (e.getStatusCode() != 404) {
                String errorMsg =
                        String.format("retrieve file Metadata file failure, " +
                                        "key: %s, exception: %s",
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
     * @param key The key is the object name that is being retrieved from the
     *            cos bucket
     * @return This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key) throws IOException {
        LOG.debug("retrive key:{}", key);
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(this.bucketName, key);
        this.setEncryptionMetadata(getObjectRequest, new ObjectMetadata());


        try {
            COSObject cosObject =
                    (COSObject) callCOSClientWithRetry(getObjectRequest);
            return cosObject.getObjectContent();
        } catch (Exception e) {
            String errMsg = String.format("retrive key %s occur a exception " +
                    "%s", key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    /**
     * @param key The key is the object name that is being retrieved from the
     *            cos bucket
     * @return This method returns null if the key is not found
     * @throws IOException
     */

    @Override
    public InputStream retrieve(String key, long byteRangeStart) throws IOException {
        try {
            LOG.debug("retrieve key:{}, byteRangeStart:{}", key,
                    byteRangeStart);
            long fileSize = getFileLength(key);
            long byteRangeEnd = fileSize - 1;
            GetObjectRequest getObjectRequest =
                    new GetObjectRequest(this.bucketName, key);

            this.setEncryptionMetadata(getObjectRequest, new ObjectMetadata());

            if (byteRangeEnd >= byteRangeStart) {
                getObjectRequest.setRange(byteRangeStart, fileSize - 1);
            }
            COSObject cosObject =
                    (COSObject) callCOSClientWithRetry(getObjectRequest);
            return cosObject.getObjectContent();
        } catch (Exception e) {
            String errMsg =
                    String.format("retrieve key %s with byteRangeStart %d " +
                                    "occur a exception: %s",
                            key, byteRangeStart, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    @Override
    public InputStream retrieveBlock(String key, long byteRangeStart,
                                     long byteRangeEnd) throws IOException {
        try {
            GetObjectRequest request = new GetObjectRequest(this.bucketName,
                    key);
            request.setRange(byteRangeStart, byteRangeEnd);
            this.setEncryptionMetadata(request, new ObjectMetadata());
            COSObject cosObject =
                    (COSObject) this.callCOSClientWithRetry(request);
            return cosObject.getObjectContent();
        } catch (CosServiceException e) {
            String errMsg =
                    String.format("retrieve key %s with byteRangeStart %d " +
                                    "occur a exception: %s",
                            key, byteRangeStart, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return null;
        } catch (CosClientException e) {
            String errMsg =
                    String.format("retrieve key %s with byteRangeStart %d " +
                                    "occur a exception: %s",
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
    public PartialListing list(String prefix, int maxListingLength,
                               String priorLastKey,
                               boolean recurse) throws IOException {

        return list(prefix, recurse ? null : PATH_DELIMITER, maxListingLength
                , priorLastKey);
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

    private PartialListing list(String prefix, String delimiter,
                                int maxListingLength,
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
            objectListing =
                    (ObjectListing) callCOSClientWithRetry(listObjectsRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "prefix: %s, delimiter: %s, maxListingLength: %d, " +
                            "priorLastKey: %s. list occur a exception: %s",
                    prefix, (delimiter == null) ? "" : delimiter,
                    maxListingLength, priorLastKey,
                    e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), prefix);
        }
        ArrayList<FileMetadata> fileMetadataArry =
                new ArrayList<FileMetadata>();
        ArrayList<FileMetadata> commonPrefixsArry =
                new ArrayList<FileMetadata>();

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
            fileMetadataArry.add(new FileMetadata(filePath, fileLen, mtime,
                    true));
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
        FileMetadata[] commonPrefixMetaData =
                new FileMetadata[commonPrefixsArry.size()];
        for (int i = 0; i < commonPrefixsArry.size(); ++i) {
            commonPrefixMetaData[i] = commonPrefixsArry.get(i);
        }
        LOG.debug("fileMetadata size: " + fileMetadata.length);
        LOG.debug("commonPrefixMetaData size: " + commonPrefixMetaData.length);
        // 如果truncated为false, 则表明已经遍历完
        if (!objectListing.isTruncated()) {
            return new PartialListing(null, fileMetadata, commonPrefixMetaData);
        } else {
            return new PartialListing(objectListing.getNextMarker(),
                    fileMetadata, commonPrefixMetaData);
        }
    }

    @Override
    public void delete(String key) throws IOException {
        LOG.debug("Deleting key: {} from bucket: {}", key, this.bucketName);
        try {
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, key);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format("delete key %s occur a exception: " +
                    "%s", key, e.toString());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
        }
    }

    public void rename(String srcKey, String dstKey) throws IOException {
        LOG.debug("store rename srcKey:{}, dstKey:{}", srcKey, dstKey);
        try {
            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName,
                            dstKey);
            this.setEncryptionMetadata(copyObjectRequest, new ObjectMetadata());
            callCOSClientWithRetry(copyObjectRequest);
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, srcKey);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "rename object failed, src cos key: %s, dst cos key: %s, " +
                            "exception: %s", srcKey,
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
                    new CopyObjectRequest(bucketName, srcKey, bucketName,
                            dstKey);
            this.setEncryptionMetadata(copyObjectRequest, new ObjectMetadata());
            callCOSClientWithRetry(copyObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "rename object failed, src cos key: %s, dst cos key: %s, " +
                            "exception: %s", srcKey,
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

    @Override
    public void close() {
        if (null != this.cosClient) {
            this.cosClient.shutdown();
        }
    }

    // process Exception and print detail
    private void handleException(Exception e, String key) throws IOException {
        String cosPath = "cosn://" + bucketName + key;
        String exceptInfo = String.format("%s : %s", cosPath, e.toString());
        throw new IOException(exceptInfo);
    }

    @Override
    public boolean retrieveBlock(String key, long byteRangeStart,
                                 long blockSize,
                                 String localBlockPath) throws IOException {
        long fileSize = getFileLength(key);
        long byteRangeEnd = 0;
        try {
            GetObjectRequest request = new GetObjectRequest(this.bucketName,
                    key);
            this.setEncryptionMetadata(request, new ObjectMetadata());
            if (fileSize > 0) {
                byteRangeEnd = Math.min(fileSize - 1,
                        byteRangeStart + blockSize - 1);
                request.setRange(byteRangeStart, byteRangeEnd);
            }
            cosClient.getObject(request, new File(localBlockPath));
            return true;
        } catch (Exception e) {
            String errMsg =
                    String.format("retrieveBlock key %s with range [%d - %d] " +
                                    "occur a exception: %s",
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
        this.setEncryptionMetadata(getObjectMetadataRequest, new ObjectMetadata());
        try {
            ObjectMetadata objectMetadata =
                    (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
            return objectMetadata.getContentLength();
        } catch (Exception e) {
            String errMsg = String.format("getFileLength occur a exception, " +
                            "key %s, exception: %s",
                    key, e.getMessage());
            LOG.error(errMsg);
            handleException(new Exception(errMsg), key);
            return 0; // never will get here
        }
    }

    private <X> void callCOSClientWithSSECOS(X request, ObjectMetadata objectMetadata) {
        try {
            objectMetadata.setServerSideEncryption(SSEAlgorithm.AES256.getAlgorithm());
            if (request instanceof PutObjectRequest) {
                ((PutObjectRequest) request).setMetadata(objectMetadata);
            } else if (request instanceof UploadPartRequest) {
                ((UploadPartRequest) request).setObjectMetadata(objectMetadata);
            } else if (request instanceof CopyObjectRequest) {
                ((CopyObjectRequest) request).setNewObjectMetadata(objectMetadata);
            } else if (request instanceof InitiateMultipartUploadRequest) {
                ((InitiateMultipartUploadRequest) request).setObjectMetadata(objectMetadata);
            } else {
                throw new IOException("Set SSE_COS request no such method");
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("callCOSClientWithSSECOS failed:" +
                            " %s", e.toString());
            LOG.error(errMsg);
        }
    }

    private <X> void callCOSClientWithSSEC(X request, SSECustomerKey SSEKey) {
        try {
            if (request instanceof PutObjectRequest) {
                ((PutObjectRequest) request).setSSECustomerKey(SSEKey);
            } else if (request instanceof UploadPartRequest) {
                ((UploadPartRequest) request).setSSECustomerKey(SSEKey);
            } else if (request instanceof GetObjectMetadataRequest) {
                ((GetObjectMetadataRequest) request).setSSECustomerKey(SSEKey);
            } else if (request instanceof CopyObjectRequest) {
                ((CopyObjectRequest) request).setDestinationSSECustomerKey(SSEKey);
                ((CopyObjectRequest) request).setSourceSSECustomerKey(SSEKey);
            } else if (request instanceof GetObjectRequest) {
                ((GetObjectRequest) request).setSSECustomerKey(SSEKey);
            } else if (request instanceof InitiateMultipartUploadRequest) {
                ((InitiateMultipartUploadRequest) request).setSSECustomerKey(SSEKey);
            } else {
                throw new IOException("Set SSE_C request no such method");
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("callCOSClientWithSSEC failed:" +
                                    " %s", e.toString());
            LOG.error(errMsg);
        }
    }

    private <X> void setEncryptionMetadata(X request, ObjectMetadata objectMetadata) {
        switch (encryptionSecrets.getEncryptionMethod()) {
             case SSE_C:
                callCOSClientWithSSEC(request, new SSECustomerKey(encryptionSecrets.getEncryptionKey()));
                break;
             case SSE_COS:
                callCOSClientWithSSECOS(request, objectMetadata);
                break;
             case NONE:
             default:
                break;
        }
    }

    private void CheckEncryptionMethod(ClientConfig config,
                                       CosEncryptionMethods CosSSE, String SSEKey) throws IOException {
        int sseKeyLen = StringUtils.isBlank(SSEKey) ? 0 : SSEKey.length();

        String description = "Encryption key:";
        if (SSEKey == null) {
            description += "null ";
        } else {
            switch (sseKeyLen) {
                case 0:
                    description += " empty"; break;
                case 1:
                    description += " of length 1"; break;
                default:
                    description = description + " of length " + sseKeyLen + " ending with "
                            + SSEKey.charAt(sseKeyLen - 1);
            }
        }

        switch (CosSSE) {
            case SSE_C:
                LOG.debug("Using SSE_C with {}", description);
                config.setHttpProtocol(HttpProtocol.https);
                if (sseKeyLen == 0) {
                    throw new IOException("missing encryption key for SSE_C ");
                } else if(!SSEKey.matches(CosNConfigKeys.BASE64_Pattern)) {
                    throw new IOException("encryption key need to Base64 encoding for SSE_C ");
                }
                break;
            case SSE_COS:
                if (sseKeyLen != 0) {
                    LOG.debug("Using SSE_COS");
                    throw new IOException("SSE_COS to encryption with key error: "
                            + " (" + description + ")");
                }
                break;
            case NONE:
            default:
                LOG.debug("Data is unencrypted");
                break;
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
                    if (((UploadPartRequest) request).getInputStream() instanceof BufferInputStream) {
                        ((UploadPartRequest) request).getInputStream().mark((int) ((UploadPartRequest) request).getPartSize());
                    }
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
                    COSObject cosObject =
                            this.cosClient.getObject((GetObjectRequest) request);
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
                        "all cos sdk failed, retryIndex: [%d / %d], call " +
                                "method: %s, exception: %s",
                        retryIndex, this.maxRetryTimes, sdkMethod,
                        cse.toString());
                int statusCode = cse.getStatusCode();
                // 对5xx错误进行重试
                if (statusCode / 100 == 5) {
                    if (retryIndex <= this.maxRetryTimes) {
                        LOG.info(errMsg, cse);
                        long sleepLeast = retryIndex * 300L;
                        long sleepBound = retryIndex * 500L;
                        try {
                            if (request instanceof UploadPartRequest) {
                                LOG.info("upload part request input stream " +
                                        "retry reset.....");
                                if (((UploadPartRequest) request).getInputStream() instanceof BufferInputStream) {
                                    ((UploadPartRequest) request).getInputStream().reset();
                                }
                            }
                            Thread.sleep(
                                    ThreadLocalRandom.current().nextLong(sleepLeast, sleepBound));
                            ++retryIndex;
                        } catch (InterruptedException e) {
                            throw new IOException(e.toString());
                        }
                    } else {
                        LOG.error(errMsg, cse);
                        throw new IOException(errMsg);
                    }
                } else {
                    throw cse;
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

}
