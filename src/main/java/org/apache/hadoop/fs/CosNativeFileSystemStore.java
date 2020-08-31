package org.apache.hadoop.fs;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.utils.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.auth.COSCredentialProviderList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.fs.CosFileSystem.PATH_DELIMITER;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNativeFileSystemStore implements NativeFileSystemStore {
    private COSClient cosClient;
    private COSCredentialProviderList cosCredentialProviderList;
    private String bucketName;
    private StorageClass storageClass;
    private int maxRetryTimes;
    private int trafficLimit;
    private CosEncryptionSecrets encryptionSecrets;
    private CustomerDomainEndpointResolver customerDomainEndpointResolver;

    public static final Logger LOG =
            LoggerFactory.getLogger(CosNativeFileSystemStore.class);


    private void initCOSClient(URI uri, Configuration conf) throws IOException {
        this.cosCredentialProviderList =
                CosNUtils.createCosCredentialsProviderSet(uri, conf);

        ClientConfig config;

        String customerDomain = conf.get(CosNConfigKeys.CUSTOMER_DOMAIN);

        if (null == customerDomain) {

            String region = conf.get(CosNConfigKeys.COSN_REGION_KEY);
            if (null == region) {
                region = conf.get(CosNConfigKeys.COSN_REGION_PREV_KEY);
            }
            String endpoint_suffix =
                    conf.get(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
            if (null == endpoint_suffix) {
                endpoint_suffix =
                        conf.get(CosNConfigKeys.COSN_ENDPOINT_SUFFIX_PREV_KEY);
            }
            if (null == region && null == endpoint_suffix) {
                String exceptionMsg = String.format("config '%s' and '%s' specify at least one.",
                        CosNConfigKeys.COSN_REGION_KEY,
                        CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY);
                throw new IOException(exceptionMsg);
            }
            if (null == region) {
                config = new ClientConfig(new Region(""));
                config.setEndPointSuffix(endpoint_suffix);
            } else {
                config = new ClientConfig(new Region(region));
            }
        } else {
            config = new ClientConfig(new Region(""));
            LOG.info("Use Customer Domain is {}", customerDomain);
            this.customerDomainEndpointResolver = new CustomerDomainEndpointResolver();
            customerDomainEndpointResolver.setEndpoint(customerDomain);
            config.setEndpointBuilder(this.customerDomainEndpointResolver);
        }

        boolean useHttps = conf.getBoolean(
                CosNConfigKeys.COSN_USE_HTTPS_KEY,
                CosNConfigKeys.DEFAULT_USE_HTTPS);

        if (useHttps) {
            config.setHttpProtocol(HttpProtocol.https);
        }

        // Proxy settings
        String httpProxyIp = conf.getTrimmed(CosNConfigKeys.HTTP_PROXY_IP);
        int httpProxyPort = conf.getInt(CosNConfigKeys.HTTP_PROXY_PORT, CosNConfigKeys.DEFAULT_HTTP_PROXY_PORT);
        if (null != httpProxyIp && !httpProxyIp.isEmpty()) {
            config.setHttpProxyIp(httpProxyIp);
            if (httpProxyPort >= 0) {
                config.setHttpProxyPort(httpProxyPort);
            } else {
                if (useHttps) {
                    LOG.warn("Proxy IP set without port. Using HTTPS default port 443");
                    config.setHttpProxyPort(443);
                } else {
                    LOG.warn("Proxy IP set without port, Using HTTP default port 80");
                    config.setHttpProxyPort(80);
                }
            }
            // setting the proxy username and password
            String proxyUserName = conf.get(CosNConfigKeys.HTTP_PROXY_USERNAME);
            String proxyPassword = conf.get(CosNConfigKeys.HTTP_PROXY_PASSWORD);
            if ((null == proxyUserName || proxyUserName.isEmpty())
                    != (null == proxyPassword || proxyPassword.isEmpty())) {
                String exceptionMessage = String.format("Proxy error: '%s' or '%s' set without the other.",
                        CosNConfigKeys.HTTP_PROXY_USERNAME, CosNConfigKeys.HTTP_PROXY_PASSWORD);
                throw new IllegalArgumentException(exceptionMessage);
            }
            config.setProxyUsername(proxyUserName);
            config.setProxyPassword(proxyPassword);
        }

        String userAgent = conf.get(
                CosNConfigKeys.USER_AGENT,
                CosNConfigKeys.DEFAULT_USER_AGENT);
        String emrVersion = conf.get(CosNConfigKeys.TENCENT_EMR_VERSION_KEY);
        if (null != emrVersion && !emrVersion.isEmpty()) {
            userAgent = String.format("%s on %s", userAgent, emrVersion);
        }
        config.setUserAgent(userAgent);

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

        // Set the traffic limit
        this.trafficLimit = conf.getInt(CosNConfigKeys.TRAFFIC_LIMIT, CosNConfigKeys.DEFAULT_TRAFFIC_LIMIT);
        if (this.trafficLimit >= 0
                && (this.trafficLimit < 100 * 1024 * 8 || this.trafficLimit > 100 * 1024 * 1024 * 8)) {
            String exceptionMessage = String.format("The '%s' needs to be between %d and %d. but %d.",
                    CosNConfigKeys.TRAFFIC_LIMIT, 100 * 1024 * 8, 100 * 1024 * 1024 * 8, this.trafficLimit);
            throw new IllegalArgumentException(exceptionMessage);
        }

        this.cosClient = new COSClient(this.cosCredentialProviderList, config);
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        try {
            initCOSClient(uri, conf);
            this.bucketName = uri.getHost();
            String storageClass = conf.get(CosNConfigKeys.COSN_STORAGE_CLASS_KEY);
            if (null != storageClass && !storageClass.isEmpty()) {
                try {
                    this.storageClass = StorageClass.fromValue(storageClass);
                } catch (IllegalArgumentException e) {
                    String exceptionMessage = String.format("The specified storage class [%s] is invalid. " +
                                    "The supported storage classes are: %s, %s, %s, %s and %s.", storageClass,
                            StorageClass.Standard.toString(), StorageClass.Standard_IA.toString(),
                            StorageClass.Maz_Standard.toString(), StorageClass.Maz_Standard_IA.toString(),
                            StorageClass.Archive.toString());
                    throw new IllegalArgumentException(exceptionMessage);
                }
            }
            if (null != this.storageClass && StorageClass.Archive == this.storageClass) {
                LOG.warn("The storage class of the CosN FileSystem is set to {}. Some file operations may not be " +
                        "supported.", this.storageClass);
            }
        } catch (Exception e) {
            handleException(e, "");
        }
    }

    private void storeFileWithRetry(String key, InputStream inputStream,
                                    byte[] md5Hash, long length)
            throws IOException {
        try {
            ObjectMetadata objectMetadata = new ObjectMetadata();
            if(null != md5Hash){
                objectMetadata.setContentMD5(Base64.encodeAsString(md5Hash));
            }
            objectMetadata.setContentLength(length);

            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucketName, key, inputStream,
                            objectMetadata);
            if (null != this.storageClass) {
                putObjectRequest.setStorageClass(this.storageClass);
            }
            if (this.trafficLimit >= 0) {
                putObjectRequest.setTrafficLimit(this.trafficLimit);
            }
            this.setEncryptionMetadata(putObjectRequest, objectMetadata);

            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            LOG.debug("Store the file successfully. cos key: {}, ETag: {}.", key, putObjectResult.getETag());
        } catch (CosServiceException cse) {
            // 避免并发上传的问题
            int statusCode = cse.getStatusCode();
            if (statusCode == 409) {
                // Check一下这个文件是否已经存在
                FileMetadata fileMetadata = this.QueryObjectMetadata(key);
                if (null == fileMetadata) {
                    // 如果文件不存在，则需要抛出异常
                    handleException(cse, key);
                }
                LOG.warn("Upload the cos key [{}] concurrently", key);
            } else {
                // 其他错误都要抛出来
                handleException(cse, key);
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("Store the file failed, cos key: %s, exception: %s.", key, e.toString());
            handleException(new Exception(errMsg), key);
        }
    }

    @Override
    public void storeFile(String key, File file, byte[] md5Hash) throws IOException {
        if(null != md5Hash){
            LOG.debug("Store the file, local path: {}, length: {}, md5hash: {}.", file.getCanonicalPath(), file.length(),
                    Hex.encodeHexString(md5Hash));
        }
        storeFileWithRetry(key,
                new BufferedInputStream(new FileInputStream(file)), md5Hash,
                file.length());
    }

    @Override
    public void storeFile(String key, InputStream inputStream, byte[] md5Hash
            , long contentLength) throws IOException {
        if(null != md5Hash){
            LOG.debug("Store the file to the cos key: {}, input stream md5 hash: {}, content length: {}.", key,
                    Hex.encodeHexString(md5Hash),
                    contentLength);
        }
        storeFileWithRetry(key, inputStream, md5Hash, contentLength);
    }

    // for cos, storeEmptyFile means create a directory
    @Override
    public void storeEmptyFile(String key) throws IOException {
        if (!key.endsWith(PATH_DELIMITER)) {
            key = key + PATH_DELIMITER;
        }

        LOG.debug("Store a empty file to the key: {}.", key);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(0);
        InputStream input = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                key, input, objectMetadata);
        if (null != this.storageClass) {
            putObjectRequest.setStorageClass(this.storageClass);
        }
        try {
            PutObjectResult putObjectResult =
                    (PutObjectResult) callCOSClientWithRetry(putObjectRequest);
            LOG.debug("Store the empty file successfully, cos key: {}, ETag: {}.", key, putObjectResult.getETag());
        } catch (CosServiceException cse) {
            int statusCode = cse.getStatusCode();
            if (statusCode == 409) {
                // 并发上传文件导致，再check一遍文件是否存在
                FileMetadata fileMetadata = this.QueryObjectMetadata(key);
                if (null == fileMetadata) {
                    // 文件还是不存在，必须要抛出异常
                    handleException(cse, key);
                }
                LOG.warn("Upload the file [{}] concurrently.", key);
            } else {
                // 其他错误必须抛出
                handleException(cse, key);
            }
        } catch (Exception e) {
            String errMsg =
                    String.format("Store the empty file failed, cos key: %s, " +
                            "exception: %s", key, e.toString());
            handleException(new Exception(errMsg), key);
        }
    }

    public PartETag uploadPart(File file, String key, String uploadId,
                               int partNum, byte[] md5Hash) throws IOException {
        InputStream inputStream = new FileInputStream(file);
        return uploadPart(inputStream, key, uploadId, partNum, file.length(), md5Hash);
    }


    @Override
    public PartETag uploadPart(
            InputStream inputStream,
            String key, String uploadId, int partNum, long partSize, byte[] md5Hash) throws IOException {
        LOG.debug("Upload the part to the cos key [{}]. upload id: {}, part number: {}, part size: {}",
                key, uploadId, partNum, partSize);
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(this.bucketName);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(inputStream);
        uploadPartRequest.setPartNumber(partNum);
        uploadPartRequest.setPartSize(partSize);
        if(null != md5Hash){
            uploadPartRequest.setMd5Digest(Base64.encodeAsString(md5Hash));
        }
        uploadPartRequest.setKey(key);
        if (this.trafficLimit >= 0) {
            uploadPartRequest.setTrafficLimit(this.trafficLimit);
        }
        this.setEncryptionMetadata(uploadPartRequest, new ObjectMetadata());

        try {
            UploadPartResult uploadPartResult =
                    (UploadPartResult) callCOSClientWithRetry(uploadPartRequest);
            return uploadPartResult.getPartETag();
        } catch (Exception e) {
            String errMsg = String.format("The current thread:%d, "
                            + "cos key: %s, upload id: %s, part num: %d, " +
                            "exception: %s",
                    Thread.currentThread().getId(), key, uploadId, partNum, e.toString());
            handleException(new Exception(errMsg), key);
        }

        return null;
    }

    public void abortMultipartUpload(String key, String uploadId) throws IOException {
        LOG.info("Ready to abort the multipart upload. cos key: {}, upload id: {}.", key, uploadId);

        try {
            AbortMultipartUploadRequest abortMultipartUploadRequest =
                    new AbortMultipartUploadRequest(
                            bucketName, key, uploadId
                    );
            this.callCOSClientWithRetry(abortMultipartUploadRequest);
        } catch (Exception e) {
            String errMsg = String.format("Aborting the multipart upload failed. cos key: %s, upload id: %s. " +
                    "exception: %s.", key, uploadId, e.toString());
            handleException(new Exception(errMsg), key);
        }
    }

    public String getUploadId(String key) throws IOException {
        if (null == key || key.length() == 0) {
            return "";
        }

        InitiateMultipartUploadRequest initiateMultipartUploadRequest =
                new InitiateMultipartUploadRequest(bucketName, key);
        if (null != this.storageClass) {
            initiateMultipartUploadRequest.setStorageClass(this.storageClass);
        }
        this.setEncryptionMetadata(initiateMultipartUploadRequest, new ObjectMetadata());
        try {
            InitiateMultipartUploadResult initiateMultipartUploadResult =
                    (InitiateMultipartUploadResult) this.callCOSClientWithRetry(initiateMultipartUploadRequest);
            return initiateMultipartUploadResult.getUploadId();
        } catch (Exception e) {
            String errMsg =
                    String.format("Get the upload id failed, cos key: %s, " +
                            "exception: %s", key, e.toString());
            handleException(new Exception(errMsg), key);
        }

        return null;
    }

    public CompleteMultipartUploadResult completeMultipartUpload(
            String key, String uploadId, List<PartETag> partETagList) throws IOException {
        Collections.sort(partETagList, new Comparator<PartETag>() {
            @Override
            public int compare(PartETag o1, PartETag o2) {
                return o1.getPartNumber() - o2.getPartNumber();
            }
        });
        try {
            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    new CompleteMultipartUploadRequest(bucketName, key, uploadId,
                            partETagList);
            return (CompleteMultipartUploadResult) this.callCOSClientWithRetry(completeMultipartUploadRequest);
        } catch (Exception e) {
            String errMsg = String.format("Complete the multipart upload failed. cos key: %s, upload id: %s, " +
                    "exception: %s", key, uploadId, e.toString());
            handleException(new Exception(errMsg), key);
        }
        return null;
    }

    private FileMetadata QueryObjectMetadata(String key) throws IOException {
        LOG.debug("Query Object metadata. cos key: {}.", key);
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

            String ETag = objectMetadata.getETag();
            String crc64ecm = objectMetadata.getCrc64Ecma();
            String versionId = objectMetadata.getVersionId();
            FileMetadata fileMetadata =
                    new FileMetadata(key, fileSize, mtime,
                            !key.endsWith(PATH_DELIMITER), ETag, crc64ecm, versionId, objectMetadata.getStorageClass());
            LOG.debug("Retrieve the file metadata. cos key: {}, ETag:{}, length:{}, crc64ecm: {}.", key,
                    objectMetadata.getETag(), objectMetadata.getContentLength(), objectMetadata.getCrc64Ecma());
            return fileMetadata;
        } catch (CosServiceException e) {
            if (e.getStatusCode() != 404) {
                String errorMsg =
                        String.format("Retrieve the file metadata file failure. " +
                                "cos key: %s, exception: %s", key, e.toString());
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
        LOG.debug("Retrieve the key: {}.", key);
        GetObjectRequest getObjectRequest =
                new GetObjectRequest(this.bucketName, key);
        if (this.trafficLimit >= 0) {
            getObjectRequest.setTrafficLimit(this.trafficLimit);
        }
        this.setEncryptionMetadata(getObjectRequest, new ObjectMetadata());

        try {
            COSObject cosObject =
                    (COSObject) callCOSClientWithRetry(getObjectRequest);
            return cosObject.getObjectContent();
        } catch (Exception e) {
            String errMsg = String.format("Retrieving the cos key [%s] occurs an exception: %s", key, e.toString());
            handleException(new Exception(errMsg), key);
        }

        return null; // never will get here
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
            LOG.debug("Retrieve the cos key: {}, byte range start: {}.", key, byteRangeStart);
            long fileSize = getFileLength(key);
            long byteRangeEnd = fileSize - 1;
            GetObjectRequest getObjectRequest =
                    new GetObjectRequest(this.bucketName, key);
            if (this.trafficLimit >= 0) {
                getObjectRequest.setTrafficLimit(this.trafficLimit);
            }
            this.setEncryptionMetadata(getObjectRequest, new ObjectMetadata());

            if (byteRangeEnd >= byteRangeStart) {
                getObjectRequest.setRange(byteRangeStart, fileSize - 1);
            }
            COSObject cosObject =
                    (COSObject) callCOSClientWithRetry(getObjectRequest);
            return cosObject.getObjectContent();
        } catch (Exception e) {
            String errMsg =
                    String.format("Retrieving key [%s] with byteRangeStart [%d] " +
                                    "occurs an exception: %s.",
                            key, byteRangeStart, e.toString());
            handleException(new Exception(errMsg), key);
            return null; // never will get here
        }
    }

    @Override
    public InputStream retrieveBlock(String key, long byteRangeStart,
                                     long byteRangeEnd) throws IOException {
        LOG.debug("Retrieve the cos key: {}, byte range start: {}, byte range end: {}.", key, byteRangeStart,
                byteRangeEnd);
        try {
            GetObjectRequest request = new GetObjectRequest(this.bucketName,
                    key);
            request.setRange(byteRangeStart, byteRangeEnd);
            if (this.trafficLimit >= 0) {
                request.setTrafficLimit(this.trafficLimit);
            }
            this.setEncryptionMetadata(request, new ObjectMetadata());
            COSObject cosObject =
                    (COSObject) this.callCOSClientWithRetry(request);
            return cosObject.getObjectContent();
        } catch (CosServiceException e) {
            String errMsg =
                    String.format("Retrieving the key %s with the byteRangeStart [%d] " +
                                    "occurs an exception: %s.",
                            key, byteRangeStart, e.toString());
            handleException(new Exception(errMsg), key);
        } catch (CosClientException e) {
            String errMsg =
                    String.format("Retrieving key %s with the byteRangeStart [%d] and the byteRangeEnd [%d] " +
                                    "occurs an exception: %s.",
                            key, byteRangeStart, byteRangeEnd, e.toString());
            handleException(new Exception(errMsg), key);
        }

        return null;
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
        LOG.debug("List the cos key prefix: {}, max listing length: {}, delimiter: {}, prior last key: {}.",
                prefix,
                delimiter, maxListingLength, priorLastKey);
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
        ArrayList<FileMetadata> fileMetadataArray =
                new ArrayList<FileMetadata>();
        ArrayList<FileMetadata> commonPrefixArray =
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
            String fileEtag = cosObjectSummary.getETag();
            fileMetadataArray.add(new FileMetadata(filePath, fileLen, mtime,
                    true, fileEtag, null, null, cosObjectSummary.getStorageClass()));
        }
        List<String> commonPrefixes = objectListing.getCommonPrefixes();
        for (String commonPrefix : commonPrefixes) {
            if (!commonPrefix.startsWith(PATH_DELIMITER)) {
                commonPrefix = PATH_DELIMITER + commonPrefix;
            }
            commonPrefixArray.add(new FileMetadata(commonPrefix, 0, 0, false));
        }

        FileMetadata[] fileMetadata = new FileMetadata[fileMetadataArray.size()];
        for (int i = 0; i < fileMetadataArray.size(); ++i) {
            fileMetadata[i] = fileMetadataArray.get(i);
        }
        FileMetadata[] commonPrefixMetaData =
                new FileMetadata[commonPrefixArray.size()];
        for (int i = 0; i < commonPrefixArray.size(); ++i) {
            commonPrefixMetaData[i] = commonPrefixArray.get(i);
        }

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
        LOG.debug("Delete the cos key: {} from bucket: {}.", key, this.bucketName);
        try {
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, key);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format("Deleting the cos key [%s] occurs an exception: " +
                    "%s", key, e.toString());
            handleException(new Exception(errMsg), key);
        }
    }

    public void rename(String srcKey, String dstKey) throws IOException {
        LOG.debug("Rename the source cos key [{}] to the dest cos key [{}].", srcKey, dstKey);
        try {
            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName,
                            dstKey);
            // get the storage class of the source file
            FileMetadata sourceFileMetadata = this.retrieveMetadata(srcKey);
            if (null != sourceFileMetadata.getStorageClass()) {
                copyObjectRequest.setStorageClass(sourceFileMetadata.getStorageClass());
            }
            this.setEncryptionMetadata(copyObjectRequest, new ObjectMetadata());

            if (null != this.customerDomainEndpointResolver) {
                if (null != this.customerDomainEndpointResolver.getEndpoint()) {
                    copyObjectRequest.setSourceEndpointBuilder(this.customerDomainEndpointResolver);
                }
            }
            callCOSClientWithRetry(copyObjectRequest);
            DeleteObjectRequest deleteObjectRequest =
                    new DeleteObjectRequest(bucketName, srcKey);
            callCOSClientWithRetry(deleteObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "Rename object failed, source cos key: %s, dest cos key: %s, " +
                            "exception: %s", srcKey,
                    dstKey, e.toString());
            handleException(new Exception(errMsg), srcKey);
        }
    }

    @Override
    public void copy(String srcKey, String dstKey) throws IOException {
        LOG.info("Copy the source key [{}] to dest key [{}].", srcKey, dstKey);
        try {
            CopyObjectRequest copyObjectRequest =
                    new CopyObjectRequest(bucketName, srcKey, bucketName,
                            dstKey);
            FileMetadata sourceFileMetadata = this.retrieveMetadata(srcKey);
            if (null != sourceFileMetadata.getStorageClass()) {
                copyObjectRequest.setStorageClass(sourceFileMetadata.getStorageClass());
            }
            this.setEncryptionMetadata(copyObjectRequest, new ObjectMetadata());
            if (null != this.customerDomainEndpointResolver) {
                if (null != this.customerDomainEndpointResolver.getEndpoint()) {
                    copyObjectRequest.setSourceEndpointBuilder(this.customerDomainEndpointResolver);
                }
            }
            callCOSClientWithRetry(copyObjectRequest);
        } catch (Exception e) {
            String errMsg = String.format(
                    "Copy the object failed, src cos key: %s, dst cos key: %s, " +
                            "exception: %s", srcKey,
                    dstKey, e.toString());
            handleException(new Exception(errMsg), srcKey);
        }
    }

    @Override
    public void purge(String prefix) throws IOException {
        throw new IOException("purge not supported");
    }

    @Override
    public void dump() throws IOException {
        throw new IOException("dump not supported");
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
            if (this.trafficLimit >= 0) {
                request.setTrafficLimit(this.trafficLimit);
            }
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
                    String.format("Retrieving block key [%s] with range [%d - %d] " +
                                    "occurs an exception: %s",
                            key, byteRangeStart, byteRangeEnd, e.getMessage());
            handleException(new Exception(errMsg), key);
            return false; // never will get here
        }
    }

    @Override
    public long getFileLength(String key) throws IOException {
        LOG.info("getFile Length, cos key: {}", key);
        GetObjectMetadataRequest getObjectMetadataRequest =
                new GetObjectMetadataRequest(bucketName, key);
        this.setEncryptionMetadata(getObjectMetadataRequest, new ObjectMetadata());
        try {
            ObjectMetadata objectMetadata =
                    (ObjectMetadata) callCOSClientWithRetry(getObjectMetadataRequest);
            return objectMetadata.getContentLength();
        } catch (Exception e) {
            String errMsg = String.format("Getting the file length occurs an exception, " +
                            "cos key: %s, exception: %s",
                    key, e.getMessage());
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
                    description += " empty";
                    break;
                case 1:
                    description += " of length 1";
                    break;
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
                } else if (!SSEKey.matches(CosNConfigKeys.BASE64_Pattern)) {
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
                    if (((PutObjectRequest) request).getInputStream() instanceof BufferInputStream) {
                        ((PutObjectRequest) request).getInputStream()
                                .mark((int) ((PutObjectRequest) request).getMetadata().getContentLength());
                    }
                    return this.cosClient.putObject((PutObjectRequest) request);
                } else if (request instanceof UploadPartRequest) {
                    sdkMethod = "uploadPart";
                    if (((UploadPartRequest) request).getInputStream() instanceof BufferInputStream) {
                        ((UploadPartRequest) request).getInputStream()
                                .mark((int) ((UploadPartRequest) request).getPartSize());
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
                    return this.cosClient.copyObject((CopyObjectRequest) request);
                } else if (request instanceof GetObjectRequest) {
                    sdkMethod = "getObject";
                    return this.cosClient.getObject((GetObjectRequest) request);
                } else if (request instanceof ListObjectsRequest) {
                    sdkMethod = "listObjects";
                    return this.cosClient.listObjects((ListObjectsRequest) request);
                } else if (request instanceof InitiateMultipartUploadRequest) {
                    sdkMethod = "initiateMultipartUpload";
                    return this.cosClient.initiateMultipartUpload((InitiateMultipartUploadRequest) request);
                } else if (request instanceof CompleteMultipartUploadRequest) {
                    sdkMethod = "completeMultipartUpload";
                    return this.cosClient.completeMultipartUpload((CompleteMultipartUploadRequest) request);
                } else if (request instanceof AbortMultipartUploadRequest) {
                    sdkMethod = "abortMultipartUpload";
                    this.cosClient.abortMultipartUpload((AbortMultipartUploadRequest) request);
                    return new Object();
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
                String errorCode = cse.getErrorCode();
                LOG.debug("fail to retry statusCode {}, errorCode {}", statusCode, errorCode);
                // 对5xx错误进行重试
                if (request instanceof CopyObjectRequest && statusCode / 100 == 2 &&
                        errorCode != null && !errorCode.isEmpty()) {
                    if (retryIndex <= this.maxRetryTimes) {
                        LOG.info(errMsg, cse);
                        ++retryIndex;
                    } else {
                        LOG.error(errMsg, cse);
                        throw new IOException(errMsg);
                    }
                } else if (statusCode / 100 == 5) {
                    if (retryIndex <= this.maxRetryTimes) {
                        LOG.info(errMsg, cse);
                        long sleepLeast = retryIndex * 300L;
                        long sleepBound = retryIndex * 500L;
                        try {
                            if (request instanceof PutObjectRequest) {
                                LOG.info("Try to reset the put object request input stream.");
                                if (((PutObjectRequest) request).getInputStream() instanceof BufferInputStream) {
                                    ((PutObjectRequest) request).getInputStream().reset();
                                } else {
                                    LOG.error("The put object request input stream can not be reset, so it can not be" +
                                            " retried.");
                                    throw cse;
                                }
                            }

                            if (request instanceof UploadPartRequest) {
                                LOG.info("Try to reset the upload part request input stream.");
                                if (((UploadPartRequest) request).getInputStream() instanceof BufferInputStream) {
                                    ((UploadPartRequest) request).getInputStream().reset();
                                } else {
                                    LOG.error("The upload part request input stream can not be reset, so it can not " +
                                            "be retried" +
                                            ".");
                                    throw cse;
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
