package org.apache.hadoop.fs.cosn;

import com.qcloud.cos.http.IdleConnectionMonitorThread;
import com.qcloud.cos.utils.ExceptionUtils;
import com.qcloud.cos.utils.Jackson;
import com.qcloud.cos.thirdparty.org.apache.http.HttpResponse;
import com.qcloud.cos.thirdparty.org.apache.http.HttpStatus;
import com.qcloud.cos.thirdparty.org.apache.http.StatusLine;
import com.qcloud.cos.thirdparty.org.apache.http.client.HttpClient;
import com.qcloud.cos.thirdparty.org.apache.http.client.config.RequestConfig;
import com.qcloud.cos.thirdparty.org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import com.qcloud.cos.thirdparty.org.apache.http.client.methods.HttpPost;
import com.qcloud.cos.thirdparty.org.apache.http.client.methods.HttpRequestBase;
import com.qcloud.cos.thirdparty.org.apache.http.client.protocol.HttpClientContext;
import com.qcloud.cos.thirdparty.org.apache.http.entity.InputStreamEntity;
import com.qcloud.cos.thirdparty.org.apache.http.impl.client.HttpClientBuilder;
import com.qcloud.cos.thirdparty.org.apache.http.impl.client.HttpClients;
import com.qcloud.cos.thirdparty.org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import com.qcloud.cos.thirdparty.org.apache.http.protocol.HttpContext;
import com.qcloud.cos.thirdparty.org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class TencentPolarisSidecarClient {
    private static final Logger LOG = LoggerFactory.getLogger(TencentPolarisSidecarClient.class);

    private static final int MAX_TOTAL_CONN = 100;
    private static final int DEFAULT_MAX_PER_ROUTE = 10;
    private static final int IDLE_CONN_TIME_MS = 60000;
    private static final int DEFAULT_CONNECTION_REQUEST_TIMEOUT = -1;
    private static final int DEFAULT_CONNECTION_TIMEOUT = 1000;
    private static final int DEFAULT_SOCKET_TIMEOUT = 1000;

    private HttpClient httpClient;
    private RequestConfig requestConfig;
    private final String polarisSideSarAddress;

    private static final PoolingHttpClientConnectionManager connectionManager;
    private static final IdleConnectionMonitorThread idleConnectionMonitor;

    static {
        connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(MAX_TOTAL_CONN);
        connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_PER_ROUTE);
        connectionManager.setValidateAfterInactivity(1);
        idleConnectionMonitor = new IdleConnectionMonitorThread(connectionManager);
        idleConnectionMonitor.setIdleAliveMS(IDLE_CONN_TIME_MS);
        idleConnectionMonitor.setDaemon(true);
        idleConnectionMonitor.start();
    }

    public TencentPolarisSidecarClient(String address) {
        this.polarisSideSarAddress = address;
        initHttpClient();
    }

    private void initHttpClient() {
        HttpClientBuilder httpClientBuilder = HttpClients.custom().setConnectionManager(connectionManager);
        this.httpClient = httpClientBuilder.build();
        this.requestConfig =
                RequestConfig.custom()
                        .setContentCompressionEnabled(false)
                        .setConnectionRequestTimeout(DEFAULT_CONNECTION_REQUEST_TIMEOUT)
                        .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
                        .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT).build();
    }

    private boolean isRequestSuccessful(HttpResponse httpResponse) {
        StatusLine statusLine = httpResponse.getStatusLine();
        int statusCode = -1;
        if (statusLine != null) {
            statusCode = statusLine.getStatusCode();
        }
        return statusCode / 100 == HttpStatus.SC_OK / 100;
    }

    public Instance getOneInstance(String namespace, String service) {
        LOG.debug("get one instance for {} and {} by polarisSidecar", namespace, service);
        String uuid = UUID.randomUUID().toString();
        HttpResponse httpResponse = null;
        HttpRequestBase httpRequest = null;
        try {
            httpRequest = buildGetOnInstanceHttpRequest(namespace, service, uuid);
            if (httpRequest != null) {
                HttpContext context = HttpClientContext.create();
                httpResponse = executeOneRequest(context, httpRequest);
                if (httpResponse != null) {
                    if (!isRequestSuccessful(httpResponse)) {
                        ErrorResponse errorResponse = ErrorResponse.FromString(responseBody(httpResponse));
                        if (errorResponse != null) {
                            LOG.error("get one instance for namespace:{}, service:{}," +
                                            " errorCode:{}, errorMsg:{}, requestId:{}",
                                    namespace, service, errorResponse.code, errorResponse.info, uuid);
                        }
                    } else {
                        GetOnInstanceResponse response = GetOnInstanceResponse.FromString(responseBody(httpResponse));
                        if (response != null && response.instances != null && !response.instances.isEmpty()) {
                            return response.instances.get(0);
                        } else {
                            LOG.error("get empty result for namespace:{}, service:{}, requestId:{}", namespace, service, uuid);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("get one instance for namespace failed:{}, service:{}, error:{}, requestId:{}", namespace, service, ex.getMessage(), uuid);
        } finally {
            if (httpRequest != null) {
                httpRequest.abort();
            }
            closeHttpResponseStream(httpResponse);
        }
        return null;
    }

    private void closeHttpResponseStream(HttpResponse httpResponse) {
        try {
            if (httpResponse != null && httpResponse.getEntity() != null
                    && httpResponse.getEntity().getContent() != null) {
                httpResponse.getEntity().getContent().close();
            }
        } catch (IOException e) {
            LOG.error("exception occur when close http response:", e);
        }
    }

    private String responseBody(HttpResponse httpResponse)
            throws Exception {
        return EntityUtils.toString(httpResponse.getEntity(), "utf-8");
    }

    private HttpRequestBase buildGetOnInstanceHttpRequest(String namespace, String service, String uuid) {
        String urlString = "http://" + this.polarisSideSarAddress + "/v1/GetOneInstance";
        try {
            HttpEntityEnclosingRequestBase httpRequestBase = new HttpPost();
            httpRequestBase.setURI(new URI(urlString));
            httpRequestBase.addHeader("Request-Id", uuid);
            byte[] requestContent = new GetOnInstanceRequest(namespace, service).JsonString().getBytes();
            InputStreamEntity reqEntity =
                    new InputStreamEntity(new ByteArrayInputStream(requestContent),
                            requestContent.length);
            httpRequestBase.setEntity(reqEntity);
            httpRequestBase.setConfig(this.requestConfig);
            return httpRequestBase;
        } catch (URISyntaxException e) {
            LOG.error("build uri failed url:{}, uuid:{}", urlString, uuid);
            return null;
        }
    }

    public void updateResult(CallResultStat result) {
        LOG.debug("updateServiceCallResult {} and {} by polarisSidecar", result.namespace, result.service);
        String uuid = UUID.randomUUID().toString();
        HttpResponse httpResponse = null;
        HttpRequestBase httpRequest = null;
        try {
            httpRequest = buildUpdateResultHttpRequest(result, uuid);
            if (httpRequest != null) {
                HttpContext context = HttpClientContext.create();
                httpResponse = executeOneRequest(context, httpRequest);
                if (httpResponse != null) {
                    ErrorResponse errorResponse = ErrorResponse.FromString(responseBody(httpResponse));
                    if (!isRequestSuccessful(httpResponse)) {
                        if (errorResponse != null) {
                            LOG.error("update result failed for namespace:{}, service:{}, errorCode:{}, errorMsg:{}, requestId:{}",
                                    result.namespace, result.service, errorResponse.code, errorResponse.info, uuid);
                        }
                    } else {
                        LOG.debug("update result success for namespace:{}, service:{}, errorCode:{}, errorMsg:{}, requestId:{}",
                                result.namespace, result.service, errorResponse.code, errorResponse.info, uuid);
                    }
                }
            }
        } catch (Exception ex) {
            LOG.error("update result for namespace:{}, service:{}, error:{}, requestId:{}",
                    result.namespace, result.service, ex.getMessage(), uuid);
        } finally {
            if (httpRequest != null) {
                httpRequest.abort();
            }
            closeHttpResponseStream(httpResponse);
        }
    }

    private HttpRequestBase buildUpdateResultHttpRequest(CallResultStat result, String uuid) {
        String urlString = "http://" + this.polarisSideSarAddress + "/v1/UpdateCallResult";
        try {
            HttpEntityEnclosingRequestBase httpRequestBase = new HttpPost();
            httpRequestBase.setURI(new URI(urlString));
            httpRequestBase.addHeader("Request-Id", uuid);
            byte[] requestContent = result.JsonString().getBytes();
            InputStreamEntity reqEntity =
                    new InputStreamEntity(new ByteArrayInputStream(requestContent),
                            requestContent.length);
            httpRequestBase.setEntity(reqEntity);
            httpRequestBase.setConfig(this.requestConfig);
            return httpRequestBase;
        } catch (URISyntaxException e) {
            LOG.error("build uri failed, url:{}, uuid:{}", urlString, uuid);
            return null;
        }
    }

    private HttpResponse executeOneRequest(HttpContext context, HttpRequestBase httpRequest) {
        HttpResponse httpResponse;
        try {
            httpResponse = httpClient.execute(httpRequest, context);
        } catch (IOException e) {
            httpRequest.abort();
            throw ExceptionUtils.createClientException(e);
        }
        return httpResponse;
    }

    public static class GetOnInstanceRequest {
        public String service;
        public String namespace;

        public GetOnInstanceRequest(String namespace, String service) {
            this.service = service;
            this.namespace = namespace;
        }

        public String JsonString() {
            return Jackson.toJsonString(this);
        }
    }

    public static class CallResultStat {
        public String namespace;
        public String service;
        public List<CallResult> callResults;

        public String JsonString() {
            return Jackson.toJsonString(this);
        }

        public CallResultStat(String namespace, String service) {
            this.namespace = namespace;
            this.service = service;
            callResults = new ArrayList<>();
        }

        public void updateCallResult(CallResult item) {
            callResults.clear();
            callResults.add(item);
        }
    }

    public static class CallResult {
        public String id;
        public String host;
        public int port;
        public int retStatus;
        public int retCode;
        public long callDelay;
    }

    public static class ErrorResponse {
        public int code;
        public String info;

        public static ErrorResponse FromString(String jsonString) {
            return Jackson.fromJsonString(jsonString, ErrorResponse.class);
        }
    }

    public static class Instance {
        public String id;
        public String host;
        public int port;
    }

    public static class GetOnInstanceResponse {
        public List<Instance> instances;

        public static GetOnInstanceResponse FromString(String jsonString) {
            return Jackson.fromJsonString(jsonString, GetOnInstanceResponse.class);
        }
    }
}
