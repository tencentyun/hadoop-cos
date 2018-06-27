/**
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * This class contains constants for configuration keys used in the cos file system.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CosNativeFileSystemConfigKeys extends CommonConfigurationKeys {

    public static final String COS_APPID_KEY = "fs.cosn.userinfo.appid";
    public static final String COS_SECRET_ID_KEY = "fs.cosn.userinfo.secretId";
    public static final String COS_SECRET_KEY_KEY = "fs.cosn.userinfo.secretKey";
    public static final String COS_REGION_KEY = "fs.cosn.userinfo.region";
    public static final String COS_ENDPOINT_SUFFIX_KEY ="fs.cosn.userinfo.endpoint_suffix";
    public static final String COS_USE_HTTPS_KEY = "fs.cosn.userinfo.usehttps";
    public static final boolean DEFAULT_USE_HTTPS = false;

    public static final String COS_BUFFER_DIR_KEY = "fs.cosn.buffer.dir";
    public static final String DEFAULT_BUFFER_DIR = "/tmp/hadoop_cos";

    public static final String UPLOAD_THREAD_POOL_SIZE_KEY = "fs.cosn.userinfo.upload_thread_pool";
    public static final int DEFAULT_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 3;          // 默认使用CPU核心数目 * 3

    public static final String COS_LOCAL_BLOCK_SIZE_KEY = "fs.cosn.local.block.size";
    public static final long DEFAULT_COS_LOCAL_BLOCK_SIZE = 1 * 1024 * 1024;

    public static final String READ_AHEAD_BLOCK_SIZE_KEY = "fs.cosn.read.ahead.block.size";
    public static final long DEFAULT_READ_AHEAD_BLOCK_SIZE = 64 * 1024 * 1024;                                  // 预取缓存块的大小

    public static final String READ_AHEAD_QUEUE_SIZE = "fs.cosn.read.ahead.queue.size";                       // 预取队列的大小
    public static final int DEFAULT_READ_AHEAD_QUEUE_SIZE = 10;                                                 // 预取队列的默认大小

    public static final String COS_NATIVE_REPLICATION_KEY = "fs.cosn.native.replication";
    public static final short COS_NATIVE_REPLICATION_DEFAULT = 1;
    public static final String COS_NATIVE_STREAM_BUFFER_SIZE_KEY = "cos.native.stream.buffer.size";
    public static final int COS_NATIVE_STREAM_BUFFER_SIZE_DEFAULT = 4096;
    public static final String COS_NATIVE_BYTES_PER_CHECKSUM_KEY = "cos.native.bytes-per-checksum";
    public static final int COS_NATIVE_BYTES_PER_CHECKSUM_DEFAULT = 512;
    public static final String COS_NATIVE_CLIENT_WRITE_PACKET_SIZE_KEY =
            "cosnative.client-write-packet-size";
    public static final int COS_NATIVE_CLIENT_WRITE_PACKET_SIZE_DEFAULT = 64 * 1024;
}

