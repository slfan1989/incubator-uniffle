/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.uniffle.shuffle.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.factory.ShuffleClientFactory;
import org.apache.uniffle.common.util.Constants;
import org.apache.uniffle.shuffle.RssFlinkConfig;

public class FlinkShuffleUtils {

  public static ShuffleWriteClient createShuffleClient(Configuration conf) {
    int heartBeatThreadNum = conf.getInteger(RssFlinkConfig.RSS_CLIENT_HEARTBEAT_THREAD_NUM);
    int retryMax = conf.getInteger(RssFlinkConfig.RSS_CLIENT_RETRY_MAX);
    long retryIntervalMax = conf.getLong(RssFlinkConfig.RSS_CLIENT_RETRY_INTERVAL_MAX);
    String clientType = conf.get(RssFlinkConfig.RSS_CLIENT_TYPE);
    int replicaWrite = conf.getInteger(RssFlinkConfig.RSS_DATA_REPLICA_WRITE);
    int replicaRead = conf.getInteger(RssFlinkConfig.RSS_DATA_REPLICA_READ);
    int replica = conf.getInteger(RssFlinkConfig.RSS_DATA_REPLICA);
    boolean replicaSkipEnabled = conf.getBoolean(RssFlinkConfig.RSS_DATA_REPLICA_SKIP_ENABLED);
    int dataTransferPoolSize = conf.getInteger(RssFlinkConfig.RSS_DATA_TRANSFER_POOL_SIZE);
    int dataCommitPoolSize = conf.getInteger(RssFlinkConfig.RSS_DATA_COMMIT_POOL_SIZE);
    ShuffleWriteClient client =
        ShuffleClientFactory.getInstance()
            .createShuffleWriteClient(
                ShuffleClientFactory.newWriteBuilder()
                    .clientType(clientType)
                    .retryMax(retryMax)
                    .retryIntervalMax(retryIntervalMax)
                    .heartBeatThreadNum(heartBeatThreadNum)
                    .replica(replica)
                    .replicaWrite(replicaWrite)
                    .replicaWrite(replicaRead)
                    .replicaSkipEnabled(replicaSkipEnabled)
                    .dataTransferPoolSize(dataTransferPoolSize)
                    .dataCommitPoolSize(dataCommitPoolSize)
                    .rssConf(RssFlinkConfig.toRssConf(conf)));
    return client;
  }

  public static Set<String> genAssignmentTags(Configuration conf) {
    Set<String> assignmentTags = new HashSet<>();
    String rawTags = conf.getString(RssFlinkConfig.RSS_CLIENT_ASSIGNMENT_TAGS, "");
    if (StringUtils.isNotBlank(rawTags)) {
      rawTags = rawTags.trim();
      assignmentTags.addAll(Arrays.asList(rawTags.split(",")));
    }
    assignmentTags.add(Constants.SHUFFLE_SERVER_VERSION);
    return assignmentTags;
  }

  public static int getRequiredShuffleServerNumber(Configuration conf) {
    return conf.getInteger(RssFlinkConfig.RSS_CLIENT_ASSIGNMENT_SHUFFLE_SERVER_NUMBER);
  }
}