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

package org.apache.uniffle.shuffle;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.shuffle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.util.ThreadUtils;
import org.apache.uniffle.shuffle.partition.RssResultPartitionInfo;
import org.apache.uniffle.shuffle.resource.DefaultRssShuffleResource;
import org.apache.uniffle.shuffle.resource.ShuffleResourceDescriptor;
import org.apache.uniffle.shuffle.resource.ShuffleTaskInfo;
import org.apache.uniffle.shuffle.utils.FlinkShuffleUtils;

import static org.apache.uniffle.shuffle.RssFlinkConfig.RSS_HEARTBEAT_INTERVAL;
import static org.apache.uniffle.shuffle.RssFlinkConfig.RSS_HEARTBEAT_TIMEOUT;
import static org.apache.uniffle.shuffle.RssFlinkConfig.RSS_QUOTA_USER;

public class RssShuffleMaster implements ShuffleMaster<RssShuffleDescriptor> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleMaster.class);
  private final ShuffleMasterContext shuffleMasterContext;
  private final long startTimeStamp;
  private String uniffleAppId;
  protected ShuffleWriteClient shuffleWriteClient;
  private ShuffleTaskInfo shuffleTaskInfo = new ShuffleTaskInfo();
  private boolean heartbeatStarted = false;
  private ScheduledExecutorService heartBeatScheduledExecutorService;

  public RssShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
    this.shuffleMasterContext = shuffleMasterContext;
    this.startTimeStamp = System.currentTimeMillis();
    this.heartBeatScheduledExecutorService =
        ThreadUtils.getDaemonSingleThreadScheduledExecutor("rss-heartbeat");
  }

  protected void registerCoordinator(Configuration config) {
    String coordinators = config.getString(RssFlinkConfig.RSS_COORDINATOR_QUORM);
    LOG.info("Start Registering Coordinator {}", coordinators);
    shuffleWriteClient.registerCoordinators(coordinators);
  }

  @Override
  public void start() throws Exception {
    ShuffleMaster.super.start();
  }

  @Override
  public void close() throws Exception {
    ShuffleMaster.super.close();
  }

  @Override
  public void registerJob(JobShuffleContext context) {
    JobID jobID = context.getJobId();
    if (shuffleWriteClient == null) {
      synchronized (RssShuffleMaster.class) {
        shuffleWriteClient =
            FlinkShuffleUtils.createShuffleClient(shuffleMasterContext.getConfiguration());
        uniffleAppId = FlinkShuffleUtils.toUniffleAppId(startTimeStamp, jobID);
        LOG.info("UniffleAppId : {}", uniffleAppId);
        registerCoordinator(shuffleMasterContext.getConfiguration());
      }
    }
  }

  private synchronized void startHeartbeat(Configuration config) {
    String user = config.getString(RSS_QUOTA_USER);
    long heartbeatInterval = config.getLong(RSS_HEARTBEAT_INTERVAL);
    long heartbeatTimeout = config.getLong(RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);

    shuffleWriteClient.registerApplicationInfo(uniffleAppId, heartbeatTimeout, user);
    if (!heartbeatStarted) {
      heartBeatScheduledExecutorService.scheduleAtFixedRate(
          () -> {
            try {
              String appId = uniffleAppId;
              shuffleWriteClient.sendAppHeartbeat(appId, heartbeatTimeout);
              LOG.info("Finish send heartbeat to coordinator and servers");
            } catch (Exception e) {
              LOG.warn("Fail to send heartbeat to coordinator and servers", e);
            }
          },
          heartbeatInterval / 2,
          heartbeatInterval,
          TimeUnit.MILLISECONDS);
      heartbeatStarted = true;
    }
  }

  @Override
  public void unregisterJob(JobID jobID) {
    ShuffleMaster.super.unregisterJob(jobID);
  }

  @Override
  public CompletableFuture<RssShuffleDescriptor> registerPartitionWithProducer(
      JobID jobID, PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
    CompletableFuture<RssShuffleDescriptor> future = new CompletableFuture<>();
    RssResultPartitionInfo rssResultPartitionInfo =
        new RssResultPartitionInfo(jobID, partitionDescriptor, producerDescriptor);
    ShuffleResourceDescriptor shuffleResourceDescriptor =
        shuffleTaskInfo.genShuffleResourceDescriptor(
            rssResultPartitionInfo.getShuffleId(),
            rssResultPartitionInfo.getTaskId(),
            rssResultPartitionInfo.getAttemptId());
    Configuration config = shuffleMasterContext.getConfiguration();
    Set<String> assignmentTags = new HashSet<>();
    ShuffleAssignmentsInfo response =
        shuffleWriteClient.getShuffleAssignments(
            uniffleAppId,
            shuffleResourceDescriptor.getShuffleId(),
            partitionDescriptor.getNumberOfSubpartitions(),
            1,
            assignmentTags,
            2,
            -1);
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges =
        response.getServerToPartitionRanges();
    for (Map.Entry<ShuffleServerInfo, List<PartitionRange>> entry :
        serverToPartitionRanges.entrySet()) {
      List<ShuffleServerInfo> shuffleServerInfos =
          Collections.singleton(entry.getKey()).stream().collect(Collectors.toList());
      DefaultRssShuffleResource resource =
          new DefaultRssShuffleResource(shuffleServerInfos, shuffleResourceDescriptor);
      RssShuffleDescriptor rsd =
          new RssShuffleDescriptor(rssResultPartitionInfo.getResultPartitionId(), jobID, resource);
      future.complete(rsd);
    }
    startHeartbeat(config);
    return future;
  }

  @Override
  public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {}

  @Override
  public MemorySize computeShuffleMemorySizeForTask(
      TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
    return ShuffleMaster.super.computeShuffleMemorySizeForTask(taskInputsOutputsDescriptor);
  }
}
