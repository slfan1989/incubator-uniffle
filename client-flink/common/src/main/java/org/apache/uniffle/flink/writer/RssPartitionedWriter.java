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

package org.apache.uniffle.flink.writer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.uniffle.common.config.RssConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.common.exception.RssException;
import org.apache.uniffle.common.util.BlockIdLayout;
import org.apache.uniffle.common.util.ChecksumUtils;
import org.apache.uniffle.common.util.JavaUtils;
import org.apache.uniffle.flink.buffer.RssBufferWithSubpartition;
import org.apache.uniffle.flink.config.RssFlinkConfig;
import org.apache.uniffle.flink.resource.DefaultRssShuffleResource;
import org.apache.uniffle.flink.resource.RssShuffleResourceDescriptor;
import org.apache.uniffle.flink.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.flink.utils.ShuffleUtils;

public class RssPartitionedWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RssPartitionedWriter.class);
  private RssShuffleDescriptor shuffleDesc;
  private ShuffleWriteClient shuffleWriteClient;
  private int numMappers;
  private int shuffleId;
  private int attemptId;
  private int bitmapSplitNum;
  private final Object lock = new Object();
  private final Map<Integer, Integer> partitionToSeqNo = Maps.newHashMap();
  private Set<Long> successBlockIds;
  private Set<Long> failedBlockIds;
  private final Map<Integer, List<Long>> partitionToBlocks = JavaUtils.newConcurrentMap();
  Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private BlockIdLayout blockIdLayout;
  private Set<ShuffleServerInfo> shuffleServerInfos;

  // server -> partitionId -> blockIds
  private Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds =
      Maps.newHashMap();

  public RssPartitionedWriter(
      RssShuffleDescriptor shuffleDesc, Configuration flinkConfig, int numMappers) {

    this.shuffleDesc = shuffleDesc;
    this.shuffleWriteClient = ShuffleUtils.createShuffleClient(flinkConfig);

    DefaultRssShuffleResource shuffleResource = shuffleDesc.getShuffleResource();
    RssShuffleResourceDescriptor shuffleResourceDescriptor =
        shuffleResource.getShuffleResourceDescriptor();
    this.shuffleId = shuffleResourceDescriptor.getShuffleId();
    this.attemptId = shuffleResourceDescriptor.getAttemptId();
    this.numMappers = numMappers;

    this.bitmapSplitNum = flinkConfig.getInteger(RssFlinkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM);

    this.successBlockIds = new HashSet<>();
    this.failedBlockIds = new HashSet<>();
    this.partitionToServers = shuffleResource.getPartitionToServers();
    int mapPartitionId = shuffleResourceDescriptor.getMapPartitionId();
    this.shuffleServerInfos = new HashSet<>(partitionToServers.get(mapPartitionId));

    RssConf rssConf = RssFlinkConfig.toRssConf(flinkConfig);
    this.blockIdLayout = BlockIdLayout.from(rssConf);
  }

  public void writeBuffers(List<RssBufferWithSubpartition> bufferWithSubpartitions) {
    if (bufferWithSubpartitions.isEmpty()) {
      return;
    }
    for (RssBufferWithSubpartition subpartition : bufferWithSubpartitions) {
      write(subpartition.getBuffer().asByteBuf(), subpartition.getSubpartitionIndex());
    }
  }

  public void write(ByteBuf byteBuf, int subIdx) {
    synchronized (lock) {
      List<ShuffleBlockInfo> sentBlocks = new ArrayList<>();
      ShuffleBlockInfo shuffleBlock = createShuffleBlock(byteBuf, subIdx);
      partitionToBlocks.computeIfAbsent(shuffleBlock.getPartitionId(), key -> Lists.newArrayList());
      partitionToBlocks.get(shuffleBlock.getPartitionId()).add(shuffleBlock.getBlockId());
      shuffleBlock
          .getShuffleServerInfos()
          .forEach(
              shuffleServerInfo -> {
                Map<Integer, Set<Long>> pToBlockIds =
                    serverToPartitionToBlockIds.computeIfAbsent(
                        shuffleServerInfo, k -> Maps.newHashMap());
                pToBlockIds
                    .computeIfAbsent(shuffleBlock.getPartitionId(), v -> Sets.newHashSet())
                    .add(shuffleBlock.getBlockId());
              });
      sentBlocks.add(shuffleBlock);
      JobID jobID = shuffleDesc.getJobId();
      SendShuffleDataResult result =
          shuffleWriteClient.sendShuffleData(jobID.toString(), sentBlocks, () -> false);
      successBlockIds.addAll(result.getSuccessBlockIds());
      failedBlockIds.addAll(result.getFailedBlockIds());
    }
  }

  protected ShuffleBlockInfo createShuffleBlock(ByteBuf byteBuf, int subIdx) {
    byte[] data = new byte[byteBuf.readableBytes()];
    final int uncompressLength = data.length;
    byte[] compressed = data;
    final long crc32 = ChecksumUtils.getCrc32(compressed);
    final long blockId = blockIdLayout.getBlockId(getNextSeqNo(subIdx), subIdx, attemptId);
    List<ShuffleServerInfo> shuffleServerInfos = partitionToServers.get(subIdx);
    return new ShuffleBlockInfo(
        shuffleId,
        subIdx,
        blockId,
        compressed.length,
        crc32,
        compressed,
        shuffleServerInfos,
        uncompressLength,
        byteBuf.readableBytes(),
        attemptId);
  }

  private int getNextSeqNo(int partitionId) {
    partitionToSeqNo.putIfAbsent(partitionId, 0);
    int seqNo = partitionToSeqNo.get(partitionId);
    partitionToSeqNo.put(partitionId, seqNo + 1);
    return seqNo;
  }

  public void startRegion(boolean isBroadcast) {
    LOG.info("regionStart, isBroadcast = {}", isBroadcast);
  }

  public void finishRegion() {
    JobID jobID = shuffleDesc.getJobId();
    shuffleWriteClient.reportShuffleResult(
        serverToPartitionToBlockIds, jobID.toString(), shuffleId, attemptId, bitmapSplitNum);
    LOG.info("regionFinish.");
  }

  public void finish() {
    sendCommit();
  }

  public void close() {
    shuffleWriteClient.close();
  }

  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    JobID jobId = shuffleDesc.getJobId();
    Future<Boolean> future =
        executor.submit(
            () ->
                shuffleWriteClient.sendCommit(
                    shuffleServerInfos, jobId.toString(), shuffleId, numMappers));
    int maxWait = 5000;
    int currentWait = 200;
    long start = System.currentTimeMillis();
    while (!future.isDone()) {
      LOG.info(
          "Wait commit to shuffle server for task[{}] cost {} ms",
          attemptId,
          (System.currentTimeMillis() - start));
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RssException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }
}
