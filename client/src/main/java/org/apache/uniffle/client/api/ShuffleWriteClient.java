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

package org.apache.uniffle.client.api;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import org.apache.uniffle.client.PartitionDataReplicaRequirementTracking;
import org.apache.uniffle.client.response.SendShuffleDataResult;
import org.apache.uniffle.common.PartitionRange;
import org.apache.uniffle.common.RemoteStorageInfo;
import org.apache.uniffle.common.ShuffleAssignmentsInfo;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.ShuffleDataDistributionType;
import org.apache.uniffle.common.ShuffleServerInfo;

public interface ShuffleWriteClient {

  default SendShuffleDataResult sendShuffleData(
      String appId,
      int stageAttemptNumber,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest) {
    throw new UnsupportedOperationException(
        this.getClass().getName()
            + " doesn't implement getShuffleAssignments with faultyServerIds");
  }

  SendShuffleDataResult sendShuffleData(
      String appId,
      List<ShuffleBlockInfo> shuffleBlockInfoList,
      Supplier<Boolean> needCancelRequest);

  void sendAppHeartbeat(String appId, long timeoutMs);

  void registerApplicationInfo(String appId, long timeoutMs, String user);

  default void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite) {
    registerShuffle(
        shuffleServerInfo,
        appId,
        shuffleId,
        partitionRanges,
        remoteStorage,
        dataDistributionType,
        maxConcurrencyPerPartitionToWrite,
        0);
  }

  void registerShuffle(
      ShuffleServerInfo shuffleServerInfo,
      String appId,
      int shuffleId,
      List<PartitionRange> partitionRanges,
      RemoteStorageInfo remoteStorage,
      ShuffleDataDistributionType dataDistributionType,
      int maxConcurrencyPerPartitionToWrite,
      int stageAttemptNumber);

  boolean sendCommit(
      Set<ShuffleServerInfo> shuffleServerInfoSet, String appId, int shuffleId, int numMaps);

  void registerCoordinators(String coordinators);

  Map<String, String> fetchClientConf(int timeoutMs);

  RemoteStorageInfo fetchRemoteStorage(String appId);

  void reportShuffleResult(
      Map<ShuffleServerInfo, Map<Integer, Set<Long>>> serverToPartitionToBlockIds,
      String appId,
      int shuffleId,
      long taskAttemptId,
      int bitmapNum);

  default ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency,
      Set<String> faultyServerIds) {
    throw new UnsupportedOperationException(
        this.getClass().getName()
            + " doesn't implement getShuffleAssignments with faultyServerIds");
  }

  ShuffleAssignmentsInfo getShuffleAssignments(
      String appId,
      int shuffleId,
      int partitionNum,
      int partitionNumPerRange,
      Set<String> requiredTags,
      int assignmentShuffleServerNumber,
      int estimateTaskConcurrency);

  Roaring64NavigableMap getShuffleResult(
      String clientType,
      Set<ShuffleServerInfo> shuffleServerInfoSet,
      String appId,
      int shuffleId,
      int partitionId);

  Roaring64NavigableMap getShuffleResultForMultiPart(
      String clientType,
      Map<ShuffleServerInfo, Set<Integer>> serverToPartitions,
      String appId,
      int shuffleId,
      Set<Integer> failedPartitions,
      PartitionDataReplicaRequirementTracking replicaRequirementTracking);

  void close();

  void unregisterShuffle(String appId, int shuffleId);

  void unregisterShuffle(String appId);
}
