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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.uniffle.common.config.RssConf;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_INPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.METRIC_GROUP_OUTPUT;
import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.createShuffleIOOwnerMetricGroup;
import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;

public class RssEnvironment implements ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> {

  private final Object lock = new Object();
  protected boolean isClosed;
  protected final RssConf conf;
  protected final NetworkBufferPool networkBufferPool;
  protected final ResultPartitionManager resultPartitionManager;
  private final RssResultPartitionFactory resultPartitionFactory;
  private final RssInputGateFactory inputGateFactory;

  public RssEnvironment(NetworkBufferPool networkBufferPool,
      ResultPartitionManager resultPartitionManager,
      RssResultPartitionFactory resultPartitionFactory,
      RssInputGateFactory inputGateFactory,
      RssConf conf) {
    this.networkBufferPool = networkBufferPool;
    this.resultPartitionManager = resultPartitionManager;
    this.conf = conf;
    this.resultPartitionFactory = resultPartitionFactory;
    this.inputGateFactory = inputGateFactory;
    this.isClosed = false;
  }

  @Override
  public int start() throws IOException {
    return 0;
  }

  @Override
  public ShuffleIOOwnerContext createShuffleIOOwnerContext(
      String ownerName, ExecutionAttemptID executionAttemptID, MetricGroup metricGroup) {
    MetricGroup nettyGroup = createShuffleIOOwnerMetricGroup(checkNotNull(metricGroup));
    return new ShuffleIOOwnerContext(
        checkNotNull(ownerName),
        checkNotNull(executionAttemptID),
        metricGroup,
        nettyGroup.addGroup(METRIC_GROUP_OUTPUT),
        nettyGroup.addGroup(METRIC_GROUP_INPUT));
  }

  @Override
  public List<ResultPartitionWriter> createResultPartitionWriters(
      ShuffleIOOwnerContext shuffleIOOwnerContext, List<ResultPartitionDeploymentDescriptor> list) {
    synchronized (lock) {
      checkState(!isClosed, "The RssEnvironment has already been shut down.");
      ResultPartitionWriter[] resultPartitions = new ResultPartitionWriter[list.size()];
      for (int index = 0; index < resultPartitions.length; index++) {
        resultPartitions[index] =
                createResultPartitionWriters(
                shuffleIOOwnerContext,
                index,
                list.get(index),
                conf);
      }
      return Arrays.asList(resultPartitions);
    }
  }

  public ResultPartitionWriter createResultPartitionWriters(
          ShuffleIOOwnerContext ownerContext,
          int index,
          ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor,
          RssConf conf) {
    return resultPartitionFactory.create(
            ownerContext.getOwnerName(), index, resultPartitionDeploymentDescriptor, conf);
  }

  @Override
  public void releasePartitionsLocally(Collection<ResultPartitionID> collection) {}

  @Override
  public Collection<ResultPartitionID> getPartitionsOccupyingLocalResources() {
    return null;
  }

  @Override
  public List<IndexedInputGate> createInputGates(
      ShuffleIOOwnerContext shuffleIOOwnerContext,
      PartitionProducerStateProvider partitionProducerStateProvider,
      List<InputGateDeploymentDescriptor> list) {
    return null;
  }

  @Override
  public boolean updatePartitionInfo(
      ExecutionAttemptID executionAttemptID, PartitionInfo partitionInfo)
      throws IOException, InterruptedException {
    return false;
  }

  @Override
  public void close() throws Exception {}
}
