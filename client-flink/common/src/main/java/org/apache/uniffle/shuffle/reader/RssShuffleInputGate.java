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

package org.apache.uniffle.shuffle.reader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.client.api.ShuffleReadClient;
import org.apache.uniffle.common.ShuffleServerInfo;
import org.apache.uniffle.shuffle.RssShuffleDescriptor;

import static org.apache.flink.shaded.guava30.com.google.common.base.Preconditions.checkState;

public class RssShuffleInputGate extends IndexedInputGate {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleInputGate.class);

  /** Name of the corresponding computing task. */
  private final String taskName;

  /** Index of the gate of the corresponding computing task. */
  private final int gateIndex;

  /** Deployment descriptor for a single input gate instance. */
  private final InputGateDeploymentDescriptor gateDescriptor;

  private final int numConcurrentReading;

  private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

  private final BufferDecompressor bufferDecompressor;

  private final int[] clientIndexMap;
  private final int[] channelIndexMap;
  private final int[] numSubPartitionsHasNotConsumed;

  private long numUnconsumedSubpartitions;

  private int startSubIndex;
  private int endSubIndex;
  private long pendingEndOfDataEvents;

  private final List<ShuffleReadClient> shuffleReadClients = new ArrayList<>();

  private Object lock = new Object();

  private Queue<Pair<Buffer, InputChannelInfo>> receivedBuffers = new LinkedList<>();

  public RssShuffleInputGate(
      String taskName,
      boolean shuffleChannels,
      int gateIndex,
      int networkBufferSize,
      InputGateDeploymentDescriptor gateDescriptor,
      int numConcurrentReading,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      BufferDecompressor bufferDecompressor,
      int startSubIndex,
      int endSubIndex) {

    this.taskName = taskName;
    this.gateIndex = gateIndex;
    this.gateDescriptor = gateDescriptor;
    this.numConcurrentReading = numConcurrentReading;
    this.bufferPoolFactory = bufferPoolFactory;
    this.bufferDecompressor = bufferDecompressor;

    int numChannels = gateDescriptor.getShuffleDescriptors().length;
    this.clientIndexMap = new int[numChannels];
    this.channelIndexMap = new int[numChannels];
    this.numSubPartitionsHasNotConsumed = new int[numChannels];
    this.numUnconsumedSubpartitions = initShuffleReadClients(networkBufferSize, shuffleChannels);
    this.pendingEndOfDataEvents = numUnconsumedSubpartitions;
    this.startSubIndex = startSubIndex;
    this.endSubIndex = endSubIndex;
  }

  private long initShuffleReadClients(int bufferSize, boolean shuffleChannels) {

    checkState(endSubIndex >= startSubIndex);
    int numSubpartitionsPerChannel = endSubIndex - startSubIndex + 1;
    long numUnconsumedSubpartitions = 0;

    // left element is index
    List<Pair<Integer, ShuffleDescriptor>> descriptors =
        IntStream.range(0, gateDescriptor.getShuffleDescriptors().length)
            .mapToObj(i -> Pair.of(i, gateDescriptor.getShuffleDescriptors()[i]))
            .collect(Collectors.toList());

    int clientIndex = 0;
    for (Pair<Integer, ShuffleDescriptor> descriptor : descriptors) {
      RssShuffleDescriptor remoteDescriptor = (RssShuffleDescriptor) descriptor.getRight();
      List<ShuffleServerInfo> mapPartitionLocation =
          remoteDescriptor.getShuffleResource().getMapPartitionLocation();
      ShuffleReadClient shuffleReadClient = null;
      shuffleReadClients.add(shuffleReadClient);
      numSubPartitionsHasNotConsumed[descriptor.getLeft()] = numSubpartitionsPerChannel;
      numUnconsumedSubpartitions += numSubpartitionsPerChannel;
      clientIndexMap[descriptor.getLeft()] = clientIndex;
      channelIndexMap[clientIndex] = descriptor.getLeft();
      ++clientIndex;
    }
    return numUnconsumedSubpartitions;
  }

  @Override
  public int getGateIndex() {
    return gateIndex;
  }

  @Override
  public List<InputChannelInfo> getUnfinishedChannels() {
    return Collections.emptyList();
  }

  @Override
  public int getBuffersInUseCount() {
    return 0;
  }

  @Override
  public void announceBufferSize(int bufferSize) {}

  @Override
  public int getNumberOfInputChannels() {
    return shuffleReadClients.size();
  }

  @Override
  public boolean isFinished() {
    synchronized (lock) {
      return allReadersEOF() && receivedBuffers.isEmpty();
    }
  }

  private boolean allReadersEOF() {
    return numUnconsumedSubpartitions <= 0;
  }

  @Override
  public boolean hasReceivedEndOfData() {
    return false;
  }

  @Override
  public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
    return Optional.empty();
  }

  @Override
  public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {
    return Optional.empty();
  }

  @Override
  public void sendTaskEvent(TaskEvent event) throws IOException {}

  @Override
  public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {}

  @Override
  public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {}

  @Override
  public InputChannel getChannel(int channelIndex) {
    return null;
  }

  @Override
  public void setup() throws IOException {}

  @Override
  public void requestPartitions() throws IOException {}

  @Override
  public CompletableFuture<Void> getStateConsumedFuture() {
    return null;
  }

  @Override
  public void finishReadRecoveredState() throws IOException {}

  @Override
  public void close() throws Exception {}
}
