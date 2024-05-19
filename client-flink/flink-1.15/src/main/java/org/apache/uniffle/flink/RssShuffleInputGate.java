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

package org.apache.uniffle.flink;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.uniffle.common.config.RssConf;
import org.apache.uniffle.flink.reader.RemoteShuffleInputGate;
import org.apache.uniffle.flink.reader.virtual.FakedRssInputChannel;

public class RssShuffleInputGate extends IndexedInputGate {

  private RemoteShuffleInputGate inputGate;

  public RssShuffleInputGate(
      String taskName,
      int gateIndex,
      InputGateDeploymentDescriptor gateDescriptor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      BufferDecompressor bufferDecompressor,
      int numConcurrentReading,
      Configuration configuration,
      RssConf conf) {

    inputGate =
        new RemoteShuffleInputGate(
            taskName,
            gateIndex,
            availabilityHelper,
            gateDescriptor,
            bufferPoolFactory,
            bufferDecompressor,
            gateDescriptor.getConsumedSubpartitionIndexRange().getStartIndex(),
            gateDescriptor.getConsumedSubpartitionIndexRange().getEndIndex(),
            configuration,
            numConcurrentReading,
            conf);
  }

  @Override
  public int getGateIndex() {
    return inputGate.getGateIndex();
  }

  @Override
  public List<InputChannelInfo> getUnfinishedChannels() {
    return Collections.emptyList();
  }

  @Override
  public void triggerDebloating() {}

  @Override
  public int getNumberOfInputChannels() {
    return inputGate.getNumberOfInputChannels();
  }

  @Override
  public boolean isFinished() {
    return inputGate.isFinished();
  }

  @Override
  public EndOfDataStatus hasReceivedEndOfData() {
    return EndOfDataStatus.DRAINED;
  }

  @Override
  public Optional<BufferOrEvent> getNext() {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Optional<BufferOrEvent> pollNext() throws IOException {
    return inputGate.pollNext();
  }

  @Override
  public void sendTaskEvent(TaskEvent event) {
    throw new FlinkRuntimeException("Not implemented yet.");
  }

  @Override
  public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
    throw new FlinkRuntimeException("Not implemented yet.");
  }

  @Override
  public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {}

  @Override
  public InputChannel getChannel(int channelIndex) {
    return new FakedRssInputChannel(getGateIndex(), channelIndex);
  }

  @Override
  public void setup() throws IOException {
    inputGate.setup();
  }

  @Override
  public void requestPartitions() throws IOException {}

  @Override
  public CompletableFuture<Void> getStateConsumedFuture() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void finishReadRecoveredState() throws IOException {}

  @Override
  public void close() throws Exception {
    inputGate.close();
  }
}
