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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.SortMergeResultPartition;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.uniffle.flink.partition.RemoteShuffleResultPartition;
import org.apache.uniffle.flink.shuffle.RssShuffleDescriptor;
import org.apache.uniffle.flink.writer.RssPartitionedWriter;

import static com.google.common.base.Preconditions.checkState;

/**
 * In the process of completing this class, we heavily referenced the design and implementation of
 * {@link SortMergeResultPartition}, taking into account Flink's multi-version support. We
 * abstracted the more generic implementation into {@link RemoteShuffleResultPartition}, allowing
 * RssShuffleResultPartition to become more streamlined.
 */
@NotThreadSafe
public class RssShuffleResultPartition extends ResultPartition {

  /**
   * The more abstract remote partitioner wraps the common methods, serving as a supplement to
   * RssShuffleResultPartition. *
   */
  private RemoteShuffleResultPartition rssResultPartition;
  /**
   * The actual shuffling data is written out by RssPartitionedWriter, and we will write the data to
   * the remote ShuffleServer. *
   */
  private RssPartitionedWriter rssPartitionedWriter;

  private final RssShuffleDescriptor rssShuffleDescriptor;
  private final Configuration configuration;
  private final int networkBufferSize;
  private final Object lock = new Object();

  public RssShuffleResultPartition(
      String owningTaskName,
      int partitionIndex,
      ResultPartitionID partitionId,
      ResultPartitionType partitionType,
      int numSubpartitions,
      int numTargetKeyGroups,
      int networkBufferSize,
      ResultPartitionManager partitionManager,
      @Nullable BufferCompressor bufferCompressor,
      SupplierWithException<BufferPool, IOException> bufferPoolFactory,
      RssShuffleDescriptor rssShuffleDescriptor,
      Configuration configuration) {
    super(
        owningTaskName,
        partitionIndex,
        partitionId,
        partitionType,
        numSubpartitions,
        numTargetKeyGroups,
        partitionManager,
        bufferCompressor,
        bufferPoolFactory);
    this.rssShuffleDescriptor = rssShuffleDescriptor;
    this.configuration = configuration;
    this.networkBufferSize = networkBufferSize;
  }

  @Override
  public void setup() throws IOException {
    // The original ResultPartition setup.
    super.setup();

    // Ensure that initialization is synchronous to avoid multi-threading competition.
    synchronized (lock) {
      if (isReleased()) {
        throw new IOException("Result partition has been released.");
      }
      // Initialize rss partition writer so that data can be written to rss shuffle server.
      try {
        rssPartitionedWriter =
            new RssPartitionedWriter(rssShuffleDescriptor, configuration, numSubpartitions);
      } catch (Throwable throwable) {
        throw new IOException("Failed to create rss shuffle writer.", throwable);
      }

      this.rssResultPartition =
          new RemoteShuffleResultPartition(
              networkBufferSize, rssPartitionedWriter, numSubpartitions);
      this.rssResultPartition.setup(
          bufferPool, bufferCompressor, numBuffersOut, numBytesOut, numBytesProduced);

      // We apply for GuaranteedBuffers here to avoid deadlocks
      this.rssResultPartition.requestGuaranteedBuffers();
    }

    LOG.info("Rss-Shuffle partition {} initialized.", getPartitionId());
  }

  // ------------------------------------------------------------------------
  // Key method.
  // ------------------------------------------------------------------------

  /**
   * Writes the given serialized record to the target subpartition.
   *
   * @param record serialized record.
   * @param targetSubpartition target subpartition.
   * @throws IOException I/O exception of some sort has occurred.
   */
  @Override
  public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
    // Ensure that the partition is not finished before proceeding to the next step.
    checkInProduceState();

    // We write records to the specified partition.
    rssResultPartition.emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER, false);
  }

  /**
   * Writes the given serialized record to all subpartitions. One can also achieve the same effect
   * by emitting the same record to all subpartitions one by one, however, this method can have
   * better performance for the underlying implementation can do some optimizations, for example
   * coping the given serialized record only once to a shared channel which can be consumed by all
   * subpartitions.
   *
   * @param record serialized record.
   * @throws IOException I/O exception of some sort has occurred.
   */
  @Override
  public void broadcastRecord(ByteBuffer record) throws IOException {
    rssResultPartition.broadcast(record, Buffer.DataType.DATA_BUFFER);
  }

  /** Writes the given {@link AbstractEvent} to all channels. */
  @Override
  public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
    Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
    try {
      ByteBuffer serializedEvent = buffer.getNioBufferReadable();
      rssResultPartition.broadcast(serializedEvent, buffer.getDataType());
    } finally {
      buffer.recycleBuffer();
    }
  }

  /**
   * Finishes the result partition.
   *
   * <p>After this operation, it is not possible to add further data to the result partition.
   *
   * <p>For BLOCKING results, this will trigger the deployment of consuming tasks.
   */
  @Override
  public void finish() throws IOException {
    synchronized (lock) {
      checkState(!isReleased(), "Result partition is already released.");
      broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
      rssResultPartition.finish();
      super.finish();
      LOG.info("Rss-Shuffle partition {} finished.", getPartitionId());
    }
  }

  /**
   * Closes the partition writer which releases the allocated resource, for example the buffer pool.
   */
  @Override
  public void close() {
    synchronized (lock) {
      rssResultPartition.close();
      super.close();
      LOG.info("Rss-Shuffle partition {} closed.", getPartitionId());
    }
  }

  /** Manually trigger the consumption of data from all subpartitions. */
  @Override
  public void flushAll() {
    rssResultPartition.flushAll();
  }

  /** Manually trigger the consumption of data from the given subpartitions. */
  @Override
  public void flush(int subpartitionIndex) {
    flushAll();
  }

  /**
   * Notifies the downstream tasks that this {@code ResultPartitionWriter} have emitted all the user
   * records.
   *
   * @param mode tells if we should flush all records or not (it is false in case of
   *     stop-with-savepoint (--no-drain))
   */
  @Override
  public void notifyEndOfData(StopMode mode) throws IOException {
    synchronized (lock) {
      if (!rssResultPartition.isEndOfDataNotified()) {
        broadcastEvent(new EndOfData(mode), false);
        rssResultPartition.setEndOfDataNotified(true);
      }
    }
  }

  // ------------------------------------------------------------------------
  // Unimplemented method.
  // ------------------------------------------------------------------------

  @Override
  public int getNumberOfQueuedBuffers() {
    return 0;
  }

  @Override
  public long getSizeOfQueuedBuffersUnsafe() {
    return 0;
  }

  @Override
  public int getNumberOfQueuedBuffers(int targetSubpartition) {
    return 0;
  }

  @Override
  protected void releaseInternal() {}

  @Override
  public ResultSubpartitionView createSubpartitionView(
      int index, BufferAvailabilityListener availabilityListener) throws IOException {
    throw new UnsupportedOperationException("RSS Not supported.");
  }

  @Override
  public CompletableFuture<?> getAvailableFuture() {
    return AVAILABLE;
  }

  @Override
  public boolean isAvailable() {
    return super.isAvailable();
  }

  @Override
  public boolean isApproximatelyAvailable() {
    return super.isApproximatelyAvailable();
  }
}
