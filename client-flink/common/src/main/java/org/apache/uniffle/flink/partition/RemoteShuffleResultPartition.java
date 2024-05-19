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

package org.apache.uniffle.flink.partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.SortMergeResultPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.uniffle.flink.buffer.RssBufferWithSubpartition;
import org.apache.uniffle.flink.buffer.RssDataBuffer;
import org.apache.uniffle.flink.buffer.RssSortBuffer;
import org.apache.uniffle.flink.writer.RssPartitionedWriter;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Abstract the common methods of ResultPartition. */
public class RemoteShuffleResultPartition {

  // for logging
  private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleResultPartition.class);

  /** Size of network buffer and write buffer. */
  private final int networkBufferSize;

  /**
   * {@link RssDataBuffer} for records sent by broadcastRecord. Like {@link
   * SortMergeResultPartition}#broadcastSortBuffer
   */
  private RssDataBuffer broadcastWriteBuffer;

  /** {@link RssDataBuffer} for records sent by emitRecord. */
  private RssDataBuffer unicastWriteBuffer;

  /** All available network buffers can be used by this result partition for a data region. */
  private final LinkedList<MemorySegment> freeSegments = new LinkedList<>();

  /**
   * Number of guaranteed network buffers can be used by {@link #unicastWriteBuffer} and {@link
   * #broadcastWriteBuffer}.
   */
  private int numBuffersForSort;

  /** Utility to write data to rss shuffle servers. */
  private final RssPartitionedWriter rssPartitionedWriter;

  /** A dynamically sized buffer pool. */
  private BufferPool bufferPool;

  /** Subpartitions Num. */
  private int numSubpartitions;

  /** Compressor for Buffer. * */
  private BufferCompressor bufferCompressor;

  private boolean endOfDataNotified;

  private Counter numBuffersOut;
  private Counter numBytesOut;
  protected Counter numBytesProduced;

  public RemoteShuffleResultPartition(
      int networkBufferSize, RssPartitionedWriter rssPartitionedWriter, int numSubpartitions) {
    this.rssPartitionedWriter = rssPartitionedWriter;
    this.networkBufferSize = networkBufferSize;
    this.numSubpartitions = numSubpartitions;
  }

  public void setup(
      BufferPool bufferPool,
      BufferCompressor bufferCompressor,
      Counter numBuffersOut,
      Counter numBytesOut,
      Counter numBytesProduced) {
    this.bufferPool = bufferPool;
    this.bufferCompressor = bufferCompressor;
    this.numBuffersOut = numBuffersOut;
    this.numBytesOut = numBytesOut;
    this.numBytesProduced = numBytesProduced;
  }

  // ------------------------------------------------------------------------
  // Write the data out to the ShuffleServer.
  // ------------------------------------------------------------------------

  /**
   * Write the data to the ShuffleServer in a batched manner.
   *
   * <p>Call stack of the program for data writing.
   *
   * <p>RecordWriteOutput.collect() \-> pushToRecordWriter() \-> RecordWriter.emit() \->
   * ResultPartitionWriter.emitRecord() \-> RssShuffleResultPartition.emitRecord() \->
   * RemoteShuffleResultPartition.emit()
   *
   * @param record serialized record.
   * @param targetSubpartition target subpartition.
   * @param dataType identify the type of data
   * @param isBroadcast Whether to use broadcasting for data output. true broadcast;false
   *     non-broadcast
   * @throws IOException I/O exception of some sort has occurred.
   */
  public void emit(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast)
      throws IOException {

    // Create a buffer of the specified type based on the condition: if it is a Broadcast, create a
    // BroadcastWriteBuffer; if it is not a Broadcast, create an UnicastWriteBuffer.
    RssDataBuffer writeBuffer = isBroadcast ? getBroadcastWriteBuffer() : getUnicastWriteBuffer();

    // Attempt to write data into the writeBuffer.
    // If the writeBuffer is large enough, return after the data is written.
    // If the writeBuffer is not large enough, attempt to use writeLargeRecord to write out the data
    if (!writeBuffer.append(record, targetSubpartition, dataType)) {
      return;
    }

    // If all the data has been read,
    // release the writeBuffer and use the large record writing method to output the data.
    if (!writeBuffer.hasRemaining()) {
      // the record can not be appended to the free sort buffer because it is too large
      writeBuffer.release();
      writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
      return;
    }

    // Reaching this step indicates that the writeBuffer is insufficient to write all the data.
    // First, write out all the data in the writeBuffer, and then attempt to write the record again.
    flushWriteBuffer(writeBuffer, isBroadcast);
    writeBuffer.release();
    if (record.hasRemaining()) {
      emit(record, targetSubpartition, dataType, isBroadcast);
    }
  }

  /**
   * Write Large Record These records typically exceed the capacity of the WriterBuffer.
   *
   * @param record serialized record.
   * @param targetSubpartition target subpartition.
   * @param dataType identify the type of data.
   * @param isBroadcast Whether to use broadcasting for data output. true broadcast; false
   *     non-broadcast
   */
  private void writeLargeRecord(
      ByteBuffer record, int targetSubpartition, Buffer.DataType dataType, boolean isBroadcast) {
    rssPartitionedWriter.startRegion(isBroadcast);

    List<RssBufferWithSubpartition> toWrite = new ArrayList<>();
    Queue<MemorySegment> segments = new ArrayDeque<>(freeSegments);

    while (record.hasRemaining()) {
      if (segments.isEmpty()) {
        rssPartitionedWriter.writeBuffers(toWrite);
        toWrite.clear();
        segments = new ArrayDeque<>(freeSegments);
      }

      int toCopy = Math.min(record.remaining(), networkBufferSize);
      MemorySegment writeBuffer = checkNotNull(segments.poll());
      writeBuffer.put(0, record, toCopy);

      NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);
      RssBufferWithSubpartition bufferWithSubpartition =
          new RssBufferWithSubpartition(buffer, targetSubpartition);
      updateStatistics(bufferWithSubpartition, isBroadcast);
      toWrite.add(compressBufferIfPossible(bufferWithSubpartition));
    }

    rssPartitionedWriter.writeBuffers(toWrite);
    releaseFreeBuffers();
    rssPartitionedWriter.finishRegion();
  }

  /**
   * Flush WriteBuffer. We will write out the data stored in the buffer to the ShuffleServer.
   *
   * @param writeBuffer WriteBuffer.
   * @param isBroadcast Whether to use broadcasting for data output. true broadcast; false
   *     non-broadcast
   * @throws IOException I/O exception of some sort has occurred.
   */
  private void flushWriteBuffer(RssDataBuffer writeBuffer, boolean isBroadcast) throws IOException {
    if (writeBuffer == null || writeBuffer.isReleased() || !writeBuffer.hasRemaining()) {
      return;
    }

    writeBuffer.finish();

    Queue<MemorySegment> segments = new ArrayDeque<>(freeSegments);
    int numBuffersToWrite = segments.size();
    List<RssBufferWithSubpartition> toWrite = new ArrayList<>();
    rssPartitionedWriter.startRegion(isBroadcast);

    do {
      if (toWrite.size() >= numBuffersToWrite) {
        writeBuffers(toWrite);
        segments = new ArrayDeque<>(freeSegments);
      }

      RssBufferWithSubpartition bufferWithSubpartition = writeBuffer.getNextBuffer(segments.poll());
      if (bufferWithSubpartition == null) {
        writeBuffers(toWrite);
        break;
      }

      updateStatistics(bufferWithSubpartition, isBroadcast);
      toWrite.add(compressBufferIfPossible(bufferWithSubpartition));
    } while (true);

    releaseFreeBuffers();
  }

  public void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
    emit(record, 0, dataType, true);
  }

  public void flushAll() {
    try {
      flushUnicastWriteBuffer();
      flushBroadcastWriteBuffer();
    } catch (Throwable t) {
      LOG.error("Failed to flush the current sort buffer.", t);
      throw new RuntimeException(t);
    }
  }

  private void flushBroadcastWriteBuffer() throws IOException {
    if (broadcastWriteBuffer != null) {
      flushWriteBuffer(broadcastWriteBuffer, true);
      broadcastWriteBuffer.release();
      broadcastWriteBuffer = null;
    }
  }

  private void flushUnicastWriteBuffer() throws IOException {
    if (unicastWriteBuffer != null) {
      flushWriteBuffer(unicastWriteBuffer, false);
      unicastWriteBuffer.release();
      unicastWriteBuffer = null;
    }
  }

  private void writeBuffers(List<RssBufferWithSubpartition> buffers) throws IOException {
    rssPartitionedWriter.writeBuffers(buffers);
    buffers.forEach(buffer -> buffer.getBuffer().recycleBuffer());
    buffers.clear();
  }

  // ------------------------------------------------------------------------
  // Compressed data
  // ------------------------------------------------------------------------

  /**
   * If we have configured a compression algorithm, we attempt to compress the data.
   *
   * @param bufferWithSubpartition Data that needs to be compressed.
   * @return new RssBufferWithSubpartition.
   */
  private RssBufferWithSubpartition compressBufferIfPossible(
      RssBufferWithSubpartition bufferWithSubpartition) {
    Buffer buffer = bufferWithSubpartition.getBuffer();
    if (!canBeCompressed(buffer)) {
      return bufferWithSubpartition;
    }

    buffer = checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
    return new RssBufferWithSubpartition(buffer, bufferWithSubpartition.getSubpartitionIndex());
  }

  protected boolean canBeCompressed(Buffer buffer) {
    return bufferCompressor != null && buffer.isBuffer() && buffer.readableBytes() > 0;
  }

  // ------------------------------------------------------------------------
  // UnicastWriteBuffer & BroadcastWriteBuffer
  // ------------------------------------------------------------------------

  private RssDataBuffer getUnicastWriteBuffer() throws IOException {
    flushBroadcastWriteBuffer();

    if (unicastWriteBuffer != null
        && !unicastWriteBuffer.isFinished()
        && !unicastWriteBuffer.isReleased()) {
      return unicastWriteBuffer;
    }

    unicastWriteBuffer = createNewWriteBuffer();
    return unicastWriteBuffer;
  }

  private RssDataBuffer getBroadcastWriteBuffer() throws IOException {
    flushUnicastWriteBuffer();

    if (broadcastWriteBuffer != null
        && !broadcastWriteBuffer.isFinished()
        && !broadcastWriteBuffer.isReleased()) {
      return broadcastWriteBuffer;
    }

    broadcastWriteBuffer = createNewWriteBuffer();
    return broadcastWriteBuffer;
  }

  private RssDataBuffer createNewWriteBuffer() throws IOException {
    requestNetworkBuffers();
    return new RssSortBuffer(
        freeSegments, bufferPool, numSubpartitions, networkBufferSize, numBuffersForSort);
  }

  private void requestNetworkBuffers() throws IOException {
    requestGuaranteedBuffers();

    // avoid taking too many buffers in one result partition
    while (freeSegments.size() < bufferPool.getMaxNumberOfMemorySegments()) {
      MemorySegment segment = bufferPool.requestMemorySegment();
      if (segment == null) {
        break;
      }
      freeSegments.add(segment);
    }

    int numWriteBuffers = freeSegments.size() / 2;
    numBuffersForSort = freeSegments.size() - numWriteBuffers;
  }

  public void requestGuaranteedBuffers() throws IOException {
    int numRequiredBuffer = bufferPool.getNumberOfRequiredMemorySegments();
    if (numRequiredBuffer < 2) {
      throw new IOException(
          String.format(
              "Too few sort buffers, please increase %s.",
              NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS));
    }

    try {
      while (freeSegments.size() < numRequiredBuffer) {
        freeSegments.add(checkNotNull(bufferPool.requestMemorySegmentBlocking()));
      }
    } catch (InterruptedException exception) {
      releaseFreeBuffers();
      throw new IOException("Failed to allocate buffers for result partition.", exception);
    }
  }

  private void releaseFreeBuffers() {
    if (bufferPool != null) {
      freeSegments.forEach(buffer -> bufferPool.recycle(buffer));
      freeSegments.clear();
    }
  }

  private void releaseFreeSegments() {
    if (bufferPool != null) {
      freeSegments.forEach(buffer -> bufferPool.recycle(buffer));
      freeSegments.clear();
    }
  }

  private void releaseWriteBuffer(RssDataBuffer writeBuffer) {
    if (writeBuffer != null) {
      writeBuffer.release();
    }
  }

  public void close() {
    releaseFreeSegments();
    releaseWriteBuffer(unicastWriteBuffer);
    releaseWriteBuffer(broadcastWriteBuffer);
    rssPartitionedWriter.close();
  }

  public void finish() throws IOException {
    checkState(
        unicastWriteBuffer == null, "The unicast write buffer should be either null or released.");
    flushBroadcastWriteBuffer();
    rssPartitionedWriter.finish();
  }

  public boolean isEndOfDataNotified() {
    return endOfDataNotified;
  }

  public void setEndOfDataNotified(boolean endOfDataNotified) {
    this.endOfDataNotified = endOfDataNotified;
  }

  // ------------------------------------------------------------------------
  // Statistics
  // ------------------------------------------------------------------------

  private void updateStatistics(
      RssBufferWithSubpartition bufferWithSubpartition, boolean isBroadcast) {
    numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
    long readableBytes = bufferWithSubpartition.getBuffer().readableBytes();
    numBytesProduced.inc(readableBytes);
    numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
  }
}
