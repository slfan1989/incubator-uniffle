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

package org.apache.uniffle.flink.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import javax.annotation.Nullable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import static org.apache.flink.util.Preconditions.*;

public class RssSortBuffer implements RssDataBuffer {

  /**
   * Size of an index entry: 4 bytes for record length, 4 bytes for data type and 8 bytes for
   * pointer to next entry.
   */
  protected static final int INDEX_ENTRY_SIZE = 4 + 4 + 8;

  /** A list of {@link MemorySegment}s used to store data in memory. */
  protected final LinkedList<MemorySegment> freeSegments;

  /** {@link BufferRecycler} used to recycle {@link #freeSegments}. */
  protected final BufferRecycler bufferRecycler;

  /** A segment list as a joint buffer which stores all records and index entries. */
  public final ArrayList<MemorySegment> segments = new ArrayList<>();

  /** Addresses of the first record's index entry for each subpartition. */
  private final long[] firstIndexEntryAddresses;

  /** Addresses of the last record's index entry for each subpartition. */
  protected final long[] lastIndexEntryAddresses;

  /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
  private final int bufferSize;

  /** Number of guaranteed buffers can be allocated from the buffer pool for data sort. */
  private int numGuaranteedBuffers;

  // ---------------------------------------------------------------------------------------------
  // Statistics and states
  // ---------------------------------------------------------------------------------------------

  /** Total number of bytes already appended to this sort buffer. */
  private long numTotalBytes;

  /** Total number of records already appended to this sort buffer. */
  private long numTotalRecords;

  /** Total number of bytes already read from this sort buffer. */
  protected long numTotalBytesRead;

  /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
  protected boolean isFinished;

  /** Whether this sort buffer is released. A released sort buffer can not be used. */
  protected boolean isReleased;

  // ---------------------------------------------------------------------------------------------
  // For writing
  // ---------------------------------------------------------------------------------------------

  /** Array index in the segment list of the current available buffer for writing. */
  private int writeSegmentIndex;

  /** Next position in the current available buffer for writing. */
  private int writeSegmentOffset;

  // ---------------------------------------------------------------------------------------------
  // For reading
  // ---------------------------------------------------------------------------------------------

  /** Data of different subpartitions in this sort buffer will be read in this order. */
  protected final int[] subpartitionReadOrder;

  /** Index entry address of the current record or event to be read. */
  protected long readIndexEntryAddress;

  /** Record bytes remaining after last copy, which must be read first in next copy. */
  protected int recordRemainingBytes;

  /** Used to index the current available subpartition to read data from. */
  protected int readOrderIndex = -1;

  public RssSortBuffer(
      LinkedList<MemorySegment> freeSegments,
      BufferRecycler bufferRecycler,
      int numSubpartitions,
      int bufferSize,
      int numGuaranteedBuffers) {
    checkArgument(bufferSize > INDEX_ENTRY_SIZE, "Buffer size is too small.");
    checkArgument(numGuaranteedBuffers > 0, "No guaranteed buffers for sort.");

    this.freeSegments = checkNotNull(freeSegments);
    this.bufferRecycler = checkNotNull(bufferRecycler);
    this.bufferSize = bufferSize;
    this.numGuaranteedBuffers = numGuaranteedBuffers;
    checkState(numGuaranteedBuffers <= freeSegments.size(), "Wrong number of free segments.");
    this.firstIndexEntryAddresses = new long[numSubpartitions];
    this.lastIndexEntryAddresses = new long[numSubpartitions];

    // initialized with -1 means the corresponding subpartition has no data
    Arrays.fill(firstIndexEntryAddresses, -1L);
    Arrays.fill(lastIndexEntryAddresses, -1L);

    this.subpartitionReadOrder = new int[numSubpartitions];
    for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
      this.subpartitionReadOrder[subpartition] = subpartition;
    }
  }

  /**
   * Appends data of the specified channel to this {@link RssDataBuffer} and returns true if this
   * {@link RssDataBuffer} is full.
   */
  @Override
  public boolean append(ByteBuffer source, int targetSubpartition, Buffer.DataType dataType)
      throws IOException {
    checkArgument(source.hasRemaining(), "Cannot append empty data.");
    checkState(!isFinished, "Rss Write buffer is already finished.");
    checkState(!isReleased, "Rss Write buffer is already released.");

    int totalBytes = source.remaining();

    // return true directly if it can not allocate enough buffers for the given record
    if (!allocateBuffersForRecord(totalBytes)) {
      return true;
    }

    // write the index entry and record or event data
    writeIndex(targetSubpartition, totalBytes, dataType);
    writeRecord(source);

    ++numTotalRecords;
    numTotalBytes += totalBytes;

    return false;
  }

  private void writeIndex(int subpartitionIndex, int numRecordBytes, Buffer.DataType dataType) {
    MemorySegment segment = segments.get(writeSegmentIndex);

    // record length takes the high 32 bits and data type takes the low 32 bits
    segment.putLong(writeSegmentOffset, ((long) numRecordBytes << 32) | dataType.ordinal());

    // segment index takes the high 32 bits and segment offset takes the low 32 bits
    long indexEntryAddress = ((long) writeSegmentIndex << 32) | writeSegmentOffset;

    long lastIndexEntryAddress = lastIndexEntryAddresses[subpartitionIndex];
    lastIndexEntryAddresses[subpartitionIndex] = indexEntryAddress;

    if (lastIndexEntryAddress >= 0) {
      // link the previous index entry of the given subpartition to the new index entry
      segment = segments.get(getSegmentIndexFromPointer(lastIndexEntryAddress));
      segment.putLong(getSegmentOffsetFromPointer(lastIndexEntryAddress) + 8, indexEntryAddress);
    } else {
      firstIndexEntryAddresses[subpartitionIndex] = indexEntryAddress;
    }

    // move the write position forward so as to write the corresponding record
    updateWriteSegmentIndexAndOffset(INDEX_ENTRY_SIZE);
  }

  protected int getSegmentIndexFromPointer(long value) {
    return (int) (value >>> 32);
  }

  protected int getSegmentOffsetFromPointer(long value) {
    return (int) (value);
  }

  private void writeRecord(ByteBuffer source) {
    while (source.hasRemaining()) {
      MemorySegment segment = segments.get(writeSegmentIndex);
      int toCopy = Math.min(bufferSize - writeSegmentOffset, source.remaining());
      segment.put(writeSegmentOffset, source, toCopy);

      // move the write position forward so as to write the remaining bytes or next record
      updateWriteSegmentIndexAndOffset(toCopy);
    }
  }

  private boolean allocateBuffersForRecord(int numRecordBytes) {
    int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
    int availableBytes = writeSegmentIndex == segments.size() ? 0 : bufferSize - writeSegmentOffset;

    // return directly if current available bytes is adequate
    if (availableBytes >= numBytesRequired) {
      return true;
    }

    // skip the remaining free space if the available bytes is not enough for an index entry
    if (availableBytes < INDEX_ENTRY_SIZE) {
      updateWriteSegmentIndexAndOffset(availableBytes);
      availableBytes = 0;
    }

    if (availableBytes + (numGuaranteedBuffers - segments.size()) * (long) bufferSize
        < numBytesRequired) {
      return false;
    }

    // allocate exactly enough buffers for the appended record
    while (availableBytes < numBytesRequired) {
      MemorySegment segment = freeSegments.poll();
      availableBytes += bufferSize;
      addBuffer(checkNotNull(segment));
    }

    return true;
  }

  private void updateWriteSegmentIndexAndOffset(int numBytes) {
    writeSegmentOffset += numBytes;

    // using the next available free buffer if the current is full
    if (writeSegmentOffset == bufferSize) {
      ++writeSegmentIndex;
      writeSegmentOffset = 0;
    }
  }

  private void addBuffer(MemorySegment segment) {
    if (segment.size() != bufferSize) {
      bufferRecycler.recycle(segment);
      throw new IllegalStateException("Illegal memory segment size.");
    }

    if (isReleased) {
      bufferRecycler.recycle(segment);
      throw new IllegalStateException("Sort buffer is already released.");
    }

    segments.add(segment);
  }

  @Override
  public RssBufferWithSubpartition getNextBuffer(@Nullable MemorySegment transitBuffer) {
    checkState(isFinished, "Sort buffer is not ready to be read.");
    checkState(!isReleased, "Sort buffer is already released.");

    if (!hasRemaining()) {
      return null;
    }

    int numBytesCopied = 0;
    Buffer.DataType bufferDataType = Buffer.DataType.DATA_BUFFER;
    int subpartitionIndex = subpartitionReadOrder[readOrderIndex];

    do {
      int sourceSegmentIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
      int sourceSegmentOffset = getSegmentOffsetFromPointer(readIndexEntryAddress);
      MemorySegment sourceSegment = segments.get(sourceSegmentIndex);

      long lengthAndDataType = sourceSegment.getLong(sourceSegmentOffset);
      int length = getSegmentIndexFromPointer(lengthAndDataType);
      Buffer.DataType dataType =
          Buffer.DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

      // return the data read directly if the next to read is an event
      if (dataType.isEvent() && numBytesCopied > 0) {
        break;
      }
      bufferDataType = dataType;

      // get the next index entry address and move the read position forward
      long nextReadIndexEntryAddress = sourceSegment.getLong(sourceSegmentOffset + 8);
      sourceSegmentOffset += INDEX_ENTRY_SIZE;

      // allocate a temp buffer for the event if the target buffer is not big enough
      if (bufferDataType.isEvent() && transitBuffer.size() < length) {
        transitBuffer = MemorySegmentFactory.allocateUnpooledSegment(length);
      }

      numBytesCopied +=
          copyRecordOrEvent(
              transitBuffer, numBytesCopied, sourceSegmentIndex, sourceSegmentOffset, length);

      if (recordRemainingBytes == 0) {
        // move to next subpartition if the current subpartition has been finished
        if (readIndexEntryAddress == lastIndexEntryAddresses[subpartitionIndex]) {
          updateReadSubpartitionAndIndexEntryAddress();
          break;
        }
        readIndexEntryAddress = nextReadIndexEntryAddress;
      }
    } while (numBytesCopied < transitBuffer.size() && bufferDataType.isBuffer());

    numTotalBytesRead += numBytesCopied;
    Buffer buffer = new NetworkBuffer(transitBuffer, (buf) -> {}, bufferDataType, numBytesCopied);
    return new RssBufferWithSubpartition(buffer, subpartitionIndex);
  }

  protected void updateReadSubpartitionAndIndexEntryAddress() {
    // skip the subpartitions without any data
    while (++readOrderIndex < firstIndexEntryAddresses.length) {
      int subpartitionIndex = subpartitionReadOrder[readOrderIndex];
      if ((readIndexEntryAddress = firstIndexEntryAddresses[subpartitionIndex]) >= 0) {
        break;
      }
    }
  }

  protected int copyRecordOrEvent(
      MemorySegment targetSegment,
      int targetSegmentOffset,
      int sourceSegmentIndex,
      int sourceSegmentOffset,
      int recordLength) {
    if (recordRemainingBytes > 0) {
      // skip the data already read if there is remaining partial record after the previous
      // copy
      long position = (long) sourceSegmentOffset + (recordLength - recordRemainingBytes);
      sourceSegmentIndex += (position / bufferSize);
      sourceSegmentOffset = (int) (position % bufferSize);
    } else {
      recordRemainingBytes = recordLength;
    }

    int targetSegmentSize = targetSegment.size();
    int numBytesToCopy = Math.min(targetSegmentSize - targetSegmentOffset, recordRemainingBytes);
    do {
      // move to next data buffer if all data of the current buffer has been copied
      if (sourceSegmentOffset == bufferSize) {
        ++sourceSegmentIndex;
        sourceSegmentOffset = 0;
      }

      int sourceRemainingBytes = Math.min(bufferSize - sourceSegmentOffset, recordRemainingBytes);
      int numBytes = Math.min(targetSegmentSize - targetSegmentOffset, sourceRemainingBytes);
      MemorySegment sourceSegment = segments.get(sourceSegmentIndex);
      sourceSegment.copyTo(sourceSegmentOffset, targetSegment, targetSegmentOffset, numBytes);

      recordRemainingBytes -= numBytes;
      targetSegmentOffset += numBytes;
      sourceSegmentOffset += numBytes;
    } while ((recordRemainingBytes > 0 && targetSegmentOffset < targetSegmentSize));

    return numBytesToCopy;
  }

  @Override
  public long numTotalRecords() {
    return numTotalRecords;
  }

  @Override
  public long numTotalBytes() {
    return numTotalBytes;
  }

  @Override
  public boolean hasRemaining() {
    return numTotalBytesRead < numTotalBytes;
  }

  @Override
  public void finish() {
    checkState(!isFinished, "DataBuffer is already finished.");

    isFinished = true;

    // prepare for reading
    updateReadSubpartitionAndIndexEntryAddress();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public void release() {
    if (isReleased) {
      return;
    }
    isReleased = true;

    for (MemorySegment segment : segments) {
      bufferRecycler.recycle(segment);
    }
    segments.clear();
  }

  @Override
  public boolean isReleased() {
    return isReleased;
  }
}
