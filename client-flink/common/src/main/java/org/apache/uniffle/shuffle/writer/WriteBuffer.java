package org.apache.uniffle.shuffle.writer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Data of different channels can be appended to a {@link WriteBuffer} and after the {@link
 * WriteBuffer} is full or finished, the appended data can be copied from it in channel index order.
 *
 * <p>The lifecycle of a {@link WriteBuffer} can be: new, write, [read, reset, write], finish, read,
 * release. There can be multiple [read, reset, write] operations before finish.
 */
public interface WriteBuffer {

  /**
   * Appends data of the specified channel to this {@link WriteBuffer} and returns true if this
   * {@link WriteBuffer} is full.
   */
  boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType)
      throws IOException;

  /**
   * Copies data in this {@link WriteBuffer} to the target {@link MemorySegment} in channel index
   * order and returns {@link BufferWithChannel} which contains the copied data and the
   * corresponding channel index.
   */
  BufferWithChannel getNextBuffer(@Nullable MemorySegment transitBuffer);

  /** Returns the total number of records written to this {@link WriteBuffer}. */
  long numTotalRecords();

  /** Returns the total number of bytes written to this {@link WriteBuffer}. */
  long numTotalBytes();

  /** Finishes this {@link WriteBuffer} which means no record can be appended any more. */
  void finish();

  /** Whether this {@link WriteBuffer} is finished or not. */
  boolean isFinished();

  /** Releases this {@link WriteBuffer} which releases all resources. */
  void release();

  /** Whether this {@link WriteBuffer} is released or not. */
  boolean isReleased();

  /** Resets this {@link WriteBuffer} to be reused for data appending. */
  void reset();

  /** Returns true if there is still data can be consumed in this {@link WriteBuffer}. */
  boolean hasRemaining();
}
