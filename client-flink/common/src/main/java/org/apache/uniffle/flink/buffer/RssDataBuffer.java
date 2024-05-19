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
import javax.annotation.Nullable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

/**
 * Data of different channels can be appended to a {@link RssDataBuffer} and after the {@link
 * RssDataBuffer} is full or finished, the appended data can be copied from it in channel index
 * order.
 *
 * <p>The lifecycle of a {@link RssDataBuffer} can be: new, write, [read, reset, write], finish,
 * read, release. There can be multiple [read, reset, write] operations before finish.
 *
 * <p>我们在完成这个类的时候，参考DataBuffer，但是因为我们要支持多个版本，没办法
 */
public interface RssDataBuffer {

  /**
   * Appends data of the specified channel to this {@link RssDataBuffer} and returns true if this
   * {@link RssDataBuffer} is full.
   */
  boolean append(ByteBuffer source, int targetSubpartition, Buffer.DataType dataType)
      throws IOException;

  /**
   * Copies data in this {@link RssDataBuffer} to the target {@link MemorySegment} in channel index
   * order and returns {@link RssBufferWithSubpartition} which contains the copied data and the
   * corresponding channel index.
   */
  RssBufferWithSubpartition getNextBuffer(@Nullable MemorySegment transitBuffer);

  /** Returns the total number of records written to this {@link RssDataBuffer}. */
  long numTotalRecords();

  /** Returns the total number of bytes written to this {@link RssDataBuffer}. */
  long numTotalBytes();

  /** Returns true if not all data appended to this {@link RssDataBuffer} is consumed. */
  boolean hasRemaining();

  /** Finishes this {@link RssDataBuffer} which means no record can be appended any more. */
  void finish();

  /** Whether this {@link RssDataBuffer} is finished or not. */
  boolean isFinished();

  /** Releases this {@link RssDataBuffer} which releases all resources. */
  void release();

  /** Whether this {@link RssDataBuffer} is released or not. */
  boolean isReleased();
}
